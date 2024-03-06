#include "./protocol-aria-fb.hpp"

/*
    This is a implementation of "Aria: A Fast and Practical Deterministic OLTP Database" (Yi Lu, Xiangyao Yu, Lei Cao, Samuel Madden). 
    In this implementation we adopt the fallback strategy and reordering strategy discussed in this paper. 
 */

namespace spectrum 
{

#define K std::tuple<evmc::address, evmc::bytes32>
#define T AriaTransaction

using namespace std::chrono;

/// @brief initialize aria protocol
/// @param workload an evm transaction workload
/// @param batch_size batch size
/// @param n_threads the number of threads in thread pool
/// @param table_partitions the number of partitions in table
Aria::Aria(
    Workload& workload, size_t batch_size, 
    size_t n_threads, size_t table_partitions
):
    workload{workload},
    batch_size{batch_size},
    table_partitions{table_partitions},
    pool{(unsigned int) n_threads},
    table(table_partitions)
{}

/// @brief execute multiple transactions in parallel
/// @param map the function to execute
/// @param batch the current aria batch to work on
void Aria::ParallelEach(
    std::function<void(T&)>                                 map, 
    std::vector<std::optional<std::unique_ptr<T>>>&         batch
) {
    pool.submit_loop(
        size_t{0}, batch.size(), 
        [&](size_t i) {
            if (batch[i] == std::nullopt) {
                batch[i].emplace(std::move(this->NextTransaction()));
            }
            map(*batch[i].value());
        }
    ).wait();
}

/// @brief generate a wrapped transaction with atomic incremental id
/// @return the wrapped transactions
std::unique_ptr<T> Aria::NextTransaction() {
    auto id = tx_counter.fetch_add(1);
    return std::unique_ptr<T>(new T(std::move(workload.Next()), id, id / batch_size));
}

/// @brief start aria protocol
void Aria::Start() {
    // this macro computes the latency of one transaction
    #define LATENCY duration_cast<microseconds>(steady_clock::now() - tx.start_time).count()
    pool.detach_task([&]() {while(!stop_flag.load()) {
        // -- construct an empty batch
        auto batch = std::vector<std::optional<std::unique_ptr<T>>>();
        for (size_t i = 0; i < batch_size; ++i) {
            batch.push_back(std::nullopt);
        }
        // -- execution stage
        ParallelEach([&](auto& tx) {
            AriaExecutor::Execute(tx, table);
            AriaExecutor::Reserve(tx, table);
            statistics.JournalExecute();
        }, batch);
        // -- first commit stage
        auto has_conflict = std::atomic<bool>{false};
        ParallelEach([&](auto& tx) {
            AriaExecutor::Verify(tx, table);
            if (tx.flag_conflict) {
                has_conflict.store(true);
                return;
            }
            AriaExecutor::Commit(tx, table);
            statistics.JournalCommit(LATENCY);
        }, batch);
        // -- prepare fallback, analyze dependencies
        if (!has_conflict.load()) { continue; }
        auto lock_table = AriaLockTable(batch_size);
        ParallelEach([&](auto& tx) {
            if (!tx.flag_conflict) { return; }
            AriaExecutor::PrepareLockTable(tx, lock_table);
        }, batch);
        // -- run fallback strategy
        ParallelEach([&](auto& tx) {
            if (!tx.flag_conflict) { return; }
            AriaExecutor::Fallback(tx, table, lock_table);
            statistics.JournalExecute();
            statistics.JournalCommit(LATENCY);
        }, batch);
    }});
    #undef LATENCY
}

/// @brief stop aria protocol and return statistics
/// @return statistics of current execution
Statistics Aria::Stop() {
    this->stop_flag.store(true);
    return this->statistics;
}


/// @brief report aria statistics
/// @return current statistics of aria execution
Statistics Aria::Report() {
    return this->statistics;
}

/// @brief construct an empty aria transaction
AriaTransaction::AriaTransaction(
    Transaction&& inner, 
    size_t id, size_t batch_id
):
    Transaction{std::move(inner)},
    id{id},
    batch_id{batch_id},
    start_time{std::chrono::steady_clock::now()}
{}

/// @brief reserved a get entry
/// @param tx the transaction
/// @param k the reserved key
void AriaTable::ReserveGet(T* tx, const K& k) {
    Table::Put(k, [&](AriaEntry& entry) {
        if (entry.batch_id_get != tx->batch_id) {
            entry.reserved_get_tx = nullptr;
            entry.batch_id_get = tx->batch_id;
        }
        if (entry.reserved_get_tx == nullptr || entry.reserved_get_tx->id < tx->id) {
            entry.reserved_get_tx = tx;
        }
    });
}

/// @brief reserve a put entry
/// @param tx the transaction
/// @param k the reserved key
void AriaTable::ReservePut(T* tx, const K& k) {
    Table::Put(k, [&](AriaEntry& entry) {
        if (entry.batch_id_get != tx->batch_id) {
            entry.reserved_put_tx = nullptr;
            entry.batch_id_put = tx->batch_id;
        }
        if (entry.reserved_put_tx == nullptr || entry.reserved_put_tx->id < tx->id) {
            entry.reserved_put_tx = tx;
        }
    });
}

/// @brief compare reserved get transaction
/// @param tx the transaction
/// @param k the compared key
/// @return if current transaction reserved this entry successfully
bool AriaTable::CompareReservedGet(T* tx, const K& k) {
    bool eq = true;
    Table::Get(k, [&](auto entry) {
        eq = entry.reserved_get_tx == nullptr || 
             entry.reserved_get_tx->id >= tx->id;
    });
    return eq;
}

/// @brief compare reserved put transaction
/// @param tx the transaction
/// @param k the compared key
/// @return if current transaction reserved this entry successfully
bool AriaTable::CompareReservedPut(T* tx, const K& k) {
    bool eq = true;
    Table::Get(k, [&](auto entry) {
        eq = entry.reserved_put_tx == nullptr || 
             entry.reserved_put_tx->id >= tx->id;
    });
    return eq;
}

/// @brief initialize an aria lock table
/// @param partitions the number of partitions used in parallel hash table
AriaLockTable::AriaLockTable(size_t partitions): 
    Table::Table(partitions)
{}

/// @brief execute a transaction and journal write operations locally
/// @param tx the transaction
/// @param table the table
void AriaExecutor::Execute(T& tx, AriaTable& table) {
    // read from the public table
    tx.UpdateGetStorageHandler([&](
        const evmc::address &addr,
        const evmc::bytes32 &key
    ) {
        // if some write on this entry is issued previously, 
        //  the read dependency will be barricated from journal. 
        auto tup = std::make_tuple(addr, key);
        if (tx.local_put.contains(tup)) {
            return tx.local_put[tup];
        }
        if (tx.local_get.contains(tup)) {
            return tx.local_get[tup];
        }
        auto value = evmc::bytes32{0};
        table.Get(tup, [&](auto& entry){
            value = entry.value;
        });
        tx.local_get[tup] = value;
        return value;
    });
    // write locally to local storage
    tx.UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key,
        const evmc::bytes32 &value
    ) {
        auto tup = std::make_tuple(addr, key);
        tx.local_put[tup] = value;
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    // execute the transaction
    tx.Execute();
}

/// @brief journal the smallest reader/writer transaction to table
/// @param tx the transaction
/// @param table the aria shared table
void AriaExecutor::Reserve(T& tx, AriaTable& table) {
    // journal all entries to the reservation table
    for (auto& tup: tx.local_get) {
        table.ReserveGet(&tx, std::get<0>(tup));
    }
    for (auto& tup: tx.local_put) {
        table.ReservePut(&tx, std::get<0>(tup));
    }
}

/// @brief verify transaction by checking dependencies
/// @param tx the transaction, flag_conflict will be altered
/// @param table the aria shared table
void AriaExecutor::Verify(T& tx, AriaTable& table) {
    // conceptually, we take a snapshot on the database before we execute a batch
    //  , and all transactions are executed viewing the snapshot. 
    // however, we want the global state transitioned 
    //  as if we executed some of these transactions sequentially. 
    // therefore, we have to pick some transactions and arange them into a sequence. 
    // this algorithm implicitly does it for us. 
    bool war = false, raw = false, waw = false;
    for (auto& tup: tx.local_get) {
        // the value is updated, snapshot contains out-dated value
        raw |= !table.CompareReservedPut(&tx, std::get<0>(tup));
    }
    for (auto& tup: tx.local_put) {
        // the value is read before, therefore we should not update it
        war |= !table.CompareReservedGet(&tx, std::get<0>(tup));
    }
    for (auto& tup: tx.local_put) {
        // if some write happened after write
        waw |= !table.CompareReservedPut(&tx, std::get<0>(tup));
    }
    tx.flag_conflict = waw || (raw && war);
}

/// @brief commit written values into table
/// @param tx the transaction
/// @param table the aria shared table
void AriaExecutor::Commit(T& tx, AriaTable& table) {
    for (auto& tup: tx.local_put) {
        table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.value = std::get<1>(tup);
        });
    }
}

/// @brief put transaction id (local id) into table
/// @param tx the transaction
/// @param table the aria lock table
void AriaExecutor::PrepareLockTable(T& tx, AriaLockTable& table) {
    for (auto& tup: tx.local_get) {
        table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.deps_get.push_back(&tx);
        });
    }
    for (auto& tup: tx.local_put) {
        table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.deps_put.push_back(&tx);
        });
    }
}

/// @brief fallback execution without constant
/// @param tx the transaction
/// @param table the aria table
/// @param lock_table the lock table with registered lock information
void AriaExecutor::Fallback(T& tx, AriaTable& table, AriaLockTable& lock_table) {
    // read from the public table
    tx.UpdateGetStorageHandler([&](
        const evmc::address &addr,
        const evmc::bytes32 &key
    ) {
        auto tup = std::make_tuple(addr, key);
        auto value = evmc::bytes32{0};
        table.Get(tup, [&](auto& entry){
            value = entry.value;
        });
        return value;
    });
    // write directly into the public table
    tx.UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key,
        const evmc::bytes32 &value
    ) {
        auto tup = std::make_tuple(addr, key);
        table.Put(tup, [&](auto& entry){
            entry.value = value;
        });
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    // get the latest dependency and wait on it
    T* should_wait = nullptr;
    #define COND (_tx->id < tx.id && (should_wait == nullptr || _tx->id > should_wait->id))
    for (auto& tup: tx.local_put) {
        lock_table.Get(std::get<0>(tup), [&](auto& entry) {
            for (auto _tx: entry.deps_get) { if (COND) { should_wait = _tx; } }
            for (auto _tx: entry.deps_put) { if (COND) { should_wait = _tx; } }
        });
    }
    for (auto& tup: tx.local_get) {
        lock_table.Get(std::get<0>(tup), [&](auto& entry) {
            for (auto _tx: entry.deps_put) { if (COND) { should_wait = _tx; } }
        });
    }
    #undef COND
    while(should_wait && !should_wait->commited.load()) {
        std::this_thread::yield();
    }
    tx.Execute();
    tx.commited.store(true);
}

#undef K
#undef T

} // namespace spectrum