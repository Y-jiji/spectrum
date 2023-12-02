#pragma once
#include <dcc/benchmark/evm/Transaction.h>
#include <dcc/common/HashMap.h>
#include <dcc/common/Percentile.h>
#include <dcc/core/Defs.h>
#include <dcc/core/Delay.h>
#include <dcc/core/Partitioner.h>
#include <dcc/core/Worker.h>
#include <dcc/protocol/Spectrum/SpecPredScheduler.h>
#include <dcc/protocol/Spectrum/Spectrum.h>
#include <dcc/protocol/Spectrum/SpectrumHelper.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <intx/intx.hpp>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

#include "glog/logging.h"

namespace dcc {

template <class Workload>
class SpectrumExecutor : public Worker {
 public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SpectrumTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Spectrum<DatabaseType>;

  SpectrumExecutor(
      std::size_t coordinator_id, std::size_t id, DatabaseType &db,
      ContextType &context,
      std::vector<std::unique_ptr<TransactionType>> &transactions,
      std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
      std::atomic<uint32_t> &lock_manager_status,
      std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &total_abort,
      std::atomic<uint32_t> &n_complete_workers,
      std::atomic<uint32_t> &n_started_workers, std::atomic<bool> &stopFlag,
      std::atomic<uint32_t> &NEXT_TX,
      std::vector<std::unique_ptr<SpecPredScheduler>> &scheduler_vec)
      : Worker(coordinator_id, id),
        db(db),
        context(context),
        transactions(transactions),
        storages(storages),
        epoch(epoch),
        lock_manager_status(lock_manager_status),
        worker_status(worker_status),
        total_abort(total_abort),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        n_lock_manager(context.sparkle_lock_manager),
        n_workers(context.worker_num - n_lock_manager),
        lock_manager_id(SpectrumHelper::worker_id_to_lock_manager_id(
            id, n_lock_manager, n_workers)),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)),
        stopFlag(stopFlag),
        NEXT_TX(NEXT_TX),
        spec_scheduler_vec(scheduler_vec) {}

  ~SpectrumExecutor() = default;

  void start() override {
    LOG(INFO) << "SpectrumExecutor " << id << " started. ";

    CHECK(n_lock_manager > 0) << "at least one sparkle_lock_manager";

    if (id < n_lock_manager) {
      // schedule transactions
      LOG(INFO) << "scheduler " << id << " starts.";
      schedule_sparkle_transactions();
    } else {
      // if (id >= context.worker_num) {
      //   // work as committer
      //   LOG(INFO) << "committer " << id << " starts.";
      //   commit_sparkle_transactions();
      // } else {
      // work as executor
      LOG(INFO) << "worker " << id << " starts.";
      run_sparkle_transactions();
      // }
    }

    LOG(INFO) << "SpectrumExecutor " << id << " exits. ";
  }

  void push_message(Message *message) override {}

  Message *pop_message() override { return nullptr; }

  void speculative_commit(TransactionType &txn) {
    txn.stage = 1;
    // LOG(INFO)<<"writeSet_size: "<<txn.writeSet.size();
    for (auto i = txn.writePub + 1; i < txn.writeSet.size(); i++) {
      auto &writeKey = txn.writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();

      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      CHECK(key && value);

      auto &tmp_key = *static_cast<const evmc::bytes32 *>(key);
      auto &tmp_val = *static_cast<const evmc::bytes32 *>(value);
      // LOG(INFO)<<"txnid: "<< txn.id <<", sp key:
      // "<<silkworm::to_hex(tmp_key); LOG(INFO)<<"sp value:
      // "<<silkworm::to_hex(tmp_val);
      if (table->addVersion(key, value, &txn, NEXT_TX) == 0) {
        return;
      };
      txn.writePub = i;
    }
    txn.write_public = true;
  }

  bool finalCommit(TransactionType &txn) {
    txn.stage = 2;
    // LOG(INFO) << txn.id << " " << NEXT_TX.load() << " "
    //           << (txn.id != NEXT_TX.load());
    if (txn.id != NEXT_TX.load() || txn.will_local_abort()) {
      return false;
    }
    if (txn.id == NEXT_TX.load() && !txn.will_local_abort()) {
      for (auto i = 0u; i < txn.writeSet.size(); i++) {
        auto &writeKey = txn.writeSet[i];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();

        auto table = db.find_table(tableId, partitionId);
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        CHECK(key && value);

        // print key & value
        auto &tmp_key = *static_cast<const evmc::bytes32 *>(key);
        auto &tmp_val = *static_cast<const evmc::bytes32 *>(value);
        // LOG(INFO)<<"remove key: "<<silkworm::to_hex(tmp_key);
        // LOG(INFO)<<"remove value: "<<silkworm::to_hex(tmp_val);
        table->UnlockAndVaccum(key, &txn);
      }

      for (auto i = 0u; i < txn.readSet.size(); i++) {
        auto &readKey = txn.readSet[i];
        auto tableId = readKey.get_table_id();
        auto partitionId = readKey.get_partition_id();
        auto table = db.find_table(tableId, partitionId);
        auto key = readKey.get_key();
        table->RemoveFromDeps(key, &txn);
      }

      auto &evmTxn = *static_cast<dcc::evm::Invoke<TransactionType> *>(&txn);
      n_operations.fetch_add(evmTxn.evm.execution_state->count);
      NEXT_TX.fetch_add(1);
      return true;
    } else {
      return false;
    }
  }

  void localAbort(TransactionType &txn) {
    // auto rollback_key_guard = std::lock_guard{txn.rollback_key_mu};
    n_abort_lock.fetch_add(1);
    auto rollback_key_set = std::unordered_set<evmc::bytes32>{};
    {
      std::lock_guard<std::mutex> mu_lock(txn.rollback_key_mu);
      std::swap(txn.rollback_key, rollback_key_set);
    }
    auto writeSetLength = 0u;
    auto readSetLength = 0u;
    evmc::bytes32 rollback_key{0};
    txn.health_check("before rollback");
    for (auto i = 0; i <= txn.localCheckpoint.size(); ++i) {
      if (i == txn.localCheckpoint.size()) {
        return;
      }
      if (!rollback_key_set.contains(std::get<0>(txn.localCheckpoint[i]))) {
        continue;
      } else {
        std::tie(readSetLength, writeSetLength) =
            std::get<1>(txn.localCheckpoint[i]);
        rollback_key = std::get<0>(txn.localCheckpoint[i]);
        partial_revert[txn.localCheckpoint.size() - i].fetch_add(1);
        txn.localCheckpoint.resize(i);
        break;
      }
    }
    // LOG(INFO) << "ab " << txn.id << ":" << txn.localCheckpoint.size() <<
    // std::endl;
    // ---------------------------------------------------------------
    txn.health_check("before reset write keys");
    for (auto i = writeSetLength; i < txn.writeSet.size(); i++) {
      auto &writeKey = txn.writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();

      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();

      table->UnlockAndRemove(key, &txn);
    }
    // Add release write locks

    // ---------------------------------------------------------------
    txn.writePub = std::min(txn.writePub, int{writeSetLength} - 1);
    auto &evmTxn = *static_cast<dcc::evm::Invoke<TransactionType> *>(&txn);
    evmTxn.evm.execution_state->partial_revert_key =
        intx::be::load<intx::uint256>(rollback_key);
    evmTxn.evm.execution_state->will_partial_revert = true;
    txn.health_check("after set rollback key");
    // ---------------------------------------------------------------
    txn.reset();
    txn.health_check("before resize");
    // ---------------------------------------------------------------
    txn.writeSet.resize(writeSetLength);
    txn.readSet.resize(readSetLength);
    txn.tuple_num = readSetLength + writeSetLength;
    // ---------------------------------------------------------------
    txn.health_check("after resize");
    txn.write_public = false;
    txn.stage = 0;
    txn.has_recorded_abort = true;
    // ---------------------------------------------------------------
  }

  void schedule_sparkle_transactions() {
    // grant locks, once all locks are acquired, assign the transaction to
    // a worker thread in a round-robin manner.

    auto i = id;
    // int batch_id = 0;
    // auto nxt_tid = id;
    auto request_id = 0;
    std::queue<std::unique_ptr<TransactionType>> pool;

    while (!stopFlag.load()) {
      if (!done_queue.empty()) {
        while (!done_queue.empty()) {
          // std::unique_ptr<TransactionType> _transaction(done_queue.front());
          auto _transaction = done_queue.front();
          done_queue.pop();

          // LOG(INFO) << "txn done: " << _transaction->id;
          spec_scheduler_vec[id]->Release(_transaction);
          delete _transaction;
        }

      } else if (!abort_queue.empty()) {
        // reschedule
        while (!abort_queue.empty()) {
          auto _transaction = abort_queue.front();
          abort_queue.pop();

          _transaction->update_rwset();
          // auto worker = get_available_worker(request_id++);
          // _transaction->scheduler_id = id;
          // _transaction->executor_id = worker;
          spec_scheduler_vec[id]->Re_Lock(_transaction);
          // LOG(INFO) << "abort txnid: " << _transaction->id
          //           << ", abort txn execid: " << _transaction->executor_id;
          // all_executors[_transaction->executor_id]->transaction_queue.push(
          //     _transaction);
        }

      } else {
        auto ntx = NEXT_TX.load();
        // LOG(INFO)<<"ntx: "<<ntx<<" "<<i<<" "<<i - (ntx - 1);

        if (i - (ntx - 1) > context.batch_size * 2) {
          __asm volatile("pause" : :);
          continue;
        }

        for (int j = 0; j < context.batch_size; ++j) {
          auto partition_id = id;
          // int vec_size = transactions.size();
          // int tx_idx = i % vec_size;
          auto _transaction = workload.next_transaction(context, partition_id,
                                                        storages[0], i + 1);
          // _transaction->health_check("boom");
          _transaction->set_id(i + 1);
          _transaction->stage = 0;

          auto &_evm_transaction =
              *static_cast<dcc::evm::Invoke<TransactionType> *>(
                  _transaction.get());
          _evm_transaction.evm.id = i + 1;
          pool.push(std::move(_transaction));

          i += n_lock_manager;
        }

        for (int j = 0; j < context.batch_size; ++j) {
          auto transaction = std::move(pool.front());
          pool.pop();
          setupHandlers(*transaction);
          transaction->set_exec_type(ExecType::Exec_Spectrum);
          spec_scheduler_vec[id]->Lock(transaction.release());
        }
      }

      while (!spec_scheduler_vec[id]->ready_txns_.empty()) {
        auto txn = spec_scheduler_vec[id]->ready_txns_.front();
        spec_scheduler_vec[id]->ready_txns_.pop_front();

        // LOG(INFO) << "ready txnid: " << txn->id;

        if (txn->executor_id != 0) {
          // LOG(INFO) << "resche: " << txn->id << " " << txn->executor_id;
          all_executors[txn->executor_id]->transaction_queue.push(txn);
          continue;
        }

        // LOG(INFO)<<"txn ready: "<< txn->id;
        auto worker = get_available_worker(request_id++);
        // LOG(INFO) << "txn ready: " << txn->id << ", scheduler: " << id
        //           << ", worker: " << worker;
        txn->scheduler_id = id;
        txn->executor_id = worker;
        // {
        all_executors[worker]->transaction_queue.push(txn);
        // }
      }
      // set_lock_manager_bit(id);
    }
  }

  void run_sparkle_transactions() {
    auto request_id = 0;

    auto cmp = [](const TransactionType *tx1, const TransactionType *tx2) {
      return tx1->id >= tx2->id;
    };
    std::priority_queue<TransactionType *, std::vector<TransactionType *>,
                        decltype(cmp)>
        prior_pool(cmp);

    while (!stopFlag.load()) {
      while (transaction_queue.empty() && prior_pool.empty()) {
        std::this_thread::yield();
      }

      if (!transaction_queue.empty()) {
        // while (!transaction_queue.empty()) {
        auto _transaction = transaction_queue.front();
        transaction_queue.pop();
        prior_pool.push(_transaction);
        // }
        // LOG(INFO) << "executor id: " << id << ", get tx: " <<
        // _transaction->id;
      }

      if (!prior_pool.empty()) {
        auto transaction = prior_pool.top();
        prior_pool.pop();
        CHECK(nullptr != transaction);
        // -------------------------------------------------------------------------------------
        auto ntx = NEXT_TX.load();

        // LOG(INFO) << "exec txnid: " << transaction->id;

        if (transaction->will_local_abort()) {
          localAbort(*transaction);
          transaction->stage = 0;
          prior_pool.push(transaction);
          // {
          //   std::lock_guard<std::mutex> lock(
          //       all_executors[transaction->scheduler_id]->mpmc_abort_mu);
          //   all_executors[transaction->scheduler_id]->abort_queue.push(
          //       transaction);
          // }
          continue;
        }
        // LOG(INFO) << "pl " << transaction->id << ":" <<
        // transaction->localCheckpoint.size() << ":" << (ntx ==
        // transaction->id) << transaction->stage << std::endl;
        // -------------------------------------------------------------------------------------
        switch (transaction->stage) {
          case 0:
            goto EXECUTE;
          case 1:
            goto SPECULATIVE_COMMIT;
          case 2:
            goto FINAL_COMMIT;
          default:
            CHECK(false) << "unreachable";
        }
      // -------------------------------------------------------------------------------------
      EXECUTE:
        // -- //

        // while (NEXT_TX.load() + context.batch_size <= transaction->id) {
        //   LOG(INFO) << NEXT_TX.load() << " " << transaction->id;
        //   std::this_thread::yield();
        // }

        transaction->stage = 1;
        transaction->execution_phase = true;
        transaction->health_check("before execution");
        transaction->execute(id);
        transaction->health_check("after execution");
        if (transaction->will_local_abort()) {
          localAbort(*transaction);
          transaction->stage = 0;
          // prior_pool.push(transaction);
          push_to_abort_queue(transaction);
          continue;
        }
      // LOG(INFO) << "ex " << transaction->id << ":" << (ntx ==
      // transaction->id) << transaction->stage <<  std::endl;
      // -------------------------------------------------------------------------------------
      SPECULATIVE_COMMIT:
        // ------------- //
        transaction->stage = 1;
        speculative_commit(*transaction);
        if (transaction->will_local_abort()) {
          CHECK(transaction->id != ntx);
          localAbort(*transaction);
          transaction->stage = 0;
          // prior_pool.push(transaction);
          push_to_abort_queue(transaction);
          continue;
        }
      // LOG(INFO) << "sc " << transaction->id << ":" << (ntx ==
      // transaction->id) << transaction->stage << std::endl;
      // -------------------------------------------------------------------------------------
      FINAL_COMMIT:
        // ------- //
        transaction->stage = 2;
        bool fc_success = finalCommit(*transaction);
        if (fc_success) {
          // LOG(INFO) << "fc " << transaction->id << ":"
          //           << (ntx == transaction->id) << " " << transaction->stage
          //           << std::endl;

          n_commit.fetch_add(1);
          auto now = std::chrono::steady_clock::now();
          auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
              now - transaction->startTime);
          percentile.add(latency.count());

          // push to scheduler done_queue for lock release
          push_to_done_queue(transaction);
          // break;
        } else if (transaction->will_local_abort()) {
          CHECK(transaction->id != ntx);
          localAbort(*transaction);
          transaction->stage = 0;
          // prior_pool.push(transaction);
          push_to_abort_queue(transaction);
          continue;
        } else {
          CHECK(transaction->id != ntx);
          transaction->stage = 2;
          prior_pool.push(transaction);
          // break;
        }
      }
    }
  }

  void push_to_done_queue(TransactionType *txn) {
    std::lock_guard<std::mutex> lock(
        all_executors[txn->scheduler_id]->mpmc_done_mu);
    all_executors[txn->scheduler_id]->done_queue.push(txn);
  }
  void push_to_abort_queue(TransactionType *txn) {
    std::lock_guard<std::mutex> lock(
        all_executors[txn->scheduler_id]->mpmc_abort_mu);
    all_executors[txn->scheduler_id]->abort_queue.push(txn);
  }

  // void run_sparkle_transactions() {
  //     std::queue<std::unique_ptr<TransactionType>> working_pool;
  //     auto request_id = 0;

  //     auto cmp = [](const TransactionType *tx1, const TransactionType *tx2) {
  //       return tx1->id >= tx2->id;
  //     };
  //     std::priority_queue<TransactionType *, std::vector<TransactionType *>,
  //                         decltype(cmp)>
  //         prior_pool;

  //     while (!stopFlag.load()) {
  //       transaction_queue.wait_till_non_empty();

  //       std::unique_ptr<TransactionType>
  //       _transaction(transaction_queue.front()); transaction_queue.pop();

  //       // LOG(INFO)<<"executor id: "<<id<<", get tx: "<<_transaction->id;

  //       working_pool.push(std::move(_transaction));

  //       while (true) {
  //         auto transaction = std::move(working_pool.front());
  //         working_pool.pop();
  //         CHECK(nullptr != transaction);
  //         //
  //         -------------------------------------------------------------------------------------
  //         auto ntx = NEXT_TX.load();

  //         if (transaction->will_local_abort()) {
  //           localAbort(*transaction);
  //           transaction->stage = 0;
  //           working_pool.push(std::move(transaction));
  //           continue;
  //         }
  //         // LOG(INFO) << "pl " << transaction->id << ":" <<
  //         // transaction->localCheckpoint.size() << ":" << (ntx ==
  //         // transaction->id) << transaction->stage << std::endl;
  //         //
  //         -------------------------------------------------------------------------------------
  //         switch (transaction->stage) {
  //           case 0:
  //             goto EXECUTE;
  //           case 1:
  //             goto SPECULATIVE_COMMIT;
  //           case 2:
  //             goto FINAL_COMMIT;
  //           default:
  //             CHECK(false) << "unreachable";
  //         }
  //       //
  //       -------------------------------------------------------------------------------------
  //       EXECUTE:
  //         // -- //

  //         // while (NEXT_TX.load() + context.batch_size <= transaction->id) {
  //         //   LOG(INFO) << NEXT_TX.load() << " " << transaction->id;
  //         //   std::this_thread::yield();
  //         // }

  //         transaction->stage = 1;
  //         transaction->execution_phase = true;
  //         transaction->health_check("before execution");
  //         transaction->execute(id);
  //         transaction->health_check("after execution");
  //         if (transaction->will_local_abort()) {
  //           localAbort(*transaction);
  //           transaction->stage = 0;
  //           working_pool.push(std::move(transaction));
  //           continue;
  //         }
  //       // LOG(INFO) << "ex " << transaction->id << ":" << (ntx ==
  //       // transaction->id) << transaction->stage <<  std::endl;
  //       //
  //       -------------------------------------------------------------------------------------
  //       SPECULATIVE_COMMIT:
  //         // ------------- //
  //         transaction->stage = 1;
  //         speculative_commit(*transaction);
  //         if (transaction->will_local_abort()) {
  //           CHECK(transaction->id != ntx);
  //           localAbort(*transaction);
  //           transaction->stage = 0;
  //           working_pool.push(std::move(transaction));
  //           continue;
  //         }
  //       // LOG(INFO) << "sc " << transaction->id << ":" << (ntx ==
  //       // transaction->id) << transaction->stage << std::endl;
  //       //
  //       -------------------------------------------------------------------------------------
  //       FINAL_COMMIT:
  //         // ------- //
  //         transaction->stage = 2;
  //         bool fc_success = finalCommit(*transaction);
  //         if (fc_success) {
  //           // LOG(INFO) << "fc " << transaction->id << ":"
  //           //           << (ntx == transaction->id) << " " <<
  //           transaction->stage
  //           //           << std::endl;

  //           n_commit.fetch_add(1);
  //           auto now = std::chrono::steady_clock::now();
  //           auto latency =
  //           std::chrono::duration_cast<std::chrono::microseconds>(
  //               now - transaction->startTime);
  //           percentile.add(latency.count());

  //           // push to scheduler done_queue for lock release
  //             std::lock_guard<std::mutex> lock(
  //                 all_executors[transaction->scheduler_id]->mpmc_done_mu);
  //             all_executors[transaction->scheduler_id]->done_queue.push(
  //                 transaction.release());
  //           break;
  //         } else if (transaction->will_local_abort()) {
  //           CHECK(transaction->id != ntx);
  //           localAbort(*transaction);
  //           transaction->stage = 0;
  //           working_pool.push(std::move(transaction));
  //           continue;
  //         } else {
  //           CHECK(transaction->id != ntx);
  //           transaction->stage = 2;
  //           // working_pool.push(std::move(transaction));
  //           // continue;
  //           {
  //             std::lock_guard<std::mutex> lock(
  //                 all_executors[context.worker_num +
  //                               request_id % context.commit_threads]
  //                     ->mpmc_done_mu);
  //             all_executors[context.worker_num +
  //                           request_id % context.commit_threads]
  //                 ->done_queue.push(transaction.release());
  //             request_id++;
  //           }
  //           break;
  //         }
  //       }
  //     }
  //   }

  // void commit_sparkle_transactions() {
  //   auto i = id;
  //   auto request_id = 0;

  //   auto cmp = [](const TransactionType *tx1, const TransactionType *tx2) {
  //     return tx1->id >= tx2->id;
  //   };
  //   std::priority_queue<TransactionType *, std::vector<TransactionType *>,
  //                       decltype(cmp)>
  //       prior_pool;

  //   while (!stopFlag.load()) {
  //     if (!prior_pool.empty()) {
  //       auto transaction = prior_pool.top();
  //       prior_pool.pop();

  //       transaction->stage = 2;
  //       bool fc_success = finalCommit(*transaction);
  //       if (fc_success) {
  //         n_commit.fetch_add(1);
  //         auto now = std::chrono::steady_clock::now();
  //         auto latency =
  //         std::chrono::duration_cast<std::chrono::microseconds>(
  //             now - transaction->startTime);
  //         percentile.add(latency.count());

  //         auto manager_id = id - context.worker_num;

  //         // push to scheduler done_queue for lock release
  //         {
  //           std::lock_guard<std::mutex> lock(
  //               all_executors[transaction->scheduler_id]->mpmc_done_mu);
  //           all_executors[transaction->scheduler_id]->done_queue.push(
  //               transaction);
  //         }

  //       } else if (transaction->will_local_abort()) {
  //         localAbort(*transaction);
  //         transaction->stage = 0;

  //         {
  //           std::lock_guard<std::mutex> lock(
  //               all_executors[transaction->executor_id]->mpmc_transaction_mu);
  //           all_executors[transaction->executor_id]->transaction_queue.push(
  //               transaction);
  //         }
  //       } else {
  //         transaction->stage = 2;
  //         prior_pool.push(transaction);
  //         // __asm volatile("pause" : :);
  //         // LOG(INFO)<<"committer recheck tx: "<<transaction->id<<"ntx:
  //         // "<<NEXT_TX.load();
  //       }
  //     }

  //     if (!done_queue.empty()) {
  //       // std::unique_ptr<TransactionType> _transaction(done_queue.front())
  //       auto _transaction = done_queue.front();
  //       done_queue.pop();

  //       prior_pool.push(_transaction);
  //     }
  //   }
  // }

  void set_all_executors(const std::vector<SpectrumExecutor *> &executors) {
    all_executors = executors;
  }

  std::size_t get_partition_id() {
    std::size_t partition_id;

    CHECK(context.partition_num % context.coordinator_num == 0);

    auto partition_num_per_node =
        context.partition_num / context.coordinator_num;
    partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                       context.coordinator_num +
                   coordinator_id;
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void setupHandlers(TransactionType &txn) {
    txn.readRequestHandler = [this, &txn](SpectrumRWKey &readKey,
                                          std::size_t tid,
                                          uint32_t key_offset) {
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();

      if (context.cold_record_ratio > 0) {
        // simulate disk read
        auto rand = random.uniform_dist(1, 100);
        if (rand <= context.cold_record_ratio) {
          int a = 0;
          for (int i = 0; i < 100; i++) a++;
        }
      }

      ITable *table = db.find_table(table_id, partition_id);
      auto row = table->read(key, &txn);
      SpectrumHelper::read(row, value, table->value_size());
    };

    txn.writeRequestHandler = [this, &txn](SpectrumRWKey &writeKey,
                                           std::size_t tid,
                                           uint32_t key_offset) {
      auto table_id = writeKey.get_table_id();
      auto partition_id = writeKey.get_partition_id();
      const void *key = writeKey.get_key();
      void *value = writeKey.get_value();

      ITable *table = db.find_table(table_id, partition_id);
      int count = 0;
      while (table->lock(key, &txn) == 0) {
        count += 1;
        if (count == 10) {
          txn.add_rollback_key(*static_cast<const evmc::bytes32 *>(key), 0);
          break;
        }
      }
    };
  }

  std::size_t get_available_worker(std::size_t request_id) {
    // assume there are n lock managers and m workers
    // 0, 1, .. n-1 are lock managers
    // n, n + 1, .., n + m -1 are workers

    // the first lock managers assign transactions to n, .. , n + m/n - 1

    auto start_worker_id = n_lock_manager + n_workers / n_lock_manager * id;
    auto len = n_workers / n_lock_manager;
    return request_id % len + start_worker_id;
  }

  std::size_t get_available_worker_for_committer(std::size_t request_id,
                                                 std::size_t manager_id) {
    // assume there are n lock managers and m workers
    // 0, 1, .. n-1 are lock managers
    // n, n + 1, .., n + m -1 are workers

    // the first lock managers assign transactions to n, .. , n + m/n - 1

    auto start_worker_id =
        n_lock_manager + n_workers / n_lock_manager * manager_id;
    auto len = n_workers / n_lock_manager;
    return request_id % len + start_worker_id;
  }

  void set_lock_manager_bit(int id) {
    uint32_t old_value, new_value;
    do {
      old_value = lock_manager_status.load();
      DCHECK(((old_value >> id) & 1) == 0);
      new_value = old_value | (1 << id);
    } while (!lock_manager_status.compare_exchange_weak(old_value, new_value));
  }

  bool get_lock_manager_bit(int id) {
    return (lock_manager_status.load() >> id) & 1;
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

 private:
  DatabaseType &db;
  ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch, &lock_manager_status, &worker_status,
      &total_abort;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  std::size_t n_lock_manager, n_workers;
  std::size_t lock_manager_id;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::atomic<bool> &stopFlag;
  std::atomic<uint32_t> &NEXT_TX;

  LockfreeQueue<TransactionType *> transaction_queue;
  std::vector<SpectrumExecutor *> all_executors;

  LockfreeQueue<TransactionType *> done_queue;

  LockfreeQueue<TransactionType *> abort_queue;

  // boost::lockfree::queue<TransactionType *, boost::lockfree::capacity<1024>>
  // done_queue;

  std::mutex mpmc_abort_mu;

  std::mutex mpmc_done_mu;

  std::mutex mpmc_transaction_mu;

  // HashMap<9973, int, std::shared_ptr<std::atomic<uint64_t>>> &tid_map;

  std::vector<std::unique_ptr<SpecPredScheduler>> &spec_scheduler_vec;

};  // namespace dcc

}  // namespace dcc
