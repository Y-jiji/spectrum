#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <list>
#include <atomic>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <thread>
#include <barrier>

namespace spectrum {

// some shorthands to prevent prohibitively long names
#define K std::tuple<evmc::address, evmc::bytes32>
#define V SpectrumPreSchedVersionList
#define T SpectrumPreSchedTransaction

using namespace std::chrono;

struct SpectrumPreSchedPutTuple {
    K               key;
    evmc::bytes32   value;
    bool            is_committed;
};

struct SpectrumPreSchedGetTuple {
    K               key;
    evmc::bytes32   value;
    size_t          version;
    size_t          tuples_put_len;
    size_t          checkpoint_id;
};

struct SpectrumPreSchedTransaction: public Transaction {
    size_t      id;
    size_t      should_wait{0};
    SpinLock            rerun_keys_mu;
    std::vector<K>      rerun_keys;
    std::atomic<bool>   berun_flag{false};
    time_point<steady_clock>            start_time;
    std::vector<SpectrumPreSchedGetTuple>       tuples_get{};
    std::vector<SpectrumPreSchedPutTuple>       tuples_put{};
    SpectrumPreSchedTransaction(Transaction&& inner, size_t id);
    bool HasWAR();
    void SetWAR(const K& key, size_t cause_id, bool pre_schedule);
};

struct SpectrumPreSchedEntry {
    evmc::bytes32   value;
    size_t          version;
    // we store raw pointers here because when a transaction is destructed, it always removes itself from table. 
    std::unordered_set<T*>  readers;
};

struct SpectrumPreSchedVersionList {
    T*          tx = nullptr;
    std::list<SpectrumPreSchedEntry> entries;
    // readers that read default value
    std::unordered_set<T*>  readers_default;
};

struct SpectrumPreSchedTable: private Table<K, V, KeyHasher> {

    SpectrumPreSchedTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);

};

struct SpectrumPreSchedLockTable: private Table<K, V, KeyHasher> {

    SpectrumPreSchedLockTable(size_t partitions);
    void Get(T* tx, const K& k);
    void Put(T* tx, const K& k);
    void ClearPut(T* tx, const K& k);

};

using SpectrumPreSchedQueue = LockPriorityQueue<T>;
class SpectrumPreSchedExecutor;

class SpectrumPreSched: public Protocol {

    private:
    size_t                  num_executors;
    Workload&               workload;
    SpectrumPreSchedTable   table;
    Statistics&             statistics;
    std::atomic<size_t>     last_executed{1};
    std::atomic<size_t>     last_finalized{0};
    std::atomic<size_t>     last_scheduled{1};
    std::atomic<bool>       stop_flag{false};
    SpectrumPreSchedLockTable                       lock_table;
    std::vector<SpectrumPreSchedQueue>              queue_bundle;
    std::vector<std::thread>                        executors{};
    std::barrier<std::function<void()>>             stop_latch;
    friend class SpectrumPreSchedExecutor;

    public:
    SpectrumPreSched(Workload& workload, Statistics& statistics, size_t num_executors, size_t table_partitions, EVMType evm_type);
    void Start() override;
    void Stop() override;

};

class SpectrumPreSchedExecutor {

    private:
    Workload&               workload;
    SpectrumPreSchedTable&  table;
    Statistics&             statistics;
    std::atomic<size_t>&    last_executed;
    std::atomic<size_t>&    last_scheduled;
    std::atomic<size_t>&    last_finalized;
    std::atomic<bool>&      stop_flag;
    SpectrumPreSchedQueue&  queue;
    SpectrumPreSchedLockTable&                      lock_table;
    std::vector<SpectrumPreSchedQueue>&             queue_bundle;
    std::unique_ptr<T>                              tx{nullptr};
    std::barrier<std::function<void()>>&            stop_latch;

    public:
    SpectrumPreSchedExecutor(SpectrumPreSched& spectrum, SpectrumPreSchedQueue& queue);
    void Finalize();
    void Generate();
    void ReExecute();
    void Run();

};

#undef T
#undef V
#undef K

} // namespace spectrum
