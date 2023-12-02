#pragma once
#include <dcc/common/HashMap.h>
#include <dcc/core/Manager.h>
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Spectrum/SpecPredScheduler.h>
#include <dcc/protocol/Spectrum/Spectrum.h>
#include <dcc/protocol/Spectrum/SpectrumExecutor.h>
#include <dcc/protocol/Spectrum/SpectrumTransaction.h>
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <vector>

namespace dcc {

template <class Workload>
class SpectrumManager : public dcc::Manager {
 public:
  using base_type = dcc::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SpectrumTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  SpectrumManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                  ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0) {
    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
    // incorrect version stored in map (FIXED)
    // storages.resize(100001);
    // transactions.resize(100001);
    NEXT_TX.store(1);
    clear_lock_manager_status();
    LOG(INFO) << "batch size: " << context.batch_size;

    // init lock
    // for (int i = 0; i < context.partition_num; i++) {
    //   for (int j = 1; j <= context.keysPerPartition; j++) {
    //     int key = j - 1 + context.keysPerPartition * i;

    //     auto tid = std::make_shared<std::atomic<uint64_t>>(0);

    //     tid_map.insert(key, tid);
    //   }
    // }

    CHECK(context.sparkle_lock_manager == context.partition_num);

    if (context.partition_num > 1) {
      CHECK(context.global_key_space == false);
    }

    for (int i = 0; i < context.sparkle_lock_manager; i++) {
      auto spec_scheduler = std::make_unique<SpecPredScheduler>(
          context.scheduled_start_index, context.scheduled_end_index,
          context.contract_type);

      spec_scheduler_vec.push_back(std::move(spec_scheduler));
    }
  }

  void clear_lock_manager_status() { lock_manager_status.store(0); }

 public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::atomic<uint32_t> lock_manager_status;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
  std::atomic<uint32_t> NEXT_TX;

  std::vector<std::unique_ptr<SpecPredScheduler>> spec_scheduler_vec;
};
}  // namespace dcc