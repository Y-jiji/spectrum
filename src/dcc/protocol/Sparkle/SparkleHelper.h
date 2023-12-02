#pragma once

#include <dcc/core/Table.h>
#include <dcc/protocol/Sparkle/SparkleRWKey.h>

#include <atomic>
#include <cstring>
#include <tuple>

#include "glog/logging.h"

namespace dcc {

class SparkleHelper {
 public:
  using MetaDataType = std::atomic<uint64_t>;

  static void read(const std::tuple<MetaDataType *, void *> &row, void *dest,
                   std::size_t size) {  //

    // MetaDataType &tid = *std::get<0>(row);
    // LOG(INFO)<<row;
    void *src = std::get<1>(row);
    // LOG(INFO)<<src;
    std::memcpy(dest, src, size);
    return;
  }

  static std::atomic<uint64_t> &get_metadata(ITable *table,
                                             const SparkleRWKey &key) {
    // auto tid = key.get_tid();
    // if (!tid) {
    auto tid = &table->search_metadata(key.get_key());
    // }
    return *tid;
  }

  /* the following functions are for Calvin */

  // assume there are n = 2 lock managers and m = 4 workers
  // the following function maps
  // (2, 2, 4) => 0
  // (3, 2, 4) => 0
  // (4, 2, 4) => 1
  // (5, 2, 4) => 1

  static std::size_t worker_id_to_lock_manager_id(std::size_t id,
                                                  std::size_t n_lock_manager,
                                                  std::size_t n_worker) {
    if (id < n_lock_manager) {
      return id;
    }
    return (id - n_lock_manager) / (n_worker / n_lock_manager);
  }

  static std::size_t partition_id_to_lock_manager_id(
      std::size_t partition_id, std::size_t n_lock_manager,
      std::size_t replica_group_size) {
    return partition_id / replica_group_size % n_lock_manager;
  }

  static bool is_read_locked(uint64_t value) {
    return value & (READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
  }

  static bool is_write_locked(uint64_t value) {
    return value & (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

  static uint64_t read_lock_num(uint64_t value) {
    return (value >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK;
  }

  static uint64_t read_lock_max() { return READ_LOCK_BIT_MASK; }

  static uint64_t read_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_write_locked(old_value) ||
               read_lock_num(old_value) == read_lock_max());
      new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  static uint64_t write_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_read_locked(old_value) || is_write_locked(old_value));
      new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);

    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  static void read_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(is_read_locked(old_value));
      DCHECK(!is_write_locked(old_value));
      new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
  }

  static void write_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    old_value = a.load();
    DCHECK(!is_read_locked(old_value));
    DCHECK(is_write_locked(old_value));
    new_value = old_value - (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
    bool ok = a.compare_exchange_strong(old_value, new_value);
    DCHECK(ok);
  }

  static uint64_t remove_lock_bit(uint64_t value) {
    return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
  }

  static uint64_t remove_read_lock_bit(uint64_t value) {
    return value & ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
  }

  static uint64_t remove_write_lock_bit(uint64_t value) {
    return value & ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

  static bool isReadOnly(AriaFBRWKey &readKey,
                         std::vector<AriaFBRWKey> &writeSet) {
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      if (*static_cast<const evmc::bytes32 *>(readKey.get_key()) ==
          *static_cast<const evmc::bytes32 *>(writeKey.get_key())) {
        return false;
      }
    }
    return true;
  }

 public:
  /*
   * [epoch (24) | read-rts  (20) | write-wts (20)]
   *
   */

  static constexpr int EPOCH_OFFSET = 40;
  static constexpr uint64_t EPOCH_MASK = 0xffffffull;

  static constexpr int RTS_OFFSET = 20;
  static constexpr uint64_t RTS_MASK = 0xfffffull;

  static constexpr int WTS_OFFSET = 0;
  static constexpr uint64_t WTS_MASK = 0xfffffull;

  /* the following masks are for Calvin */

  static constexpr int LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t LOCK_BIT_MASK = 0x3ffull;

  static constexpr int READ_LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

  static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;
};

}  // namespace dcc