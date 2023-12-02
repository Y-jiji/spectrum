#pragma once

#include <dcc/benchmark/evm/Database.h>
#include <dcc/benchmark/evm/Query.h>
#include <dcc/benchmark/evm/Schema.h>
#include <dcc/benchmark/evm/Storage.h>
#include <dcc/common/Operation.h>
#include <dcc/core/Defs.h>
#include <dcc/core/Partitioner.h>
#include <dcc/core/Table.h>

#include <silkworm/common/test_util.hpp>
#include <silkworm/execution/evm.hpp>
#include <sstream>

#include "glog/logging.h"

namespace dcc {
namespace evm {

template <class T>
class Invoke : public T {
 public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  static constexpr std::size_t un_used = 10;
  bool is_noop = false;
  int wr_locked_idx = -1;
  int rd_locked_idx = -1;
  silkworm::EVM evm;

  Invoke(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db,
         ContextType &context, RandomType &random, Partitioner &partitioner,
         Storage &storage)
      : T(coordinator_id, partition_id, partitioner),
        db(db),
        context(context),
        random(random),
        storage(storage),
        partition_id(partition_id),
        query(makeEVMQuery<un_used>()(context, partition_id, random)),
        evm{silkworm::Block{}, db.state_db_wrapper(),
            silkworm::test::kShanghaiConfig} {
    evm.id = id;
  }

  virtual ~Invoke() {
    delete evm.execution_state;
    evm.execution_state = nullptr;
  }

  TransactionResult execute(std::size_t worker_id) override {
    silkworm::Bytes input_code{*silkworm::from_hex(query.INPUT_CODE)};
    evmc::address caller{0x8e4d1ea201b908ab5e1f5a1c3f9f1b4f6c1e9cf1_address};
    evmc::address contract{0x3589d05a1ec4af9f65b0e5554e645707775ee43c_address};

    silkworm::Transaction txn{};
    txn.from = caller;
    txn.to = contract;
    txn.data = input_code;

    uint64_t gas{1'000'000'000};

    // link wrapped txn
    // txn.wrapped_txn = dynamic_cast<SerialTransaction*>(this);
    txn.wrapped_txn = this;
    txn.worker_id = worker_id;
    txn.wrapped_exec_type = exec_type;
    txn.partition_num = context.partition_num;
    if (is_init) {
      txn.is_init = true;
    }
    // before execute clear
    // db.state_db_wrapper().clear_journal_and_substate();
    silkworm::CallResult res{evm.execute(txn, gas)};
    if (!is_replay) {
      CHECK(res.status == EVMC_SUCCESS) << "xx " << evm.id;
      // return TransactionResult::ABORT;
    }

    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    // query = makeYCSBQuery<keys_num>()(context, partition_id, random);
  }

  void set_exec_type(dcc::ExecType exec_type) { this->exec_type = exec_type; }

  void set_input_param(std::string &str) {
    this->query.INPUT_CODE = str;
    this->is_init = true;
  };

  void set_input_param_replay(std::string &str) {
    this->query.INPUT_CODE = str;
    this->is_replay = true;
  };

  // get read set
  std::vector<int32_t> get_rdset() {
    // for (auto x : this->query.RKEY) {
    //   rdvec.push_back(x);
    // }
    return this->query.RKEY;
  };

  // get write set
  std::vector<int32_t> get_wrset() {
    // for (auto x : this->query.WKEY) {
    //   wrvec.push_back(x);
    // }
    return this->query.WKEY;
  };

  int get_wr_locked_idx() { return this->wr_locked_idx; }
  void set_wr_locked_idx(int idx) { this->wr_locked_idx = idx; }

  int get_rd_locked_idx() { return this->rd_locked_idx; }
  void set_rd_locked_idx(int idx) { this->rd_locked_idx = idx; }

  void update_rwset() {
    // maintain reschedule info
    auto &pre_wvec = this->query.pre_WKEY;
    auto pre_wvec_size = pre_wvec.size();

    auto &wvec = this->query.WKEY;
    auto wvec_size = wvec.size();

    // LOG(INFO) << "pre_wvec_size: " << pre_wvec_size
    //           << ", wvec_size: " << wvec_size;

    // std::stringstream ss1;
    // for (int k1 : pre_wvec) {
    //   ss1 << std::to_string(k1) + ", ";
    // }

    // std::stringstream ss2;
    // for (int k1 : wvec) {
    //   ss2 << std::to_string(k1) + ", ";
    // }

    // LOG(INFO) << ss1.str() << " ||| " << ss2.str();
    for (int i = 0; i < pre_wvec_size; ++i) {
      // bool flag = false;
      // for (int j = 0; j < wvec_size; ++j) {
      //   if (pre_wvec[i] == wvec[j]) {
      //     flag = true;
      //     break;
      //   }
      // }
      // if (!flag) {
      wvec.push_back(pre_wvec[i]);
      // }
    }

    pre_wvec.clear();

    auto &pre_rvec = this->query.pre_RKEY;
    auto pre_rvec_size = pre_rvec.size();

    auto &rvec = this->query.RKEY;
    auto rvec_size = rvec.size();

    for (int i = 0; i < pre_rvec_size; ++i) {
      // bool flag = false;
      // for (int j = 0; j < rvec_size; ++j) {
      //   if (pre_rvec[i] == rvec[j]) {
      //     flag = true;
      //     break;
      //   }
      // }
      // if (!flag) {
      rvec.push_back(pre_rvec[i]);
      // }
    }
    pre_rvec.clear();
  }

 private:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  EVMQuery<un_used> query;
  std::size_t id;
  dcc::ExecType exec_type;
  bool is_init{false};
  bool is_replay{false};
};

}  // namespace evm

}  // namespace dcc
