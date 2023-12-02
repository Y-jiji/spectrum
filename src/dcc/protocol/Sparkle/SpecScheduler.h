#pragma once

#include <dcc/protocol/Sparkle/Sparkle.h>

#include <deque>
#include <sstream>
#include <string>
#include <vector>

#include "glog/logging.h"

namespace dcc {

enum LockMode {
  UNLOCKED = 0,
  READ = 1,
  WRITE = 2,
};

#define TABLE_SIZE 100000

class SpecScheduler {
  using TransactionType = SparkleTransaction;

 public:
  SpecScheduler(int s, int e, int contract_type)
      : start_index(s), end_index(e), contract_type(contract_type) {
    for (int i = 0; i < TABLE_SIZE; i++) {
      lock_table_[i] = new std::deque<KeysList>();
      lock_table_[i]->clear();
    }
    ready_txns_.clear();
    txn_waits_.clear();
  }

  ~SpecScheduler() {
    for (int i = 0; i < TABLE_SIZE; i++) {
      delete lock_table_[i];
    }
  }

  int Lock(TransactionType *txn) {
    int not_acquired = 0;
    // Handle read/write lock requests.

    auto wrvec = txn->get_wrset();
    int wr_size = wrvec.size();

    int wr_start_idx = txn->get_wr_locked_idx() + 1;

    for (int i = wr_start_idx; i < wr_size; i++) {
      std::string wr_key = std::to_string(wrvec[i]);

      std::deque<KeysList> *key_requests = lock_table_[Hash(wr_key)];
      std::deque<KeysList>::iterator it;
      for (it = key_requests->begin();
           (it != key_requests->end()) && (it->key != wr_key); ++it) {
      }
      std::deque<LockRequest> *requests;
      if (it == key_requests->end()) {
        requests = new std::deque<LockRequest>();
        requests->clear();
        key_requests->push_back(KeysList(wr_key, requests));
      } else {
        requests = it->locksrequest;
      }

      // Only need to request this if lock txn hasn't already requested it.
      if (requests->empty() || txn != requests->back().txn) {
        requests->push_back(LockRequest(WRITE, txn));
        // Write lock request fails if there is any previous request at all.
        if (requests->size() > 1) not_acquired++;
      }
    }
    txn->set_wr_locked_idx(wr_size - 1);

    auto rdvec = txn->get_rdset();
    int rd_size = rdvec.size();

    int rd_start_idx = txn->get_rd_locked_idx() + 1;

    for (int i = rd_start_idx; i < rd_size; i++) {
      std::string rd_key = std::to_string(rdvec[i]);
      std::deque<KeysList> *key_requests = lock_table_[Hash(rd_key)];

      std::deque<KeysList>::iterator it;
      for (it = key_requests->begin();
           it != key_requests->end() && it->key != rd_key; ++it) {
      }
      std::deque<LockRequest> *requests;
      if (it == key_requests->end()) {
        requests = new std::deque<LockRequest>();
        key_requests->push_back(KeysList(rd_key, requests));
      } else {
        requests = it->locksrequest;
      }

      // Only need to request this if lock txn hasn't already requested it.
      if (requests->empty() || txn != requests->back().txn) {
        requests->push_back(LockRequest(READ, txn));
        // Read lock request fails if there is any previous write request.
        for (std::deque<LockRequest>::iterator it = requests->begin();
             it != requests->end(); ++it) {
          if (it->mode == WRITE) {
            not_acquired++;
            break;
          }
        }
      }
    }

    txn->set_rd_locked_idx(rd_size - 1);

    // Record and return the number of locks that the txn is blocked on.
    if (not_acquired > 0) {
      txn_waits_[txn] = not_acquired;
    } else {
      ready_txns_.push_back(txn);
    }
    return not_acquired;
  }

  void Release(const std::string &key, TransactionType *txn) {
    // std::cout << "enter release" << txn->txid() << std::endl;
    // Avoid repeatedly looking up key in the unordered_map.
    std::deque<KeysList> *key_requests = lock_table_[Hash(key)];

    std::deque<KeysList>::iterator it1;
    for (it1 = key_requests->begin();
         it1 != key_requests->end() && it1->key != key; ++it1) {
    }

    if (it1 == key_requests->end()) return;

    std::deque<LockRequest> *requests = it1->locksrequest;

    // Seek to the target request. Note whether any write lock requests precede
    // the target.
    bool write_requests_precede_target = false;
    std::deque<LockRequest>::iterator it;
    for (it = requests->begin(); it != requests->end() && it->txn != txn;
         ++it) {
      if (it->mode == WRITE) write_requests_precede_target = true;
    }

    // If we found the request, erase it. No need to do anything otherwise.
    if (it != requests->end()) {
      std::deque<LockRequest>::iterator target = it;
      ++it;
      // std::cout<<"iterator:"<<it->txn->txid()<<std::endl;
      if (it != requests->end()) {
        std::vector<TransactionType *> new_owners;
        if (target == requests->begin() &&
            (target->mode == WRITE ||
             (target->mode == READ && it->mode == WRITE))) {  // (a) or (b)
          // If a write lock request follows, grant it.
          if (it->mode == WRITE) new_owners.push_back(it->txn);
          // If a sequence of read lock requests follows, grant all of them.
          for (; it != requests->end() && it->mode == READ; ++it)
            new_owners.push_back(it->txn);
        } else if (!write_requests_precede_target && target->mode == WRITE &&
                   it->mode == READ) {  // (c)
          // If a sequence of read lock requests follows, grant all of them.
          for (; it != requests->end() && it->mode == READ; ++it)
            new_owners.push_back(it->txn);
        }

        // Handle txns with newly granted requests that may now be ready to run.
        for (uint64_t j = 0; j < new_owners.size(); j++) {
          txn_waits_[new_owners[j]]--;
          if (txn_waits_[new_owners[j]] == 0) {
            ready_txns_.push_back(new_owners[j]);
            txn_waits_.erase(new_owners[j]);
          }
        }
      }

      // Now it is safe to actually erase the target request.
      requests->erase(target);
      if (requests->size() == 0) {
        delete requests;
        key_requests->erase(it1);
      }
    }
  }

  void Release(TransactionType *txn) {
    CHECK(txn != nullptr) << "txn should not be nullptr";
    auto rdvec = txn->get_rdset();

    int rd_size = rdvec.size();
    for (int i = 0; i < rd_size; i++) {
      Release(std::to_string(rdvec[i]), txn);
    }

    auto wrvec = txn->get_wrset();

    int wr_size = wrvec.size();
    for (int i = 0; i < wr_size; i++) {
      Release(std::to_string(wrvec[i]), txn);
    }
  }

  int Re_Lock(TransactionType *txn) {
    int not_acquired = 0;

    auto wrvec = txn->get_wrset();
    int wr_size = wrvec.size();

    int wr_start_idx = txn->get_wr_locked_idx() + 1;

    // LOG(INFO) << "RE_LOCK: " << txn->id << ", wr_start_idx: " << wr_start_idx
    //           << ", wr_size: " << wr_size;
    for (int i = wr_start_idx; i < wr_size; i++) {
      std::string wr_key = std::to_string(wrvec[i]);

      std::deque<KeysList> *key_requests = lock_table_[Hash(wr_key)];
      std::deque<KeysList>::iterator it;
      for (it = key_requests->begin();
           (it != key_requests->end()) && (it->key != wr_key); ++it) {
      }
      std::deque<LockRequest> *requests;
      if (it == key_requests->end()) {
        requests = new std::deque<LockRequest>();
        requests->clear();
        key_requests->push_back(KeysList(wr_key, requests));
      } else {
        requests = it->locksrequest;
      }

      // Only need to request this if lock txn hasn't already requested it.
      if (requests->empty() || txn != requests->back().txn) {
        // additional rules for re-schedule
        if (requests->empty()) {
          requests->push_back(LockRequest(WRITE, txn));
          // if (requests->size() > 1) not_acquired++;
        } else {
          if (txn->id < requests->front().txn->id) continue;

          std::deque<LockRequest>::iterator target;
          for (std::deque<LockRequest>::iterator it = requests->begin();
               it != requests->end(); ++it) {
            if (it->txn->id < txn->id) {
              // LOG(INFO) << "pre txnid: " << it->txn->id;
              target = it;
            } else {
              break;
            }
          }
          // LOG(INFO) << target->txn->id << " " << txn->id;
          requests->insert(++target, LockRequest(WRITE, txn));
          if (requests->front().txn->id != txn->id) not_acquired++;
          // if (requests->size() > 1) not_acquired++;

          // std::stringstream ss;

          // for (std::deque<LockRequest>::iterator it = requests->begin();
          //      it != requests->end(); ++it) {
          //   ss << it->txn->id << " ";
          // }
          // LOG(INFO) << "requests: " << ss.str();
        }
        // if (txn->id < requests->front().txn->id) continue;
        // requests->push_back(LockRequest(WRITE, txn));
        // // Write lock request fails if there is any previous request at all.
        // if (requests->size() > 1) not_acquired++;
      }
    }

    txn->set_wr_locked_idx(wr_size - 1);

    auto rdvec = txn->get_rdset();
    int rd_size = rdvec.size();

    int rd_start_idx = txn->get_rd_locked_idx() + 1;

    for (int i = rd_start_idx; i < rd_size; i++) {
      std::string rd_key = std::to_string(rdvec[i]);
      std::deque<KeysList> *key_requests = lock_table_[Hash(rd_key)];

      std::deque<KeysList>::iterator it;
      for (it = key_requests->begin();
           it != key_requests->end() && it->key != rd_key; ++it) {
      }
      std::deque<LockRequest> *requests;
      if (it == key_requests->end()) {
        requests = new std::deque<LockRequest>();
        key_requests->push_back(KeysList(rd_key, requests));
      } else {
        requests = it->locksrequest;
      }

      // Only need to request this if lock txn hasn't already requested it.
      // if (requests->empty() || txn != requests->back().txn) {
      if (requests->empty()) {
        requests->push_back(LockRequest(READ, txn));
      } else {
        if (txn->id < requests->front().txn->id) continue;

        std::deque<LockRequest>::iterator target;
        bool prior_write = false;
        target = requests->begin();
        for (std::deque<LockRequest>::iterator it = requests->begin();
             it != requests->end(); ++it) {
          if (it->txn->id < txn->id) {
            target = it;
            if (it->mode == WRITE) {
              prior_write = true;
            }
          } else {
            break;
          }
        }
        if (prior_write) {
          not_acquired++;
        }
        requests->insert(++target, LockRequest(READ, txn));
      }
      // }
    }

    txn->set_rd_locked_idx(rd_size - 1);

    // Record and return the number of locks that the txn is blocked on.
    if (not_acquired > 0) {
      // LOG(INFO) << "RELOCK NOT: " << txn->id;
      txn_waits_[txn] = not_acquired;
    } else {
      // LOG(INFO) << "RELOCK YES: " << txn->id;
      ready_txns_.push_back(txn);
    }

    return not_acquired;
  }

 public:
  /// easy to use switch to lib later
  int Hash(const std::string &key) {
    uint64_t hash = 2166136261;
    for (size_t i = 0; i < key.size(); i++) {
      hash = hash ^ (key[i]);
      hash = hash * 16777619;
    }
    return hash % TABLE_SIZE;
  }

  struct LockRequest {
    LockRequest(LockMode m, TransactionType *t) : txn(t), mode(m) {}
    TransactionType *txn;  // Pointer to txn requesting the lock.
    LockMode mode;  // Specifies whether this is a read or write lock request.
  };

  struct KeysList {
    KeysList(std::string m, std::deque<LockRequest> *t)
        : key(m), locksrequest(t) {}

    std::string key;
    std::deque<LockRequest> *locksrequest;
  };

  std::deque<KeysList> *lock_table_[TABLE_SIZE];
  std::deque<TransactionType *> ready_txns_;
  std::unordered_map<TransactionType *, int> txn_waits_;

  int contract_type;
  int start_index;
  int end_index;
};
}  // namespace dcc
