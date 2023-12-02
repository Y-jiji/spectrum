#pragma once

#include <dcc/common/LockfreeQueue.h>
#include <dcc/common/Message.h>
#include <dcc/common/Socket.h>
#include <dcc/core/ControlMessage.h>
#include <dcc/core/Dispatcher.h>
#include <dcc/core/Executor.h>
#include <dcc/core/Worker.h>
#include <dcc/core/factory/WorkerFactory.h>
#include <glog/logging.h>

#include <thread>
#include <vector>

namespace dcc {

class Coordinator {
 public:
  template <class Database, class Context>
  Coordinator(std::size_t id, Database &db, Context &context)
      : id(id),
        coordinator_num(context.peers.size()),
        peers(context.peers),
        context(context) {
    workerStopFlag.store(false);
    ioStopFlag.store(false);
    workers = WorkerFactory::create_workers(id, db, context, workerStopFlag);
  }

  ~Coordinator() = default;

  template <class Database, class Context>
  void prepare_tables(std::size_t id, Database &db, Context &context) {
    WorkerFactory::init_tables(id, db, context);
  }

  void start() {
    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

    for (auto i = 0u; i < workers.size(); i++) {
      if (i == workers.size() - 1 &&
          (context.protocol == "Serial" || context.protocol == "Sparkle")) {
        LOG(INFO) << "Protocol Serial/Sparkle detected! Ignore manager "
                     "initialization.";
        continue;
      }

      threads.emplace_back(&Worker::start, workers[i].get());
      if (context.cpu_affinity) {
        // if (i > context.sparkle_lock_manager)
        pin_thread_to_core(threads[i]);
      }
    }

    // run timeToRun seconds
    int timeToRun = context.time_to_run;
    auto warmup = 0, cooldown = 0;
    if (context.replay_transaction) {
      timeToRun *= 2;
    }
    auto startTime = std::chrono::steady_clock::now();

    uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0,
             total_abort_read_validation = 0, total_local = 0,
             total_operations = 0, total_si_in_serializable = 0,
             total_network_size = 0;
    int count = 0;

    do {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      uint64_t n_commit = 0, n_abort_no_retry = 0, n_abort_lock = 0,
               n_abort_read_validation = 0, n_local = 0, n_operations = 0,
               n_si_in_serializable = 0, n_network_size = 0;
      uint64_t partial_revert[20] = {0};

      for (auto i = 0u; i < workers.size(); ++i) {
        LOG(INFO) << "worker " << i << ", " << workers[i]->n_commit;
        n_operations += workers[i]->n_operations.exchange(0);
        n_commit += workers[i]->n_commit.exchange(0);
        n_abort_no_retry += workers[i]->n_abort_no_retry.exchange(0);
        n_abort_lock += workers[i]->n_abort_lock.exchange(0);
        n_abort_read_validation +=
            workers[i]->n_abort_read_validation.exchange(0);
        n_local += workers[i]->n_local.exchange(0);
        n_si_in_serializable += workers[i]->n_si_in_serializable.exchange(0);
        n_network_size += workers[i]->n_network_size.exchange(0);
        if (context.protocol == "Sparkle" || context.protocol == "Spectrum") {
          for (auto j = 0u; j < 20; ++j) {
            partial_revert[j] += workers[i]->partial_revert[j].exchange(0);
          }
        }
      }

      LOG(INFO) << "commit: " << n_commit << " abort: "
                << n_abort_no_retry + n_abort_lock + n_abort_read_validation
                << " (" << n_abort_no_retry << "/" << n_abort_lock << "/"
                << n_abort_read_validation
                << "), network size: " << n_network_size
                << ", avg network size: " << 1.0 * n_network_size / n_commit
                << ", si_in_serializable: " << n_si_in_serializable << " "
                << 100.0 * n_si_in_serializable / n_commit << " %"
                << ", local: " << 100.0 * n_local / n_commit << " %"
                << ", operations: : " << n_operations
                << "\n\tpartial revert 0  " << partial_revert[0]
                << "\n\tpartial revert 1  " << partial_revert[1]
                << "\n\tpartial revert 2  " << partial_revert[2]
                << "\n\tpartial revert 3  " << partial_revert[3]
                << "\n\tpartial revert 4  " << partial_revert[4]
                << "\n\tpartial revert 5  " << partial_revert[5]
                << "\n\tpartial revert 6  " << partial_revert[6]
                << "\n\tpartial revert 7  " << partial_revert[7]
                << "\n\tpartial revert 8  " << partial_revert[8]
                << "\n\tpartial revert 9  " << partial_revert[9]
                << "\n\tpartial revert 10 " << partial_revert[10] << std::endl;
      count++;
      if (count > warmup && count <= timeToRun - cooldown) {
        total_commit += n_commit;
        total_abort_no_retry += n_abort_no_retry;
        total_abort_lock += n_abort_lock;
        total_abort_read_validation += n_abort_read_validation;
        total_local += n_local;
        total_si_in_serializable += n_si_in_serializable;
        total_network_size += n_network_size;
      }

    } while (std::chrono::duration_cast<std::chrono::seconds>(
                 std::chrono::steady_clock::now() - startTime)
                 .count() < timeToRun);

    count = timeToRun - warmup - cooldown;

    LOG(INFO) << "average commit: " << 1.0 * total_commit / count << " abort: "
              << 1.0 *
                     (total_abort_no_retry + total_abort_lock +
                      total_abort_read_validation) /
                     count
              << " (" << 1.0 * total_abort_no_retry / count << "/"
              << 1.0 * total_abort_lock / count << "/"
              << 1.0 * total_abort_read_validation / count
              << "), network size: " << total_network_size
              << ", operations: " << total_operations << ", avg network size: "
              << 1.0 * total_network_size / total_commit
              << ", si_in_serializable: " << total_si_in_serializable << " "
              << 100.0 * total_si_in_serializable / total_commit << " %"
              << ", local: " << 100.0 * total_local / total_commit << " %";

    workerStopFlag.store(true);

    for (auto i = 0u; i < threads.size(); i++) {
      workers[i]->onExit();
      threads[i].join();
    }

    // gather throughput
    // double sum_commit = gather(1.0 * total_commit / count);
    // if (id == 0) {
    //   LOG(INFO) << "total commit: " << sum_commit;
    // }

    // make sure all messages are sent
    // std::this_thread::sleep_for(std::chrono::seconds(1));

    ioStopFlag.store(true);

    LOG(INFO) << "Coordinator exits.";
  }

 private:
  void pin_thread_to_core(std::thread &t) {
#ifndef __APPLE__
    static std::size_t core_id = context.cpu_core_id;
    LOG(INFO) << "core_id: " << core_id;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    auto _core_id = core_id;
    ++core_id;
    CPU_SET(core_id, &cpuset);
    int rc =
        pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    CHECK(rc == 0);
#endif
  }

 private:
  std::size_t id, coordinator_num;
  const std::vector<std::string> &peers;
  Context &context;
  std::vector<std::vector<Socket>> inSockets, outSockets;
  std::atomic<bool> workerStopFlag, ioStopFlag;
  std::vector<std::shared_ptr<Worker>> workers;
};
}  // namespace dcc
