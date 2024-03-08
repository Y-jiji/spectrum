#pragma once
#include <atomic>
#include <chrono>
#include <mutex>

namespace spectrum {

class Statistics {
    public:
    std::mutex mu;
    size_t count_commit{0};
    float  count_execution{0};
    size_t count_latency_25ms{0};
    size_t count_latency_50ms{0};
    size_t count_latency_100ms{0};
    size_t count_latency_100ms_above{0};
    Statistics() = default;
    Statistics(const Statistics& statistics) = delete;
    void JournalCommit(size_t latency);
    void JournalExecute();
    std::string Print();
    std::string PrintWithDuration(std::chrono::milliseconds duration);
};

} // namespace spectrum