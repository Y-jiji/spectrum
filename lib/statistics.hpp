#pragma once
#include <atomic>
#include <chrono>
#include "./lock-util.hpp"
#include <set>
#include <iterator>
#include <array>

namespace spectrum {


class Statistics {

    private:
    static const int SAMPLE = 1000;
    std::atomic<size_t> count_commit{0};
    std::atomic<size_t> count_execution{0};
    std::atomic<size_t> count_latency_25us{0};
    std::atomic<size_t> count_latency_50us{0};
    std::atomic<size_t> count_latency_100us{0};
    std::atomic<size_t> count_latency_100us_above{0};
    std::array<std::atomic<size_t>, SAMPLE> sample_latency;

    public:
    Statistics() = default;
    Statistics(const Statistics& statistics) = delete;
    void JournalCommit(size_t latency);
    void JournalExecute();
    std::string Print();
    std::string PrintWithDuration(std::chrono::milliseconds duration);

};

} // namespace spectrum
