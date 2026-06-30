#pragma once
#include <atomic>
#include <cstdint>

namespace kafka_lite {

class MetricsManager {
public:
    static void record_published(int count = 1);
    static void record_consumed(int count = 1);
    
    static uint64_t get_published();
    static uint64_t get_consumed();

private:
    static std::atomic<uint64_t> total_published_;
    static std::atomic<uint64_t> total_consumed_;
};

} // namespace kafka_lite
