#include "MetricsManager.h"

namespace kafka_lite {

std::atomic<uint64_t> MetricsManager::total_published_{0};
std::atomic<uint64_t> MetricsManager::total_consumed_{0};

void MetricsManager::record_published(int count) {
    total_published_ += count;
}

void MetricsManager::record_consumed(int count) {
    total_consumed_ += count;
}

uint64_t MetricsManager::get_published() {
    return total_published_.load();
}

uint64_t MetricsManager::get_consumed() {
    return total_consumed_.load();
}

} // namespace kafka_lite
