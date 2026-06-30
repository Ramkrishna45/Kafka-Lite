#include "Topic.h"
#include <functional>
#include <cstdlib>

namespace kafka_lite {

Topic::Topic(const std::string& name, int num_partitions) : name_(name) {
    for (int i = 0; i < num_partitions; ++i) {
        partitions_.push_back(std::make_unique<Partition>(name, i));
    }
}

Topic::~Topic() = default;

PublishResult Topic::publish(Message& msg) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (partitions_.empty()) return {0, 0, false};
    
    uint32_t partition_id = 0;
    if (!msg.key.empty()) {
        std::hash<std::string> hasher;
        partition_id = hasher(msg.key) % partitions_.size();
    } else {
        partition_id = roundRobinCounter_.fetch_add(1) % partitions_.size();
    }
    
    partitions_[partition_id]->append(msg);
    return {partition_id, msg.offset, true};
}

std::vector<Message> Topic::consume(uint32_t partition_id, uint64_t start_offset, size_t max_messages) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (partition_id >= partitions_.size()) {
        return {};
    }
    return partitions_[partition_id]->read(start_offset, max_messages);
}

std::vector<Topic::PartitionMetrics> Topic::get_metrics() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<PartitionMetrics> metrics;
    for (const auto& part : partitions_) {
        metrics.push_back({part->get_id(), part->get_next_offset()});
    }
    return metrics;
}

} // namespace kafka_lite
