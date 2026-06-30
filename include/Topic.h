#pragma once
#include "Partition.h"
#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include <atomic>

namespace kafka_lite {

struct PublishResult {
    uint32_t partition;
    uint64_t offset;
    bool success;
};

class Topic {
public:
    Topic(const std::string& name, int num_partitions);
    ~Topic();

    PublishResult publish(Message& msg);
    std::vector<Message> consume(uint32_t partition_id, uint64_t start_offset, size_t max_messages);

    std::string get_name() const { return name_; }
    int get_num_partitions() const { return partitions_.size(); }

    struct PartitionMetrics {
        uint32_t id;
        uint64_t messages;
    };
    std::vector<PartitionMetrics> get_metrics();

private:
    std::string name_;
    std::vector<std::unique_ptr<Partition>> partitions_;
    mutable std::shared_mutex mutex_;
    std::atomic<uint64_t> roundRobinCounter_{0};
};

} // namespace kafka_lite
