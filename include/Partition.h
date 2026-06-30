#pragma once
#include "LogSegment.h"
#include <string>
#include <memory>
#include <atomic>

namespace kafka_lite {

class Partition {
public:
    Partition(const std::string& topic_name, uint32_t partition_id);
    ~Partition();

    void append(Message& msg); // Updates msg.offset before appending
    std::vector<Message> read(uint64_t start_offset, size_t max_messages);

    uint32_t get_id() const { return partition_id_; }
    uint64_t get_next_offset() const { return next_offset_.load(); }

private:
    std::string topic_name_;
    uint32_t partition_id_;
    std::unique_ptr<LogSegment> active_segment_;
    std::atomic<uint64_t> next_offset_;
    std::mutex append_mutex_;
    
    void load_segment();
};

} // namespace kafka_lite
