#include "Partition.h"
#include "Config.h"
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

namespace kafka_lite {

Partition::Partition(const std::string& topic_name, uint32_t partition_id)
    : topic_name_(topic_name), partition_id_(partition_id), next_offset_(0) {
    load_segment();
}

Partition::~Partition() = default;

void Partition::load_segment() {
    std::string dir_path = std::string(Config::DATA_DIR) + "/" + topic_name_;
    if (!fs::exists(dir_path)) {
        fs::create_directories(dir_path);
    }
    
    std::string log_file = dir_path + "/partition" + std::to_string(partition_id_) + ".log";
    active_segment_ = std::make_unique<LogSegment>(log_file);
    
    uint64_t last_offset = 0;
    bool has_messages = false;
    
    std::vector<Message> all_msgs = active_segment_->read(0, static_cast<size_t>(-1));
    if (!all_msgs.empty()) {
        last_offset = all_msgs.back().offset;
        has_messages = true;
    }
    
    next_offset_.store(has_messages ? last_offset + 1 : 0);
}

void Partition::append(Message& msg) {
    std::lock_guard<std::mutex> lock(append_mutex_);
    msg.offset = next_offset_.fetch_add(1);
    msg.partition = partition_id_;
    active_segment_->append(msg);
}

std::vector<Message> Partition::read(uint64_t start_offset, size_t max_messages) {
    return active_segment_->read(start_offset, max_messages);
}

} // namespace kafka_lite
