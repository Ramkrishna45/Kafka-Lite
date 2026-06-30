#pragma once
#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <cstdint>

namespace kafka_lite {

class OffsetManager {
public:
    OffsetManager();
    ~OffsetManager();

    void commit_offset(const std::string& group_id, const std::string& topic, uint32_t partition, uint64_t offset);
    uint64_t get_offset(const std::string& group_id, const std::string& topic, uint32_t partition);
    
    void load_offsets();
    void save_offsets();

private:
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> offsets_;
    mutable std::shared_mutex mutex_;
    
    std::string make_key(const std::string& topic, uint32_t partition) const {
        return topic + "_" + std::to_string(partition);
    }
};

} // namespace kafka_lite
