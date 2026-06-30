#include "OffsetManager.h"
#include "Config.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace kafka_lite {

OffsetManager::OffsetManager() {
    if (!fs::exists(Config::METADATA_DIR)) {
        fs::create_directories(Config::METADATA_DIR);
    }
    load_offsets();
}

OffsetManager::~OffsetManager() {
    save_offsets();
}

void OffsetManager::commit_offset(const std::string& group_id, const std::string& topic, uint32_t partition, uint64_t offset) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    offsets_[group_id][make_key(topic, partition)] = offset;
}

uint64_t OffsetManager::get_offset(const std::string& group_id, const std::string& topic, uint32_t partition) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto group_it = offsets_.find(group_id);
    if (group_it != offsets_.end()) {
        std::string key = make_key(topic, partition);
        auto offset_it = group_it->second.find(key);
        if (offset_it != group_it->second.end()) {
            return offset_it->second;
        }
    }
    return 0; // Default offset if not found
}

void OffsetManager::load_offsets() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (fs::exists(Config::OFFSETS_FILE)) {
        std::ifstream file(Config::OFFSETS_FILE);
        if (file.is_open()) {
            json j;
            try {
                file >> j;
                for (auto& [group_id, group_offsets] : j.items()) {
                    for (auto& [topic_partition, offset] : group_offsets.items()) {
                        offsets_[group_id][topic_partition] = offset.get<uint64_t>();
                    }
                }
            } catch (const json::exception& e) {
                std::cerr << "Failed to parse offsets.json: " << e.what() << std::endl;
            }
        }
    }
}

void OffsetManager::save_offsets() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    json j;
    for (const auto& [group_id, group_offsets] : offsets_) {
        for (const auto& [topic_partition, offset] : group_offsets) {
            j[group_id][topic_partition] = offset;
        }
    }
    
    std::ofstream file(Config::OFFSETS_FILE);
    if (file.is_open()) {
        file << j.dump(4);
    }
}

} // namespace kafka_lite
