#pragma once
#include <string>
#include <cstdint>

namespace kafka_lite {

struct Config {
    static constexpr int DEFAULT_PORT = 9092;
    static constexpr const char* DATA_DIR = "data";
    static constexpr const char* METADATA_DIR = "data/metadata";
    static constexpr const char* TOPICS_FILE = "data/metadata/topics.json";
    static constexpr const char* OFFSETS_FILE = "data/metadata/offsets.json";
    
    static constexpr size_t MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB
    static constexpr int DEFAULT_PARTITIONS = 3;
};

} // namespace kafka_lite
