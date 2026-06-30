#pragma once
#include <string>
#include <cstdint>
#include <vector>

namespace kafka_lite {

struct Message {
    uint64_t offset = 0;
    uint64_t timestamp = 0;
    std::string key;
    std::string value;
    uint32_t partition = 0;
    
    // Calculate total size when serialized
    size_t size() const {
        // offset(8) + timestamp(8) + partition(4) + key_len(4) + key + val_len(4) + val
        return 8 + 8 + 4 + 4 + key.size() + 4 + value.size();
    }
};

} // namespace kafka_lite
