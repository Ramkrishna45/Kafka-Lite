#pragma once
#include "Message.h"
#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include <optional>

namespace kafka_lite {

class LogSegment {
public:
    LogSegment(const std::string& filepath);
    ~LogSegment();

    // Append a message. Returns true on success.
    bool append(const Message& msg);

    // Read messages starting from a specific offset up to max_messages
    std::vector<Message> read(uint64_t start_offset, size_t max_messages);

    uint64_t get_size() const;
    void close();

private:
    std::string filepath_;
    std::fstream file_;
    std::mutex mutex_;
    uint64_t current_size_ = 0;

    void serialize_message(const Message& msg, std::ostream& os);
    std::optional<Message> deserialize_message(std::istream& is);
};

} // namespace kafka_lite
