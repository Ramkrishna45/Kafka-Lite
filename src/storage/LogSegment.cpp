#include "LogSegment.h"
#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;

namespace kafka_lite {

LogSegment::LogSegment(const std::string& filepath) : filepath_(filepath) {
    if (!fs::exists(fs::path(filepath_).parent_path())) {
        fs::create_directories(fs::path(filepath_).parent_path());
    }
    
    // Open for read and write, append mode, binary
    file_.open(filepath_, std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
    if (!file_.is_open()) {
        // Create it if it doesn't exist
        file_.clear();
        file_.open(filepath_, std::ios::out | std::ios::binary);
        file_.close();
        file_.open(filepath_, std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
    }
    
    if (file_.is_open()) {
        file_.seekg(0, std::ios::end);
        current_size_ = file_.tellg();
    }
}

LogSegment::~LogSegment() {
    close();
}

void LogSegment::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (file_.is_open()) {
        file_.close();
    }
}

void LogSegment::serialize_message(const Message& msg, std::ostream& os) {
    os.write(reinterpret_cast<const char*>(&msg.offset), sizeof(msg.offset));
    os.write(reinterpret_cast<const char*>(&msg.timestamp), sizeof(msg.timestamp));
    os.write(reinterpret_cast<const char*>(&msg.partition), sizeof(msg.partition));
    
    uint32_t key_len = msg.key.size();
    os.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    os.write(msg.key.data(), key_len);
    
    uint32_t val_len = msg.value.size();
    os.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
    os.write(msg.value.data(), val_len);
}

std::optional<Message> LogSegment::deserialize_message(std::istream& is) {
    Message msg;
    if (!is.read(reinterpret_cast<char*>(&msg.offset), sizeof(msg.offset))) return std::nullopt;
    if (!is.read(reinterpret_cast<char*>(&msg.timestamp), sizeof(msg.timestamp))) return std::nullopt;
    if (!is.read(reinterpret_cast<char*>(&msg.partition), sizeof(msg.partition))) return std::nullopt;
    
    uint32_t key_len;
    if (!is.read(reinterpret_cast<char*>(&key_len), sizeof(key_len))) return std::nullopt;
    msg.key.resize(key_len);
    if (key_len > 0 && !is.read(msg.key.data(), key_len)) return std::nullopt;
    
    uint32_t val_len;
    if (!is.read(reinterpret_cast<char*>(&val_len), sizeof(val_len))) return std::nullopt;
    msg.value.resize(val_len);
    if (val_len > 0 && !is.read(msg.value.data(), val_len)) return std::nullopt;
    
    return msg;
}

bool LogSegment::append(const Message& msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!file_.is_open()) return false;
    
    file_.seekp(0, std::ios::end);
    serialize_message(msg, file_);
    file_.flush();
    
    current_size_ = file_.tellp();
    return true;
}

std::vector<Message> LogSegment::read(uint64_t start_offset, size_t max_messages) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Message> messages;
    if (!file_.is_open()) return messages;
    
    file_.seekg(0, std::ios::beg);
    
    while (messages.size() < max_messages) {
        auto msg_opt = deserialize_message(file_);
        if (!msg_opt) break; // EOF or error
        
        if (msg_opt->offset >= start_offset) {
            messages.push_back(*msg_opt);
        }
    }
    
    file_.clear(); // Clear EOF flags
    return messages;
}

uint64_t LogSegment::get_size() const {
    return current_size_;
}

} // namespace kafka_lite
