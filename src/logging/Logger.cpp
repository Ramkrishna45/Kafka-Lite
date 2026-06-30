#include "Logger.h"

namespace kafka_lite {

std::mutex Logger::mutex_;

void Logger::log(const std::string& level, const std::string& msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::cout << "[" << level << "] " << msg << std::endl;
}

void Logger::info(const std::string& msg) { log("INFO", msg); }
void Logger::warn(const std::string& msg) { log("WARN", msg); }
void Logger::error(const std::string& msg) { log("ERROR", msg); }
void Logger::debug(const std::string& msg) { log("DEBUG", msg); }

} // namespace kafka_lite
