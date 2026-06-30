#pragma once
#include <string>
#include <mutex>
#include <iostream>

namespace kafka_lite {

class Logger {
public:
    static void info(const std::string& msg);
    static void warn(const std::string& msg);
    static void error(const std::string& msg);
    static void debug(const std::string& msg);

private:
    static std::mutex mutex_;
    static void log(const std::string& level, const std::string& msg);
};

} // namespace kafka_lite
