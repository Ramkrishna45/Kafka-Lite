#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>

namespace kafka_lite {

class ConsumerGroupManager;

class HeartbeatManager {
public:
    HeartbeatManager(ConsumerGroupManager* group_manager);
    ~HeartbeatManager();

    void heartbeat(const std::string& consumer_id);

private:
    ConsumerGroupManager* group_manager_;
    std::unordered_map<std::string, std::chrono::time_point<std::chrono::system_clock>> last_heartbeats_;
    mutable std::shared_mutex mutex_;
    
    std::thread monitor_thread_;
    std::atomic<bool> running_;
    
    void monitor_loop();
};

} // namespace kafka_lite
