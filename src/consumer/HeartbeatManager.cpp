#include "HeartbeatManager.h"
#include "ConsumerGroupManager.h"
#include <iostream>

namespace kafka_lite {

HeartbeatManager::HeartbeatManager(ConsumerGroupManager* group_manager) 
    : group_manager_(group_manager), running_(true) {
    monitor_thread_ = std::thread(&HeartbeatManager::monitor_loop, this);
}

HeartbeatManager::~HeartbeatManager() {
    running_ = false;
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }
}

void HeartbeatManager::heartbeat(const std::string& consumer_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    last_heartbeats_[consumer_id] = std::chrono::system_clock::now();
}

void HeartbeatManager::monitor_loop() {
    while (running_) {
        // Sleep in small chunks to allow quick exit on shutdown
        for(int i = 0; i < 50 && running_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!running_) break;
        
        auto now = std::chrono::system_clock::now();
        std::vector<std::string> dead_consumers;
        
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            for (const auto& [consumer_id, last_hb] : last_heartbeats_) {
                auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_hb).count();
                if (duration > 15) {
                    dead_consumers.push_back(consumer_id);
                }
            }
        }
        
        if (!dead_consumers.empty()) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            for (const auto& c_id : dead_consumers) {
                std::cout << "Consumer " << c_id << " timed out. Removing and rebalancing." << std::endl;
                last_heartbeats_.erase(c_id);
                group_manager_->remove_consumer(c_id);
            }
        }
    }
}

} // namespace kafka_lite
