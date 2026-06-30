#pragma once
#include <thread>
#include <atomic>

namespace kafka_lite {

class TopicManager;

class RetentionManager {
public:
    RetentionManager(TopicManager* topic_manager);
    ~RetentionManager();

private:
    TopicManager* topic_manager_;
    std::thread monitor_thread_;
    std::atomic<bool> running_;
    
    void monitor_loop();
    void apply_retention_policies();
};

} // namespace kafka_lite
