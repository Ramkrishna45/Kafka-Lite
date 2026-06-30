#pragma once
#include "TopicManager.h"
#include "OffsetManager.h"
#include "ConsumerGroupManager.h"
#include "HeartbeatManager.h"
#include "RetentionManager.h"
#include <memory>
#include <string>
#include <nlohmann/json.hpp>

namespace kafka_lite {

class Broker {
public:
    Broker();
    ~Broker();

    PublishResult publish(const std::string& topic_name, Message& msg);
    
    std::vector<Message> consume(const std::string& group_id, const std::string& topic_name, const std::string& consumer_id, size_t max_messages = 10);
    
    void subscribe(const std::string& group_id, const std::string& topic_name, const std::string& consumer_id);
    void heartbeat(const std::string& consumer_id);
    
    bool create_topic(const std::string& name, int num_partitions);
    nlohmann::json get_metrics();
    nlohmann::json get_groups_info();
    nlohmann::json get_health();

private:
    std::unique_ptr<TopicManager> topic_manager_;
    std::unique_ptr<OffsetManager> offset_manager_;
    std::unique_ptr<ConsumerGroupManager> group_manager_;
    std::unique_ptr<HeartbeatManager> heartbeat_manager_;
    std::unique_ptr<RetentionManager> retention_manager_;
};

} // namespace kafka_lite
