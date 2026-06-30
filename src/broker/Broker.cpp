#include "Broker.h"
#include "Config.h"
#include "MetricsManager.h"
#include "Logger.h"
#include <chrono>

namespace kafka_lite {

static auto start_time = std::chrono::steady_clock::now();

Broker::Broker() {
    topic_manager_ = std::make_unique<TopicManager>();
    offset_manager_ = std::make_unique<OffsetManager>();
    group_manager_ = std::make_unique<ConsumerGroupManager>(topic_manager_.get());
    heartbeat_manager_ = std::make_unique<HeartbeatManager>(group_manager_.get());
    retention_manager_ = std::make_unique<RetentionManager>(topic_manager_.get());
}

Broker::~Broker() = default;

PublishResult Broker::publish(const std::string& topic_name, Message& msg) {
    auto topic = topic_manager_->get_topic(topic_name);
    if (!topic) {
        return {0, 0, false};
    }
    
    MetricsManager::record_published(1);
    return topic->publish(msg);
}

void Broker::subscribe(const std::string& group_id, const std::string& topic_name, const std::string& consumer_id) {
    group_manager_->subscribe(group_id, topic_name, consumer_id);
    heartbeat_manager_->heartbeat(consumer_id);
}

void Broker::heartbeat(const std::string& consumer_id) {
    heartbeat_manager_->heartbeat(consumer_id);
}

std::vector<Message> Broker::consume(const std::string& group_id, const std::string& topic_name, const std::string& consumer_id, size_t max_messages) {
    auto topic = topic_manager_->get_topic(topic_name);
    if (!topic) return {};
    
    auto assigned_partitions = group_manager_->get_assignments(group_id, topic_name, consumer_id);
    if (assigned_partitions.empty()) return {};
    
    std::vector<Message> all_messages;
    
    size_t per_partition_max = max_messages / assigned_partitions.size();
    if (per_partition_max == 0) per_partition_max = 1;
    
    for (uint32_t partition_id : assigned_partitions) {
        uint64_t current_offset = offset_manager_->get_offset(group_id, topic_name, partition_id);
        auto messages = topic->consume(partition_id, current_offset, per_partition_max);
        
        if (!messages.empty()) {
            uint64_t next_offset = messages.back().offset + 1;
            offset_manager_->commit_offset(group_id, topic_name, partition_id, next_offset);
            all_messages.insert(all_messages.end(), messages.begin(), messages.end());
        }
        
        if (all_messages.size() >= max_messages) break;
    }
    
    if (!all_messages.empty()) {
        MetricsManager::record_consumed(all_messages.size());
    }
    return all_messages;
}

bool Broker::create_topic(const std::string& name, int num_partitions) {
    if (topic_manager_->get_topic(name)) return false;
    topic_manager_->create_topic(name, num_partitions);
    Logger::info("Topic created: " + name + " with " + std::to_string(num_partitions) + " partitions");
    return true;
}

nlohmann::json Broker::get_metrics() {
    nlohmann::json metrics;
    auto topics = topic_manager_->get_all_topics();
    int total_partitions = 0;
    for (const auto& topic : topics) {
        nlohmann::json partitions_json = nlohmann::json::array();
        for (const auto& part : topic->get_metrics()) {
            partitions_json.push_back({
                {"id", part.id},
                {"messages", part.messages}
            });
            total_partitions++;
        }
        metrics["topics_data"][topic->get_name()] = {
            {"partitions", partitions_json}
        };
    }
    
    metrics["topics"] = topics.size();
    metrics["partitions"] = total_partitions;
    metrics["messagesPublished"] = MetricsManager::get_published();
    metrics["messagesConsumed"] = MetricsManager::get_consumed();
    
    nlohmann::json lag_json = nlohmann::json::object();
    nlohmann::json groups_info = group_manager_->get_groups_info();
    
    for (auto& [group_id, group_data] : groups_info.items()) {
        if (group_data.contains("assignments")) {
            for (auto& [topic_name, assignments] : group_data["assignments"].items()) {
                auto topic = topic_manager_->get_topic(topic_name);
                if (!topic) continue;
                
                auto part_metrics = topic->get_metrics();
                for (auto& [p_str, consumer] : assignments.items()) {
                    uint32_t p_id = std::stoi(p_str);
                    uint64_t consumer_offset = offset_manager_->get_offset(group_id, topic_name, p_id);
                    uint64_t latest_offset = 0;
                    
                    for (const auto& m : part_metrics) {
                        if (m.id == p_id) latest_offset = m.messages;
                    }
                    
                    std::string lag_key = topic_name + "_partition" + p_str;
                    lag_json[group_id][lag_key] = (latest_offset > consumer_offset) ? (latest_offset - consumer_offset) : 0;
                }
            }
        }
    }
    
    metrics["consumerLag"] = lag_json;
    return metrics;
}

nlohmann::json Broker::get_health() {
    nlohmann::json health;
    health["status"] = "healthy";
    auto now = std::chrono::steady_clock::now();
    health["uptimeSeconds"] = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
    
    // Connected clients info could be tracked in TcpServer, 
    // but for simplicity we return health status here.
    return health;
}

nlohmann::json Broker::get_groups_info() {
    return group_manager_->get_groups_info();
}

} // namespace kafka_lite
