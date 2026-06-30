#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <set>
#include <nlohmann/json.hpp>

namespace kafka_lite {

class TopicManager;

class ConsumerGroup {
public:
    ConsumerGroup(const std::string& group_id);
    std::string get_id() const { return group_id_; }
    
    void add_consumer(const std::string& topic, const std::string& consumer_id);
    void remove_consumer(const std::string& consumer_id);
    
    void rebalance(TopicManager* topic_manager);
    std::vector<uint32_t> get_assignments(const std::string& topic, const std::string& consumer_id) const;
    
    nlohmann::json to_json() const;
    void from_json(const nlohmann::json& j);

private:
    std::string group_id_;
    // topic -> set of consumer_ids
    std::unordered_map<std::string, std::set<std::string>> topic_consumers_;
    // topic -> (partition -> consumer_id)
    std::unordered_map<std::string, std::unordered_map<uint32_t, std::string>> partition_assignments_;
    
    mutable std::shared_mutex mutex_;
};

class ConsumerGroupManager {
public:
    ConsumerGroupManager(TopicManager* topic_manager);
    ~ConsumerGroupManager();

    void subscribe(const std::string& group_id, const std::string& topic, const std::string& consumer_id);
    void remove_consumer(const std::string& consumer_id); // from all groups
    
    std::vector<uint32_t> get_assignments(const std::string& group_id, const std::string& topic, const std::string& consumer_id);
    
    nlohmann::json get_groups_info();
    
    void load_metadata();
    void save_metadata();

private:
    TopicManager* topic_manager_;
    std::unordered_map<std::string, std::shared_ptr<ConsumerGroup>> groups_;
    mutable std::shared_mutex mutex_;
};

} // namespace kafka_lite
