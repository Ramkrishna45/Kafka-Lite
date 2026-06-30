#include "ConsumerGroupManager.h"
#include "TopicManager.h"
#include "Config.h"
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <iostream>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace kafka_lite {

// --- ConsumerGroup ---

ConsumerGroup::ConsumerGroup(const std::string& group_id) : group_id_(group_id) {}

void ConsumerGroup::add_consumer(const std::string& topic, const std::string& consumer_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    topic_consumers_[topic].insert(consumer_id);
}

void ConsumerGroup::remove_consumer(const std::string& consumer_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto& [topic, consumers] : topic_consumers_) {
        consumers.erase(consumer_id);
    }
}

void ConsumerGroup::rebalance(TopicManager* topic_manager) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    partition_assignments_.clear();
    
    for (const auto& [topic_name, consumers] : topic_consumers_) {
        if (consumers.empty()) continue;
        
        auto topic = topic_manager->get_topic(topic_name);
        if (!topic) continue;
        
        int num_partitions = topic->get_num_partitions();
        std::vector<std::string> sorted_consumers(consumers.begin(), consumers.end());
        
        for (int p = 0; p < num_partitions; ++p) {
            std::string assigned_consumer = sorted_consumers[p % sorted_consumers.size()];
            partition_assignments_[topic_name][p] = assigned_consumer;
        }
    }
}

std::vector<uint32_t> ConsumerGroup::get_assignments(const std::string& topic, const std::string& consumer_id) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<uint32_t> assigned_partitions;
    
    auto it = partition_assignments_.find(topic);
    if (it != partition_assignments_.end()) {
        for (const auto& [partition_id, c_id] : it->second) {
            if (c_id == consumer_id) {
                assigned_partitions.push_back(partition_id);
            }
        }
    }
    return assigned_partitions;
}

nlohmann::json ConsumerGroup::to_json() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    json j;
    
    json consumers_j = json::object();
    for (const auto& [topic, consumers] : topic_consumers_) {
        consumers_j[topic] = consumers;
    }
    j["consumers"] = consumers_j;
    
    json assignments_j = json::object();
    for (const auto& [topic, assignments] : partition_assignments_) {
        json tp_j = json::object();
        for (const auto& [p, c] : assignments) {
            tp_j[std::to_string(p)] = c;
        }
        assignments_j[topic] = tp_j;
    }
    j["assignments"] = assignments_j;
    
    return j;
}

void ConsumerGroup::from_json(const nlohmann::json& j) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (j.contains("consumers")) {
        for (auto& [topic, consumers] : j["consumers"].items()) {
            for (const auto& c : consumers) {
                topic_consumers_[topic].insert(c.get<std::string>());
            }
        }
    }
    if (j.contains("assignments")) {
        for (auto& [topic, assignments] : j["assignments"].items()) {
            for (auto& [p_str, c] : assignments.items()) {
                partition_assignments_[topic][std::stoi(p_str)] = c.get<std::string>();
            }
        }
    }
}

// --- ConsumerGroupManager ---

ConsumerGroupManager::ConsumerGroupManager(TopicManager* topic_manager) : topic_manager_(topic_manager) {
    if (!fs::exists(Config::METADATA_DIR)) {
        fs::create_directories(Config::METADATA_DIR);
    }
    load_metadata();
}

ConsumerGroupManager::~ConsumerGroupManager() {
    save_metadata();
}

void ConsumerGroupManager::subscribe(const std::string& group_id, const std::string& topic, const std::string& consumer_id) {
    std::shared_ptr<ConsumerGroup> group;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (groups_.find(group_id) == groups_.end()) {
            groups_[group_id] = std::make_shared<ConsumerGroup>(group_id);
        }
        group = groups_[group_id];
    }
    
    group->add_consumer(topic, consumer_id);
    group->rebalance(topic_manager_);
    save_metadata();
}

void ConsumerGroupManager::remove_consumer(const std::string& consumer_id) {
    std::vector<std::shared_ptr<ConsumerGroup>> groups_to_rebalance;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        for (const auto& [group_id, group] : groups_) {
            group->remove_consumer(consumer_id);
            groups_to_rebalance.push_back(group);
        }
    }
    
    for (auto& group : groups_to_rebalance) {
        group->rebalance(topic_manager_);
    }
    save_metadata();
}

std::vector<uint32_t> ConsumerGroupManager::get_assignments(const std::string& group_id, const std::string& topic, const std::string& consumer_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
        return it->second->get_assignments(topic, consumer_id);
    }
    return {};
}

nlohmann::json ConsumerGroupManager::get_groups_info() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    json j = json::object();
    for (const auto& [group_id, group] : groups_) {
        j[group_id] = group->to_json();
    }
    return j;
}

void ConsumerGroupManager::load_metadata() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::string filepath = std::string(Config::METADATA_DIR) + "/groups.json";
    if (fs::exists(filepath)) {
        std::ifstream file(filepath);
        if (file.is_open()) {
            try {
                json j;
                file >> j;
                for (auto& [group_id, group_j] : j.items()) {
                    auto group = std::make_shared<ConsumerGroup>(group_id);
                    group->from_json(group_j);
                    groups_[group_id] = group;
                }
            } catch (const json::exception&) {}
        }
    }
}

void ConsumerGroupManager::save_metadata() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    json j = json::object();
    for (const auto& [group_id, group] : groups_) {
        j[group_id] = group->to_json();
    }
    
    std::string filepath = std::string(Config::METADATA_DIR) + "/groups.json";
    std::ofstream file(filepath);
    if (file.is_open()) {
        file << j.dump(4);
    }
}

} // namespace kafka_lite
