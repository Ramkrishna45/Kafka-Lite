#include "TopicManager.h"
#include "Config.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace kafka_lite {

TopicManager::TopicManager() {
    if (!fs::exists(Config::METADATA_DIR)) {
        fs::create_directories(Config::METADATA_DIR);
    }
    load_metadata();
}

TopicManager::~TopicManager() {
    save_metadata();
}

void TopicManager::create_topic(const std::string& name, int num_partitions) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (topics_.find(name) == topics_.end()) {
        topics_[name] = std::make_shared<Topic>(name, num_partitions);
    }
}

std::shared_ptr<Topic> TopicManager::get_topic(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = topics_.find(name);
    if (it != topics_.end()) {
        return it->second;
    }
    return nullptr;
}

std::vector<std::shared_ptr<Topic>> TopicManager::get_all_topics() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<std::shared_ptr<Topic>> all_topics;
    for (const auto& [name, topic] : topics_) {
        all_topics.push_back(topic);
    }
    return all_topics;
}

void TopicManager::load_metadata() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (fs::exists(Config::TOPICS_FILE)) {
        std::ifstream file(Config::TOPICS_FILE);
        if (file.is_open()) {
            json j;
            try {
                file >> j;
                for (const auto& item : j) {
                    std::string name = item["name"];
                    int partitions = item["partitions"];
                    topics_[name] = std::make_shared<Topic>(name, partitions);
                }
            } catch (const json::exception& e) {
                std::cerr << "Failed to parse topics.json: " << e.what() << std::endl;
            }
        }
    }
}

void TopicManager::save_metadata() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    json j = json::array();
    for (const auto& [name, topic] : topics_) {
        j.push_back({
            {"name", name},
            {"partitions", topic->get_num_partitions()}
        });
    }
    
    std::ofstream file(Config::TOPICS_FILE);
    if (file.is_open()) {
        file << j.dump(4);
    }
}

} // namespace kafka_lite
