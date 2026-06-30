#pragma once
#include "Topic.h"
#include <unordered_map>
#include <string>
#include <shared_mutex>
#include <memory>
#include <vector>

namespace kafka_lite {

class TopicManager {
public:
    TopicManager();
    ~TopicManager();

    void create_topic(const std::string& name, int num_partitions);
    std::shared_ptr<Topic> get_topic(const std::string& name);
    std::vector<std::shared_ptr<Topic>> get_all_topics();
    
    void load_metadata();
    void save_metadata();

private:
    std::unordered_map<std::string, std::shared_ptr<Topic>> topics_;
    mutable std::shared_mutex mutex_;
};

} // namespace kafka_lite
