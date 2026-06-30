#include "RetentionManager.h"
#include "TopicManager.h"
#include "ConfigManager.h"
#include "Config.h" // For DATA_DIR
#include "Logger.h"
#include <filesystem>
#include <fstream>
#include <chrono>

namespace fs = std::filesystem;

namespace kafka_lite {

RetentionManager::RetentionManager(TopicManager* topic_manager) 
    : topic_manager_(topic_manager), running_(true) {
    monitor_thread_ = std::thread(&RetentionManager::monitor_loop, this);
}

RetentionManager::~RetentionManager() {
    running_ = false;
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }
}

void RetentionManager::monitor_loop() {
    while (running_) {
        for(int i = 0; i < 600 && running_; ++i) { // 60 seconds
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!running_) break;
        
        apply_retention_policies();
    }
}

void RetentionManager::apply_retention_policies() {
    auto topics = topic_manager_->get_all_topics();
    auto now = fs::file_time_type::clock::now();
    
    for (const auto& topic : topics) {
        std::string dir_path = std::string(Config::DATA_DIR) + "/" + topic->get_name();
        if (!fs::exists(dir_path)) continue;
        
        uintmax_t dir_size = 0;
        for (const auto& entry : fs::directory_iterator(dir_path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".log") {
                auto ftime = fs::last_write_time(entry);
                auto age_hours = std::chrono::duration_cast<std::chrono::hours>(now - ftime).count();
                
                if (age_hours > ConfigManager::retentionHours) {
                    Logger::info("RetentionManager: Truncating expired log " + entry.path().string());
                    std::ofstream ofs(entry.path(), std::ios::out | std::ios::trunc);
                    continue;
                }
                
                dir_size += fs::file_size(entry);
            }
        }
        
        if ((dir_size / (1024 * 1024)) > ConfigManager::maxLogSizeMB) {
            Logger::warn("RetentionManager: Topic " + topic->get_name() + " exceeded maxLogSizeMB. Truncating old logs.");
            for (const auto& entry : fs::directory_iterator(dir_path)) {
                if (entry.is_regular_file() && entry.path().extension() == ".log") {
                    std::ofstream ofs(entry.path(), std::ios::out | std::ios::trunc);
                }
            }
        }
    }
}

} // namespace kafka_lite
