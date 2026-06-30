#include "ConfigManager.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
#include "Logger.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace kafka_lite {

int ConfigManager::port = 9092;
int ConfigManager::threadPoolSize = 8;
int ConfigManager::heartbeatIntervalSeconds = 5;
int ConfigManager::consumerTimeoutSeconds = 15;
int ConfigManager::retentionHours = 24;
int ConfigManager::maxLogSizeMB = 500;
int ConfigManager::maxBatchSize = 1000;

void ConfigManager::load() {
    if (fs::exists("config.json")) {
        std::ifstream file("config.json");
        if (file.is_open()) {
            try {
                json j;
                file >> j;
                
                if (j.contains("port")) port = j["port"];
                if (j.contains("threadPoolSize")) threadPoolSize = j["threadPoolSize"];
                if (j.contains("heartbeatIntervalSeconds")) heartbeatIntervalSeconds = j["heartbeatIntervalSeconds"];
                if (j.contains("consumerTimeoutSeconds")) consumerTimeoutSeconds = j["consumerTimeoutSeconds"];
                if (j.contains("retentionHours")) retentionHours = j["retentionHours"];
                if (j.contains("maxLogSizeMB")) maxLogSizeMB = j["maxLogSizeMB"];
                if (j.contains("maxBatchSize")) maxBatchSize = j["maxBatchSize"];
                
                Logger::info("Configuration loaded successfully");
            } catch (const json::exception& e) {
                Logger::error("Failed to parse config.json: " + std::string(e.what()));
            }
        }
    } else {
        Logger::info("config.json not found, using default configuration");
    }
}

} // namespace kafka_lite
