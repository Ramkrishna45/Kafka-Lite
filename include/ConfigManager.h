#pragma once
#include <string>

namespace kafka_lite {

class ConfigManager {
public:
    static void load();
    
    static int port;
    static int threadPoolSize;
    static int heartbeatIntervalSeconds;
    static int consumerTimeoutSeconds;
    static int retentionHours;
    static int maxLogSizeMB;
    static int maxBatchSize;
};

} // namespace kafka_lite
