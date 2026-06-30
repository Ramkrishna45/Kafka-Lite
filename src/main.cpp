#include "TcpServer.h"
#include "Broker.h"
#include "ConfigManager.h"
#include <iostream>

using namespace kafka_lite;

int main() {
    try {
        ConfigManager::load();
        
        Broker broker;
        TcpServer server(ConfigManager::port, &broker);
        
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
