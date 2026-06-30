#include "TcpServer.h"
#include "Broker.h"
#include "ConfigManager.h"
#include "Logger.h"
#include <iostream>
#include <vector>
#include <nlohmann/json.hpp>
#include <chrono>

using json = nlohmann::json;

namespace kafka_lite {

TcpServer::TcpServer(int port, Broker* broker) 
    : port_(port), broker_(broker), running_(false), thread_pool_(ConfigManager::threadPoolSize) {
    
#ifdef _WIN32
    WSADATA wsaData;
    int iResult = WSAStartup(MAKEWORD(2,2), &wsaData);
    if (iResult != 0) {
        std::cerr << "WSAStartup failed: " << iResult << std::endl;
        exit(1);
    }
#endif

    listen_socket_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listen_socket_ == -1) {
        Logger::error("Error creating socket");
        exit(1);
    }

    sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(listen_socket_, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        Logger::error("Error binding socket");
        exit(1);
    }

    if (listen(listen_socket_, SOMAXCONN) == -1) {
        Logger::error("Error listening on socket");
        exit(1);
    }
}

TcpServer::~TcpServer() {
    stop();
#ifdef _WIN32
    WSACleanup();
#endif
}

void TcpServer::start() {
    running_ = true;
    Logger::info("Broker started on port " + std::to_string(port_));

    while (running_) {
        sockaddr_in client_addr;
        int client_size = sizeof(client_addr);
#ifdef _WIN32
        Socket_t client_socket = accept(listen_socket_, (struct sockaddr*)&client_addr, &client_size);
        if (client_socket == INVALID_SOCKET) {
#else
        Socket_t client_socket = accept(listen_socket_, (struct sockaddr*)&client_addr, (socklen_t*)&client_size);
        if (client_socket == -1) {
#endif
            if (!running_) break;
            std::cerr << "Error accepting connection" << std::endl;
            continue;
        }

        thread_pool_.enqueue([this, client_socket]() {
            this->handle_client(client_socket);
        });
    }
}

void TcpServer::stop() {
    running_ = false;
#ifdef _WIN32
    closesocket(listen_socket_);
#else
    close(listen_socket_);
#endif
}

void TcpServer::handle_client(Socket_t client_socket) {
    char buffer[131072]; // 128KB buffer to handle huge batch sizes (e.g. 1000 messages)
    while (running_) {
        int bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            break;
        }
        buffer[bytes_received] = '\0';
        
        try {
            json req = json::parse(buffer);
            json resp;
            
            std::string action = req.value("action", "");
            if (action == "publish") {
                std::string topic = req.value("topic", "");
                int acks = req.value("acks", 1);
                Message msg;
                msg.key = req.value("key", "");
                msg.value = req.value("value", "");
                msg.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                
                auto result = broker_->publish(topic, msg);
                
                if (acks == 0) {
                    continue; // Fire and forget
                }
                
                if (result.success) {
                    resp["status"] = "success";
                    resp["partition"] = result.partition;
                    resp["offset"] = result.offset;
                } else {
                    resp["status"] = "error";
                    resp["message"] = "Topic not found";
                }
                
            } else if (action == "publish_batch") {
                int acks = req.value("acks", 1);
                int published = 0;
                
                if (req.contains("messages") && req["messages"].is_array()) {
                    for (const auto& m : req["messages"]) {
                        std::string topic = m.value("topic", "");
                        Message msg;
                        msg.key = m.value("key", "");
                        msg.value = m.value("value", "");
                        msg.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
                        
                        auto result = broker_->publish(topic, msg);
                        if (result.success) published++;
                    }
                }
                
                if (acks == 0) continue;
                
                resp["status"] = "success";
                resp["published"] = published;
                
            } else if (action == "consume") {
                std::string topic = req.value("topic", "");
                std::string group = req.value("group", "");
                std::string consumer_id = req.value("consumerId", "");
                size_t max_msgs = req.value("max", 10);
                
                auto messages = broker_->consume(group, topic, consumer_id, max_msgs);
                
                json msgs_json = json::array();
                for (const auto& msg : messages) {
                    msgs_json.push_back({
                        {"offset", msg.offset},
                        {"timestamp", msg.timestamp},
                        {"key", msg.key},
                        {"value", msg.value},
                        {"partition", msg.partition}
                    });
                }
                resp["status"] = "success";
                resp["messages"] = msgs_json;
            } else if (action == "create_topic") {
                std::string topic = req.value("topic", "");
                int partitions = req.value("partitions", 1);
                if (broker_->create_topic(topic, partitions)) {
                    resp["status"] = "success";
                    resp["topic"] = topic;
                    resp["partitions"] = partitions;
                } else {
                    resp["status"] = "error";
                    resp["message"] = "Topic already exists";
                }
            } else if (action == "metrics") {
                resp = broker_->get_metrics();
            } else if (action == "health") {
                resp = broker_->get_health();
            } else if (action == "topics" || action == "partitions") {
                json m = broker_->get_metrics();
                if (m.contains("topics_data")) {
                    resp["topics"] = m["topics_data"];
                } else {
                    resp["topics"] = json::object();
                }
            } else if (action == "subscribe") {
                std::string group = req.value("group", "");
                std::string topic = req.value("topic", "");
                std::string consumer_id = req.value("consumerId", "");
                
                broker_->subscribe(group, topic, consumer_id);
                resp["status"] = "success";
            } else if (action == "heartbeat") {
                std::string consumer_id = req.value("consumerId", "");
                broker_->heartbeat(consumer_id);
                resp["status"] = "success";
            } else if (action == "groups") {
                resp = broker_->get_groups_info();
            } else {
                resp["status"] = "error";
                resp["message"] = "Unknown action";
            }
            
            std::string resp_str = resp.dump() + "\n";
            send(client_socket, resp_str.c_str(), resp_str.length(), 0);
            
        } catch (const json::exception& e) {
            std::string err = "{\"status\":\"error\",\"message\":\"invalid json\"}\n";
            send(client_socket, err.c_str(), err.length(), 0);
        }
    }
    
#ifdef _WIN32
    closesocket(client_socket);
#else
    close(client_socket);
#endif
}

} // namespace kafka_lite
