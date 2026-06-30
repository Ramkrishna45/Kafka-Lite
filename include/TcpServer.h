#pragma once
#include <string>
#include <atomic>
#include "ThreadPool.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET Socket_t;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
typedef int Socket_t;
#endif

namespace kafka_lite {

class Broker;

class TcpServer {
public:
    TcpServer(int port, Broker* broker);
    ~TcpServer();

    void start();
    void stop();

private:
    int port_;
    Broker* broker_;
    Socket_t listen_socket_;
    std::atomic<bool> running_;
    ThreadPool thread_pool_;

    void handle_client(Socket_t client_socket);
};

} // namespace kafka_lite
