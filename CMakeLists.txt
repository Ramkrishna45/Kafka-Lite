cmake_minimum_required(VERSION 3.14)
project(KafkaLite VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Add Include Directories
include_directories(include)
include_directories(src)

# Threading is essential
find_package(Threads REQUIRED)

if (WIN32)
    # Windows Socket Library
    link_libraries(ws2_32)
endif()

# Source files
set(SOURCES
    src/storage/LogSegment.cpp
    src/storage/Partition.cpp
    src/topic/Topic.cpp
    src/topic/TopicManager.cpp
    src/consumer/OffsetManager.cpp
    src/consumer/ConsumerGroupManager.cpp
    src/consumer/HeartbeatManager.cpp
    src/config/ConfigManager.cpp
    src/logging/Logger.cpp
    src/metrics/MetricsManager.cpp
    src/retention/RetentionManager.cpp
    src/network/TcpServer.cpp
    src/broker/Broker.cpp
    src/main.cpp
)

add_executable(kafka-lite ${SOURCES})
target_link_libraries(kafka-lite PRIVATE Threads::Threads)
