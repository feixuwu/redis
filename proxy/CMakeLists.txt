cmake_minimum_required(VERSION 3.14)
project(redis-mux-proxy LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")

# ASAN option
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
if(ENABLE_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

# Source files
set(PROXY_SOURCES
    src/main.cpp
    src/config.cpp
    src/logger.cpp
    src/event_loop.cpp
    src/worker_thread.cpp
    src/proxy_server.cpp
    src/client_connection.cpp
    src/backend_manager.cpp
    src/mux_backend_connection.cpp
    src/admin_server.cpp
    src/hot_upgrade.cpp
    src/resp_parser.cpp
)

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../deps/hiredis
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
)

# Executable
add_executable(redis-mux-proxy ${PROXY_SOURCES})

# Link libraries
target_link_libraries(redis-mux-proxy
    pthread
)

# Install
install(TARGETS redis-mux-proxy DESTINATION bin)
install(FILES conf/proxy.yaml DESTINATION etc)
