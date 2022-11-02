cmake_minimum_required(VERSION 3.16)
project(memcached_client)


set(CMAKE_CXX_STANDARD 14)
add_definitions("-Wall -g")

MESSAGE( STATUS "clock_interval = ${clock_interval}.")
MESSAGE( STATUS "sample_interval = ${sample_interval}.")

add_definitions(-DCLOCK_INTERVAL=clock_interval)
add_definitions(-DSAMPLE_INTERVAL=sample_interval)


configure_file("config.h.in" "config.h")


add_executable(memcached_client main.cpp)
target_link_libraries(memcached_client  -lmemcached -lpthread)

add_executable(nvm_test nvm_test.cpp)
target_link_libraries(nvm_test  -lpmem)

add_executable(ycsb ycsb.cpp)
target_link_libraries(ycsb  -lmemcached -lpthread -lconfig)

add_executable(test test.c)
target_link_libraries(test -lpthread -lconfig)

add_executable(cache_replace cache_replace.c)
target_link_libraries(cache_replace -lpthread -lconfig -lm)
