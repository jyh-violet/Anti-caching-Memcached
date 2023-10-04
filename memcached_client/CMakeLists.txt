cmake_minimum_required(VERSION 3.16)
project(memcached_client)


set(CMAKE_CXX_STANDARD 14)
add_definitions("-Wall -g")

MESSAGE( STATUS "clock_interval = ${clock_interval}.")
MESSAGE( STATUS "sample_interval = ${sample_interval}.")

add_definitions(-DCLOCK_INTERVAL=clock_interval)
add_definitions(-DSAMPLE_INTERVAL=sample_interval)


configure_file("config.h.in" "config.h")

add_executable(cache_replace cache_replace.c)
target_link_libraries(cache_replace -lpthread -lconfig -lm)
