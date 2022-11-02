cmake_minimum_required(VERSION 3.10)
project(test)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "-O0 -g -fpic -march=native -Wall")


SET(CMAKE_BUILD_TYPE "Debug")

#6.add executable file，添加要编译的可执行文件
ADD_EXECUTABLE(test  testLock.c)

target_link_libraries(test  PUBLIC -pthread)
