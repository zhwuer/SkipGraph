cmake_minimum_required(VERSION 3.12)

project(actor)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

add_executable(Skip_Graph Skip_Graph.cpp)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR})

target_include_directories(
        Skip_Graph
        PUBLIC
        /usr/local/include
)

target_link_libraries(
        Skip_Graph
        /usr/local/lib/libcaf_core.so
        /usr/local/lib/libcaf_io.so
)