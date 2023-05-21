cmake_minimum_required(VERSION 3.10)

project(uthreads)

set(CMAKE_CXX_STANDARD 11)

add_executable(${PROJECT_NAME} SampleClient.cpp Barrier.h Barrier.cpp MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp)

# Set compiler flags for debugging
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -g")

# # Link with pthreads library
# target_link_libraries(${PROJECT_NAME} pthread)
