 
cmake_minimum_required(VERSION 3.0)
project(stdinServer LANGUAGES CXX)

include(ExternalProject)

ExternalProject_Add(ruperLB
    GIT_REPOSITORY https://github.com/PenRed/RUPER-LB
    STEP_TARGETS build
    EXCLUDE_FROM_ALL TRUE
    INSTALL_COMMAND cmake -E echo "Skipping install step."
    CMAKE_ARGS
        -DWITH_MPI:BOOL=FALSE
        -DWITH_HTTP:BOOL=FALSE
    SOURCE_DIR ruperLB
)

ExternalProject_Get_Property(ruperLB SOURCE_DIR)
ExternalProject_Get_Property(ruperLB BINARY_DIR)

add_executable(stdinServer ${PROJECT_SOURCE_DIR}/stdinServer.cpp)

add_dependencies(stdinServer ruperLB)

target_include_directories(stdinServer PUBLIC ${SOURCE_DIR}/src)
target_link_directories(stdinServer PUBLIC ${BINARY_DIR}/src)

target_link_libraries(stdinServer rupper)
