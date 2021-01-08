 
cmake_minimum_required(VERSION 3.0)
project(stdinServer LANGUAGES CXX)

include(ExternalProject)

ExternalProject_Add(ruperLB
    GIT_REPOSITORY https://github.com/PenRed/RUPER-LB
    STEP_TARGETS build
    EXCLUDE_FROM_ALL TRUE
    INSTALL_COMMAND cmake -E echo "Skipping install step."
    CMAKE_ARGS 
        -DWITH_HTTP:BOOL=TRUE 
        -DWITH_MPI:BOOL=FALSE 
    SOURCE_DIR ruperLB
)

add_definitions(-D_PEN_USE_LB_HTTP_=1) #enable HTTP
#Find curl package
find_package(CURL REQUIRED)

ExternalProject_Get_Property(ruperLB SOURCE_DIR)
ExternalProject_Get_Property(ruperLB BINARY_DIR)

add_executable(pi ${PROJECT_SOURCE_DIR}/pi.cpp)

add_dependencies(pi ruperLB)

target_include_directories(pi PUBLIC ${SOURCE_DIR}/src)
target_link_directories(pi PUBLIC ${BINARY_DIR}/src)

target_link_libraries(pi rupper)
target_include_directories(pi PUBLIC ${CURL_INCLUDE_DIR})
target_link_libraries(pi ${CURL_LIBRARIES})
