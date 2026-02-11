cmake_minimum_required(VERSION 3.10)
project(rdma_rpc)

# Enable compile_commands.json generation for IntelliSense
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Wall")

# Find required packages
find_library(IBVERBS_LIB ibverbs REQUIRED)
find_library(RDMACM_LIB rdmacm REQUIRED)

# Find include directories
find_path(IBVERBS_INCLUDE infiniband/verbs.h REQUIRED)
find_path(RDMACM_INCLUDE rdma/rdma_cma.h REQUIRED)

# Library target (header-only)
add_library(rdma_rpc INTERFACE)
target_include_directories(rdma_rpc
	INTERFACE
		${CMAKE_CURRENT_SOURCE_DIR}/lib/include/rdma_rpc
)
target_include_directories(rdma_rpc
	SYSTEM INTERFACE
		${IBVERBS_INCLUDE}
		${RDMACM_INCLUDE}
)
target_link_libraries(rdma_rpc
	INTERFACE
		${IBVERBS_LIB}
		${RDMACM_LIB}
)

option(RDMA_RPC_BUILD_EXAMPLES "Build rdma_rpc examples" ON)

if(RDMA_RPC_BUILD_EXAMPLES)
	add_executable(test examples/test.cpp)
	add_executable(server examples/server.cpp)
	add_executable(client examples/client.cpp)

	target_link_libraries(test rdma_rpc)
	target_link_libraries(server rdma_rpc)
	target_link_libraries(client rdma_rpc)

	target_include_directories(test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/examples/third_party)
	target_include_directories(server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/examples/third_party)
	target_include_directories(client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/examples/third_party)

	install(TARGETS test server client DESTINATION bin)
endif()

install(TARGETS rdma_rpc EXPORT rdma_rpcTargets)
install(DIRECTORY lib/include/rdma_rpc/ DESTINATION include)
