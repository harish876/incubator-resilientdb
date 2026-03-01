#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

package(default_visibility = ["//platform:__subpackages__"])

cc_library(
    name = "zrpc",
    srcs = ["zrpc.cpp"],
    hdrs = ["zrpc.h"],
    copts = ["-std=c++11"],
    deps = [
        "//third_party/rdma-rpc:rdma_rpc",
    ],
)

cc_library(
    name = "rdma_acceptor",
    srcs = ["rdma_acceptor.cpp"],
    hdrs = ["rdma_acceptor.h"],
    deps = [
        ":zrpc",
    ],
)

cc_library(
    name = "rdma_async_replica_client",
    srcs = ["rdma_async_replica_client.cpp"],
    hdrs = ["rdma_async_replica_client.h"],
    deps = [
        ":zrpc",
    ],
)

cc_binary(
    name = "rdma_send_message_main",
    srcs = ["rdma_send_message_main.cpp"],
    tags = ["manual"],
    deps = [
        ":rdma_acceptor",
        ":rdma_async_replica_client",
        ":zrpc",
    ],
)

cc_binary(
    name = "rdma_test_main",
    srcs = ["rdma_test_main.cpp"],
    tags = ["manual"],
    deps = [
        ":rdma_acceptor",
        ":rdma_async_replica_client",
        ":zrpc",
    ],
)

sh_test(
    name = "rdma_test",
    srcs = ["rdma_test.sh"],
    data = [":rdma_test_main"],
    timeout = "long",
    tags = ["manual", "no-sandbox"],
)

cc_test(
    name = "rdma_acceptor_test",
    srcs = ["rdma_acceptor_test.cpp"],
    # args = ["--gtest_no_fork", "--gtest_break_on_failure"],
    tags = ["manual", "no-sandbox"],
    deps = [
        ":rdma_acceptor",
        ":rdma_async_replica_client",
        ":zrpc",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "rdma_async_replica_client_test",
    srcs = ["rdma_async_replica_client_test.cpp"],
    # args = ["--gtest_no_fork", "--gtest_break_on_failure"],
    tags = ["manual", "no-sandbox"],
    deps = [
        ":rdma_acceptor",
        ":rdma_async_replica_client",
        ":zrpc",
        "//common/test:test_main",
    ],
)
