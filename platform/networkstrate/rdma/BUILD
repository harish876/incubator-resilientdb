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

cc_library(
    name = "rdma_replica_communicator",
    srcs = ["rdma_replica_communicator.cpp"],
    hdrs = ["rdma_replica_communicator.h"],
    deps = [
        ":rdma_async_replica_client",
        "//common/crypto:signature_verifier",
        "//interface/rdbc:net_channel",
        "//platform/networkstrate:replica_communicator_interface",
        "//platform/proto:broadcast_cc_proto",
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

cc_binary(
    name = "consensus_manager_rdma_test_main",
    srcs = ["consensus_manager_rdma_test_main.cpp"],
    tags = ["manual"],
    deps = [
        "//platform/networkstrate/rdma:rdma_acceptor",
        "//platform/networkstrate/rdma:rdma_replica_communicator",
        "//platform/networkstrate/rdma:zrpc",
        "//platform/proto:broadcast_cc_proto",
        "//platform/proto:resdb_cc_proto",
    ],
)

sh_test(
    name = "consensus_manager_rdma_test",
    srcs = ["consensus_manager_rdma_test.sh"],
    data = [":consensus_manager_rdma_test_main"],
    timeout = "long",
    tags = ["manual", "no-sandbox"],
)