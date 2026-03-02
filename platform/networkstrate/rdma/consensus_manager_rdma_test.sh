#!/bin/bash
# Runs consensus_manager_rdma_test_main - requires RDMA devices, no-sandbox.
# Usage: consensus_manager_rdma_test.sh [TestName|all]   (default: RdmaReplicaCommunicatorBroadcast)
# Or: RDMA_TEST_NAME=all bazel test ... --test_env=RDMA_TEST_NAME=all
set -e
RUNFILES_DIR="${RUNFILES_DIR:-$(dirname "$0").runfiles}"
BINARY="${RUNFILES_DIR}/com_resdb_nexres/platform/networkstrate/rdma/consensus_manager_rdma_test_main"
TEST_NAME="${1:-${RDMA_TEST_NAME:-RdmaReplicaCommunicatorBroadcast}}"
exec "$BINARY" "$TEST_NAME"
