#!/bin/bash
# Runs rdma_test_main - pass test name: SendMessage | MultiSendMessage | MultiClient
# Usage: rdma_test.sh [TestName]   (default: SendMessage)
set -e
RUNFILES_DIR="${RUNFILES_DIR:-$(dirname "$0").runfiles}"
BINARY="${RUNFILES_DIR}/com_resdb_nexres/platform/networkstrate/rdma/rdma_test_main"
exec "$BINARY" "${1:-SendMessage}"
