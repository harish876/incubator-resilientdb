#!/bin/bash
# Shell wrapper for BasicRingBuffer RDMA consensus integration tests.
# Mirrors consensus_manager_rdma_test.sh.
set -e
./consensus_manager_basic_ring_rdma_test_main all
