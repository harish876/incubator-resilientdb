## PBFT Consensus Server

This target provides a lightweight PBFT self-test binary:

- Bazel target: `//benchmark/protocols/pbft:consensus_server`
- Self-test runs 4 in-process replicas and verifies `SET` then `GET`.

## Build

```bash
bazel build //benchmark/protocols/pbft:consensus_server
```

## Self-test CLI

```bash
bazel run //benchmark/protocols/pbft:consensus_server -- \
  --selftest \
  --mode <tcp|rdma> \
  --choice <shared|per_client> \
  --ip <bind_ip> \
  --base_port <base_port>
```

### Flags

- `--selftest`: run embedded self-test mode.
- `--mode`: transport mode.
  - `tcp` (default)
  - `rdma`
- `--choice`: RDMA ring behavior (default: `shared`).
  - `shared`: shared/multi-client ring behavior.
  - `per_client`: per-client ring behavior (legacy basic ring style).
- `--ip`: replica IP to bind/connect (default: `127.0.0.1`).
- `--base_port`: base port; replicas use `base_port + {1..4}` (default: `23000`).

## Examples

### TCP self-test

```bash
bazel run //benchmark/protocols/pbft:consensus_server -- \
  --selftest --mode tcp --ip 127.0.0.1 --base_port 23000
```

### RDMA self-test (shared ring)

```bash
bazel run //benchmark/protocols/pbft:consensus_server -- \
  --selftest --mode rdma --choice shared --ip 128.110.219.180 --base_port 26000
```

### RDMA self-test (per-client ring)

```bash
bazel run //benchmark/protocols/pbft:consensus_server -- \
  --selftest --mode rdma --choice per_client --ip 128.110.219.180 --base_port 26000
```

## Backward-compatible syntax

The binary also accepts the older positional self-test syntax:

```bash
bazel run //benchmark/protocols/pbft:consensus_server -- \
  --selftest rdma 128.110.219.180 26000 per_client
```

`basic_ring` is still accepted as a legacy mode alias.

## Expected success log

On success, you should see a line like:

```text
Self-test passed mode=rdma (SET+GET verified, get_value=light_value)
```