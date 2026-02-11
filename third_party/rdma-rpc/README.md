# rdma-rpc

Header-only RDMA building blocks that can be linked as a small library target.

## Build (CMake)

```bash
mkdir -p build
cmake -S . -B build -DRDMA_RPC_BUILD_EXAMPLES=ON
cmake --build build
```

## Use as a library

In your CMake project:

```cmake
add_subdirectory(path/to/rdma-rpc)

target_link_libraries(your_target PRIVATE rdma_rpc)
```

Headers are exposed from this directory, for example:

```cpp
#include "com/protocols.hpp"
#include "ClientRDMA.hpp"
#include "ServerRDMA.hpp"
#include "VerbsEP.hpp"
```

## Examples

- examples/client.cpp
- examples/server.cpp
- examples/test.cpp
- examples/experiments.sh

## Layout

```
lib/include/rdma_rpc   Header-only library
examples/              Example client/server/test
rpc/                   Placeholder for higher-level RPC code
```
