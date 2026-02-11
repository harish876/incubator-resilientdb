#!/bin/bash
# Build script for RDMA protocols with compile_commands.json generation

set -e

# Create build directory
mkdir -p build
cd build

# Run CMake with compile_commands.json generation
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..

# Build the project
cmake --build . --config Release

# Copy compile_commands.json to root for IDE access
cp compile_commands.json ..

echo "Build complete!"
echo "compile_commands.json generated at: $(pwd)/../compile_commands.json"
