#!/bin/bash

# InfParquet Build Script for Linux/Mac
echo "InfParquet Build Script for Linux/Mac"
echo "===================================="

# Check if CMake is installed
if ! command -v cmake &> /dev/null; then
    echo "Error: CMake is not installed or not in PATH."
    echo "Please install CMake using your package manager:"
    echo "  Ubuntu/Debian: sudo apt-get install cmake"
    echo "  Fedora/RHEL:   sudo dnf install cmake"
    echo "  macOS:         brew install cmake"
    exit 1
fi

# Default values
BUILD_TYPE="Release"
BUILD_DIR="build"
ARROW_PATH=""
LZMA_PATH=""
XXHASH_PATH=""

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        -release)
            BUILD_TYPE="Release"
            shift
            ;;
        -build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        -arrow)
            ARROW_PATH="$2"
            shift 2
            ;;
        -lzma)
            LZMA_PATH="$2"
            shift 2
            ;;
        -xxhash)
            XXHASH_PATH="$2"
            shift 2
            ;;
        -help)
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -debug              Build debug version"
            echo "  -release            Build release version (default)"
            echo "  -build-dir DIR      Specify build directory (default: build)"
            echo "  -arrow PATH         Path to Arrow library"
            echo "  -lzma PATH          Path to LZMA library"
            echo "  -xxhash PATH        Path to xxHash library"
            echo "  -help               Display this help message"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -help for usage information."
            exit 1
            ;;
    esac
done

echo "Build type: $BUILD_TYPE"
echo "Build directory: $BUILD_DIR"

# Check if build directory exists, create if not
if [ ! -d "$BUILD_DIR" ]; then
    echo "Creating build directory..."
    mkdir -p "$BUILD_DIR"
fi

# Change to build directory
cd "$BUILD_DIR" || { echo "Error: Failed to change to build directory"; exit 1; }

# Prepare CMake arguments
CMAKE_ARGS="-DCMAKE_BUILD_TYPE=$BUILD_TYPE"

if [ -n "$ARROW_PATH" ]; then
    CMAKE_ARGS="$CMAKE_ARGS -DARROW_ROOT=$ARROW_PATH"
fi

if [ -n "$LZMA_PATH" ]; then
    CMAKE_ARGS="$CMAKE_ARGS -DLZMA_ROOT=$LZMA_PATH"
fi

if [ -n "$XXHASH_PATH" ]; then
    CMAKE_ARGS="$CMAKE_ARGS -DXXHASH_ROOT=$XXHASH_PATH"
fi

# Configure with CMake
echo ""
echo "Configuring with CMake..."
cmake $CMAKE_ARGS ..

if [ $? -ne 0 ]; then
    echo ""
    echo "Error: CMake configuration failed."
    exit 1
fi

# Determine the number of CPU cores for parallel build
if command -v nproc &> /dev/null; then
    # Linux
    CPU_CORES=$(nproc)
elif command -v sysctl &> /dev/null; then
    # macOS
    CPU_CORES=$(sysctl -n hw.ncpu)
else
    # Default to 2 if we can't determine
    CPU_CORES=2
fi

# Build the project
echo ""
echo "Building InfParquet using $CPU_CORES cores..."
cmake --build . -- -j$CPU_CORES

if [ $? -ne 0 ]; then
    echo ""
    echo "Error: Build failed."
    exit 1
fi

echo ""
echo "Build completed successfully!"
echo "The executable is located at: ./bin/infparquet"

# Make the binary executable (just to be safe)
chmod +x ./bin/infparquet 2>/dev/null

exit 0 