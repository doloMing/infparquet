cmake_minimum_required(VERSION 3.14)
project(infparquet VERSION 1.0.0 LANGUAGES C CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

# Set output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Compiler options
if (MSVC)
    add_compile_options(/W4 /MP)
    # Disable specific warnings
    add_compile_options(/wd4244 /wd4267) # Conversion warnings
else()
    add_compile_options(-Wall -Wextra -pedantic)
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external)

# Enable threading support
find_package(Threads REQUIRED)

# External dependencies - use relative paths or CMake variables when possible
# Try to find system libraries first, then fallback to local paths

# Arrow library
find_package(Arrow QUIET)
if(NOT Arrow_FOUND)
    set(ARROW_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/external/arrow" CACHE PATH "Root directory of Arrow library")
    include_directories(${ARROW_ROOT})
    include_directories("${CMAKE_CURRENT_SOURCE_DIR}/external/arrow")
    link_directories(${ARROW_ROOT}/lib)
    message(STATUS "Using local Arrow library: ${ARROW_ROOT}")
endif()

# LZMA2 library
find_package(LibLZMA QUIET)
if(NOT LibLZMA_FOUND)
    set(LZMA_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/external/lzma" CACHE PATH "Root directory of LZMA2 library")
    include_directories(${LZMA_ROOT})
    link_directories(${LZMA_ROOT}/lib)
    message(STATUS "Using local LZMA library: ${LZMA_ROOT}")
endif()

# JSON library (header-only)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/json)

# xxHash library has been removed as it's not directly used in the project

# Source files - include all C and C++ files
file(GLOB_RECURSE CORE_SOURCES 
    "src/core/*.c" 
    "src/core/*.cpp"
)
file(GLOB_RECURSE COMPRESSION_SOURCES 
    "src/compression/*.c" 
    "src/compression/*.cpp"
)
file(GLOB_RECURSE METADATA_SOURCES 
    "src/metadata/*.c" 
    "src/metadata/*.cpp"
)
file(GLOB_RECURSE FRAMEWORK_SOURCES 
    "src/framework/*.c"
    "src/framework/*.cpp"
)

# LZMA implementation files
set(LZMA_SOURCES
    external/lzma/Alloc.c
    external/lzma/LzmaEnc.c
    external/lzma/LzmaDec.c
    external/lzma/LzFind.c
    external/lzma/LzFindMt.c
    external/lzma/LzFindOpt.c
    external/lzma/Threads.c
    external/lzma/CpuArch.c
    external/lzma/Lzma2Dec.c
    external/lzma/Lzma2Enc.c
    external/lzma/MtCoder.c
    external/lzma/MtDec.c
    external/lzma/7zStream.c
) 

set(SOURCES 
    ${CORE_SOURCES}
    ${COMPRESSION_SOURCES}
    ${METADATA_SOURCES}
    ${FRAMEWORK_SOURCES}
    ${LZMA_SOURCES}
    src/main.cpp
)

# Add main executable
add_executable(infparquet ${SOURCES})

# Link libraries
if(Arrow_FOUND)
    target_link_libraries(infparquet Arrow::arrow Arrow::parquet)
else()
    target_link_libraries(infparquet arrow parquet)
endif()

# xxHash library dependency has been removed

# Add threading library
target_link_libraries(infparquet Threads::Threads)

# Add any platform-specific libraries
if(WIN32)
    target_link_libraries(infparquet ws2_32)
endif()

# Install targets
install(TARGETS infparquet
    RUNTIME DESTINATION bin
)

# Create a package
include(CPack)

# Print configuration information
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "C Standard: ${CMAKE_C_STANDARD}")
