# InfParquet

InfParquet is a high-performance C/C++ framework for compressing and decompressing Parquet files using LZMA2 compression with enhanced metadata generation and tracking capabilities.

## Features

- **Advanced Compression**: Uses LZMA2 compression algorithms to achieve higher compression ratios than standard Parquet compression methods
- **Parallel Processing**: Multi-threaded compression and decompression for optimal performance
- **Metadata Management**: Comprehensive metadata generation and extraction
- **SQL Query Support**: Query metadata using SQL-like syntax
- **Custom Metadata**: Define and generate custom metadata from Parquet files

## Requirements

- C++17 compatible compiler (GCC 7+, Clang 5+, MSVC 2019+)
- CMake 3.14 or higher
- Arrow and Parquet libraries
- LZMA library
- xxHash library (for checksums)

## Build Instructions

### Windows

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/infparquet.git
   cd infparquet
   ```

2. Run the build script:
   ```
   build.bat
   ```

   Options:
   - `-debug`: Build debug version
   - `-release`: Build release version (default)
   - `-generator "Generator"`: Specify CMake generator
   - `-arrow PATH`: Path to Arrow library
   - `-lzma PATH`: Path to LZMA library
   - `-xxhash PATH`: Path to xxHash library

### Linux/macOS

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/infparquet.git
   cd infparquet
   ```

2. Run the build script:
   ```
   chmod +x build.sh
   ./build.sh
   ```

   Options:
   - `-debug`: Build debug version
   - `-release`: Build release version (default)
   - `-build-dir DIR`: Specify build directory
   - `-arrow PATH`: Path to Arrow library
   - `-lzma PATH`: Path to LZMA library
   - `-xxhash PATH`: Path to xxHash library

### Manual Build

1. Create a build directory:
   ```
   mkdir build
   cd build
   ```

2. Configure with CMake:
   ```
   cmake ..
   ```

3. Build the project:
   ```
   cmake --build .
   ```

## Usage Examples

### Compressing a Parquet File

```
infparquet compress input.parquet -o output_dir -c 9 -t 4 -m
```

Parameters:
- `input.parquet`: Input Parquet file path
- `-o output_dir`: Output directory for compressed files
- `-c 9`: Compression level (1-9, where 9 is highest)
- `-t 4`: Number of threads
- `-m`: Generate metadata

### Decompressing Files

```
infparquet decompress compressed_dir -o output.parquet -t 4
```

Parameters:
- `compressed_dir`: Directory containing compressed files and metadata
- `-o output.parquet`: Output Parquet file path
- `-t 4`: Number of threads

### Querying Metadata

```
infparquet query compressed_dir -q "SELECT * FROM metadata WHERE column = 'customer_id' AND min_value > 1000"
```

Parameters:
- `compressed_dir`: Directory containing metadata
- `-q "QUERY"`: SQL-like query string

### Listing Available Metadata

```
infparquet list compressed_dir
```

## Architecture

InfParquet follows a modular architecture with the following components:

1. **Core**: Parquet file handling and structure
2. **Compression**: LZMA2 compression and parallel processing
3. **Metadata**: Metadata generation, parsing, and querying
4. **Framework**: High-level API integrating all components

## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please see CONTRIBUTING.md for details.

## Acknowledgments

- Apache Arrow and Parquet projects
- LZMA compression library
- xxHash hashing library 