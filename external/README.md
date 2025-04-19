# Third-Party Libraries

This directory contains third-party libraries used by the InfParquet framework. Instead of relying on system-installed libraries, we include necessary headers and binary components directly in the project to ensure consistent builds across different environments.

## Directory Structure

- `arrow/` - Apache Arrow library for Parquet file operations
- `json/` - JSON library for metadata parsing and serialization
- `lzma/` - LZMA2 compression library
- `xxhash/` - xxHash hashing library for data validation

## Including Libraries

When including third-party headers in the project code, always use the path relative to the `external` directory. For example:

```cpp
// Correct way to include
#include "json/json.hpp"
#include "arrow/api.h"
#include "external/lzma/LzmaDec.h"
#include "external/lzma/LzmaEnc.h"

// Incorrect way
#include "third_party/json/json.hpp"   // Don't use this
#include "include/third_party/json/json.hpp"  // Don't use this
#include "json.hpp"                    // Don't use this
#include "D:/Files/CppLibs/json/json.hpp"  // Don't use absolute paths
```

Note: The CMake configuration already includes the `external` directory in the include path, so you can directly reference libraries relative to this directory.


## Adding New Libraries

When adding a new third-party library:

1. Create a subdirectory with the library name in the `external` directory
2. Copy only the necessary header files and binary components
3. Update this README.md file
4. Update the CMakeLists.txt to include the new library

## Notes

- Avoid modifying third-party library code
- Minimize the number of third-party dependencies
- Keep binary files to a minimum for better version control
- Document any special integration requirements for each library 