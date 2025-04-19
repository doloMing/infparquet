/**
 * infparquet_framework.h
 * 
 * This header file defines the main C++ interface for the InfParquet framework.
 * It provides a high-level API for compressing and decompressing Parquet files
 * using LZMA2 compression with metadata generation and tracking.
 */

#ifndef INFPARQUET_FRAMEWORK_H
#define INFPARQUET_FRAMEWORK_H

#include <string>
#include <vector>
#include <functional>
#include <memory>

namespace infparquet {

/**
 * Callback function type for reporting progress
 * 
 * operation: Description of the current operation
 * row_group_index: Index of the current row group being processed (-1 if not applicable)
 * total_row_groups: Total number of row groups
 * percent_complete: Percentage of the current operation that's complete (0-100)
 * 
 * Return: true to continue, false to abort
 */
using ProgressCallback = std::function<bool(const std::string& operation, 
                                          int row_group_index,
                                          int total_row_groups,
                                          int percent_complete)>;

/**
 * Error callback function type for reporting errors
 * 
 * error_message: Description of the error
 * error_code: Error code
 */
using ErrorCallback = std::function<void(const std::string& error_message, int error_code)>;

/**
 * Error codes for framework operations
 */
enum class FrameworkError {
    OK = 0,
    INVALID_PARAMETER,
    FILE_NOT_FOUND,
    PERMISSION_DENIED,
    MEMORY_ERROR,
    COMPRESSION_ERROR,
    DECOMPRESSION_ERROR,
    METADATA_ERROR,
    PARQUET_ERROR,
    PARALLEL_PROCESSING_ERROR,
    INVALID_QUERY,       // Invalid SQL query
    WRITER_ERROR,        // Error in parquet writer
    UNKNOWN_ERROR
};

/**
 * Options for compressing a parquet file
 */
struct CompressionOptions {
    int compression_level = 5;  // 1-9, where 9 is highest compression
    bool generate_base_metadata = true;  // Whether to generate base metadata
    bool generate_custom_metadata = false;  // Whether to generate custom metadata
    std::string custom_metadata_config;  // Path to JSON config for custom metadata
    int parallel_tasks = 0;  // Number of parallel tasks (0 = auto)
};

/**
 * Options for decompressing a compressed parquet file
 */
struct DecompressionOptions {
    std::string output_directory;  // Directory to store the decompressed file
    int parallel_tasks = 0;  // Number of parallel tasks (0 = auto)
};

/**
 * Result of a query on metadata
 */
struct MetadataQueryResult {
    bool success;
    std::string message;
    std::vector<std::string> matching_files;
    std::vector<std::string> matching_row_groups;
    std::vector<std::string> matching_columns;
};

/**
 * Class encapsulating the InfParquet framework
 */
class InfParquet {
public:
    /**
     * Constructor
     * 
     * Creates a new instance of the InfParquet framework
     */
    InfParquet();
    
    /**
     * Destructor
     */
    ~InfParquet();
    
    /**
     * Copy constructor and assignment operator are deleted
     * to prevent copying because of internal state
     */
    InfParquet(const InfParquet&) = delete;
    InfParquet& operator=(const InfParquet&) = delete;
    
    /**
     * Move constructor and assignment operator are defaulted
     * to allow moving the object
     */
    InfParquet(InfParquet&&) = default;
    InfParquet& operator=(InfParquet&&) = default;
    
    /**
     * Sets the progress callback
     * 
     * callback: Function to call to report progress
     */
    void setProgressCallback(ProgressCallback callback);
    
    /**
     * Sets the error callback
     * 
     * callback: Function to call to report errors
     */
    void setErrorCallback(ErrorCallback callback);
    
    /**
     * Compresses a Parquet file using LZMA2 and generates metadata
     * 
     * input_file: Path to the input Parquet file
     * output_dir: Directory where compressed files and metadata will be written
     * compression_level: LZMA2 compression level (1-9, where 9 is highest compression)
     * threads: Number of threads to use for compression (0 for automatic)
     * use_basic_metadata: Whether to generate and save basic metadata
     * 
     * Return: true on success, false on failure
     */
    bool compressParquetFile(const std::string& input_file, 
                           const std::string& output_dir,
                           int compression_level = 5,
                           int threads = 0,
                           bool use_basic_metadata = true);
    
    /**
     * Decompresses a previously compressed Parquet file
     * 
     * input_dir: Directory containing compressed files and metadata
     * output_file: Path where the decompressed Parquet file will be written
     * threads: Number of threads to use for decompression (0 for automatic)
     * 
     * Return: true on success, false on failure
     */
    bool decompressParquetFile(const std::string& input_dir,
                             const std::string& output_file,
                             int threads = 0);
    
    /**
     * Adds a custom metadata item based on an SQL query
     * 
     * name: Name of the custom metadata item
     * sql_query: SQL query to generate the metadata
     * 
     * Return: true on success, false on failure
     */
    bool addCustomMetadata(const std::string& name, const std::string& sql_query);
    
    /**
     * Loads custom metadata definitions from a JSON file
     * 
     * json_file: Path to the JSON file containing custom metadata definitions
     * 
     * Return: true on success, false on failure
     */
    bool loadCustomMetadataFromJson(const std::string& json_file);
    
    /**
     * Lists metadata for a compressed Parquet file
     * 
     * input_dir: Directory containing compressed files and metadata
     * 
     * Return: String containing metadata information, empty string on failure
     */
    std::string listMetadata(const std::string& input_dir);
    
    /**
     * Queries metadata using an SQL-like query
     * 
     * input_dir: Directory containing compressed files and metadata
     * query: SQL-like query to execute against the metadata
     * 
     * Return: String containing query results, empty string on failure
     */
    std::string queryMetadata(const std::string& input_dir, const std::string& query);
    
    /**
     * Gets the last error message
     * 
     * Return: Last error message, or empty string if no error occurred
     */
    std::string getLastError() const;
    
    /**
     * Gets the last error code
     * 
     * Return: Last error code, or 0 if no error occurred
     */
    int getLastErrorCode() const;
    
    /**
     * Sets the verbose mode
     * 
     * verbose: Whether to enable verbose output
     */
    void setVerbose(bool verbose);
    
private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace infparquet

#endif /* INFPARQUET_FRAMEWORK_H */ 