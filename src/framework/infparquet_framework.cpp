#include "framework/infparquet_framework.h"
#include "core/parquet_structure.h"
#include "core/parquet_reader.h"
#include "core/parquet_writer.h"
#include "metadata/metadata_generator.h"
#include "metadata/metadata_types.h"
#include "compression/lzma_compressor.h"
#include "compression/lzma_decompressor.h"
#include "compression/parallel_processor.h"
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <numeric>  // For std::accumulate
#include <sys/stat.h>  // For stat function
#include <algorithm>  // For std::min, std::sort, std::transform
#include <unordered_map>  // For std::unordered_map
#include <iomanip>  // For std::setw, std::setfill
#include <cmath>  // For std::isnan
#include "metadata/custom_metadata.h"
#include "metadata/sql_query_parser.h"

// Forward declare required functions from json_helper.h to avoid conflicts
extern "C" {
    typedef enum {
        JSON_HELPER_OK = 0,
        // Other enum values omitted
    } JsonHelperError;
    
    extern JsonHelperError json_parse_custom_metadata_config(const char* file_path, 
                                                          std::vector<std::string>& names, 
                                                          std::vector<std::string>& queries);
    extern const char* json_helper_get_last_error();
}

// Forward declare required functions from json_serialization.h to avoid conflicts
extern "C" {
    typedef enum {
        JSON_SERIALIZATION_OK = 0,
        // Other enum values omitted
    } JsonSerializationError;

    extern JsonSerializationError json_serialization_load_from_binary(const char* file_path, Metadata** metadata);
}

// Implementation of the missing functions
extern "C" {
    JsonHelperError json_parse_custom_metadata_config(const char* file_path, 
                                                   std::vector<std::string>& names, 
                                                   std::vector<std::string>& queries) {
        // Simple implementation for the missing function
        // In a real implementation, this would parse a JSON file
        if (!file_path) {
            return JSON_HELPER_OK; // Return empty vectors
        }
        
        // Try to open the file
        std::ifstream file(file_path);
        if (!file.is_open()) {
            return JSON_HELPER_OK; // Return empty vectors
        }
        
        // Simple parser for a JSON file with format:
        // { "custom_metadata": [
        //     {"name": "name1", "query": "query1"},
        //     {"name": "name2", "query": "query2"}
        //   ]
        // }
        
        std::string line;
        std::string name, query;
        bool in_name = false;
        bool in_query = false;
        
        while (std::getline(file, line)) {
            // Very simple JSON parsing
            if (line.find("\"name\"") != std::string::npos) {
                size_t start = line.find(":") + 1;
                size_t end = line.find(",", start);
                if (end == std::string::npos) {
                    end = line.find("}", start);
                }
                if (end != std::string::npos) {
                    name = line.substr(start, end - start);
                    // Clean up quotes and whitespace
                    name.erase(0, name.find_first_not_of(" \t\""));
                    name.erase(name.find_last_not_of(" \t\"") + 1);
                    in_name = true;
                }
            }
            
            if (line.find("\"query\"") != std::string::npos) {
                size_t start = line.find(":") + 1;
                size_t end = line.find(",", start);
                if (end == std::string::npos) {
                    end = line.find("}", start);
                }
                if (end != std::string::npos) {
                    query = line.substr(start, end - start);
                    // Clean up quotes and whitespace
                    query.erase(0, query.find_first_not_of(" \t\""));
                    query.erase(query.find_last_not_of(" \t\"") + 1);
                    in_query = true;
                }
            }
            
            // If we have both name and query, add them to the vectors
            if (in_name && in_query) {
                names.push_back(name);
                queries.push_back(query);
                in_name = false;
                in_query = false;
            }
        }
        
        return JSON_HELPER_OK;
    }
    
    static std::string last_error;
    
    const char* json_helper_get_last_error() {
        return last_error.c_str();
    }
}

namespace fs = std::filesystem;

// Forward declarations for parquet writer functions
struct ParquetWriterContext;
// Use the ParquetWriterError from parquet_writer.h
#define PARQUET_WRITER_OK 0

namespace infparquet {

// Constants for file path
#define MAX_FILE_PATH_LENGTH 1024

// LZMA compression level enum (define if not available from headers)
enum LzmaCompressionLevel {
    LZMA_LEVEL_MIN = 1,
    LZMA_LEVEL_DEFAULT = 5,
    LZMA_LEVEL_MAX = 9
};

// Metadata type definitions
#define METADATA_TYPE_FILE 1

// Metadata structure for compatibility
struct MetadataFields {
    int type;                    // Type of metadata
    int child_count;             // Number of child metadata items
    struct MetadataFields** child_metadata;  // Array of child metadata
    const char* name;            // Name of the metadata item
};

// Forward declarations for parquet writer functions
ParquetWriterContext* parquet_writer_create_context();
ParquetWriterError parquet_writer_free_context(ParquetWriterContext* context);
ParquetWriterError parquet_writer_add_column_data(
    ParquetWriterContext* context,
    int row_group_index,
    int column_index,
    void* data,
    size_t size
);

namespace {

// ParquetFile functions
ParquetFile* parquet_file_init(const char* file_path) {
    ParquetFile* file = (ParquetFile*)malloc(sizeof(ParquetFile));
    if (!file) return nullptr;
    
    memset(file, 0, sizeof(ParquetFile));
    if (file_path) {
        file->file_path = strdup(file_path);
    }
    
    return file;
}

void parquet_file_free(ParquetFile* file) {
    if (!file) return;
    
    // Free row groups
    for (int i = 0; i < file->row_group_count; i++) {
        ParquetRowGroup* row_group = &file->row_groups[i];
        
        // Free columns
        if (row_group->columns) {
            free(row_group->columns);
        }
    }
    
    // Free row groups array
    if (file->row_groups) {
        free(file->row_groups);
    }
    
    // Free the file path
    if (file->file_path) {
        free(file->file_path);
    }
    
    // Free the file structure
    free(file);
}

} // namespace

// Private implementation (Pimpl pattern)
class InfParquet::Impl {
public:
    Impl() : progress_callback(nullptr), verbose(false) {}
    ~Impl() {}
    
    // Last error message
    std::string last_error;
    
    // Progress callback function
    ProgressCallback progress_callback;
    
    // Verbose mode flag
    bool verbose;
    
    // Set the last error message
    void setError(const std::string& message) {
        last_error = message;
    }
    
    // Compression task data structure
    struct CompressionTaskData {
        const ParquetFile* file;
        int row_group_id;
        const std::string* output_directory;
        LzmaCompressionLevel compression_level;
    };
    
    // Compression task function
    static int compressRowGroup(void* task_data, void** result) {
        CompressionTaskData* data = static_cast<CompressionTaskData*>(task_data);
        
        // Get the row group
        const ParquetRowGroup* row_group = &data->file->row_groups[data->row_group_id];
        
        // Create a reader context to read the parquet file
        ParquetReaderContext* reader_context = parquet_reader_open(data->file->file_path);
        if (!reader_context) {
            // Return an error code if we can't open the file
            return 1;
        }
        
        int rc = 0;  // Return code
        
        // Compress each column in the row group
        for (int i = 0; i < row_group->column_count; i++) {
            const ParquetColumn* column = &row_group->columns[i];
            
            // Build the output file path
            std::stringstream ss;
            ss << *data->output_directory << "/" 
               << fs::path(data->file->file_path).filename().string() 
               << "_rg" << data->row_group_id 
               << "_col" << i << ".lzma";
            std::string output_path = ss.str();
            
            // Read the column data
            void* column_data = nullptr;
            size_t column_data_size = 0;
            ParquetReaderError read_error = parquet_reader_read_column(
                reader_context, 
                data->row_group_id, 
                i, 
                &column_data, 
                &column_data_size
            );
            
            if (read_error != PARQUET_READER_OK) {
                rc = 2;  // Read error
                break;
            }
            
            // Calculate the maximum compressed size
            uint64_t max_compressed_size = lzma_maximum_compressed_size(column_data_size);
            
            // Allocate memory for the compressed data
            void* compressed_data = malloc(max_compressed_size);
            if (!compressed_data) {
                parquet_reader_free_buffer(column_data);
                rc = 3;  // Memory allocation error
                break;
            }
            
            // Compress the column data
            uint64_t compressed_size = max_compressed_size;
            int compression_error = lzma_compress_buffer(
                column_data, 
                column_data_size, 
                compressed_data, 
                &compressed_size, 
                0,  // Default dictionary size
                static_cast<int>(data->compression_level)
            );
            
            if (compression_error != 0) {
                free(compressed_data);
                parquet_reader_free_buffer(column_data);
                rc = 4;  // Compression error
                break;
            }
            
            // Write the compressed data to the output file
            FILE* out = fopen(output_path.c_str(), "wb");
            if (!out) {
                free(compressed_data);
                parquet_reader_free_buffer(column_data);
                rc = 5;  // Failed to create output file
                break;
            }
            
            // Write the compressed data
            if (fwrite(compressed_data, 1, compressed_size, out) != compressed_size) {
                fclose(out);
                free(compressed_data);
                parquet_reader_free_buffer(column_data);
                rc = 6;  // Failed to write output file
                break;
            }
            
            // Close the output file
            fclose(out);
            
            // Free memory
            free(compressed_data);
            parquet_reader_free_buffer(column_data);
        }
        
        // Close the reader context
        parquet_reader_close(reader_context);
        
        // Allocate and return a result (not used in this example)
        int* ret_code = static_cast<int*>(malloc(sizeof(int)));
        if (ret_code) {
            *ret_code = rc;
            *result = ret_code;
        }
        
        return rc;
    }
    
    // Decompression task data structure
    struct DecompressionTaskData {
        const Metadata* file_metadata;
        int row_group_id;
        const std::string* output_directory;
        std::vector<std::string>* column_files;
    };
    
    // Decompression task function
    static int decompressRowGroup(void* task_data, void** result) {
        DecompressionTaskData* data = static_cast<DecompressionTaskData*>(task_data);
        
        // Access metadata safely using the helper functions
        const Metadata* metadata = data->file_metadata;
        const MetadataFields* fields = reinterpret_cast<const MetadataFields*>(metadata);
        
        // Get the child metadata for this row group
        Metadata* rowGroupMetadata = nullptr;
        if (fields && data->row_group_id >= 0 && 
            data->row_group_id < fields->child_count) {
            rowGroupMetadata = reinterpret_cast<Metadata*>(fields->child_metadata[data->row_group_id]);
        }
        
        // Get the number of columns in this row group
        int columnCount = 0;
        if (rowGroupMetadata) {
            columnCount = reinterpret_cast<MetadataFields*>(rowGroupMetadata)->child_count;
        }
        
        // Build the file names for each column and prepare data structures
        std::vector<std::string> files;
        std::string input_directory = fs::path(fields->name).parent_path().string();
        std::vector<void*> decompressed_data;
        std::vector<size_t> decompressed_sizes;
        
        // Reserve space to avoid reallocations
        files.reserve(columnCount);
        decompressed_data.reserve(columnCount);
        decompressed_sizes.reserve(columnCount);
        
        // Collect all column files and prepare for decompression
        for (int i = 0; i < columnCount; i++) {
            // Build the input file path
            std::stringstream ss;
            ss << input_directory << "/" << fs::path(fields->name).filename().string() 
               << "_rg" << data->row_group_id 
               << "_col" << i << ".lzma";
            std::string file_path = ss.str();
            files.push_back(file_path);
            
            // Check if file exists
            if (!fs::exists(file_path)) {
                // Log error and continue with other columns
                std::cerr << "Error: Compressed file not found: " << file_path << std::endl;
                decompressed_data.push_back(nullptr);
                decompressed_sizes.push_back(0);
                continue;
            }
            
            // Open the compressed file
            FILE* compressed_file = fopen(file_path.c_str(), "rb");
            if (!compressed_file) {
                // Log error and continue with other columns
                std::cerr << "Error: Failed to open compressed file: " << file_path << std::endl;
                decompressed_data.push_back(nullptr);
                decompressed_sizes.push_back(0);
                continue;
            }
            
            // Get file size
            fseek(compressed_file, 0, SEEK_END);
            size_t compressed_size = ftell(compressed_file);
            rewind(compressed_file);
            
            // Allocate memory for compressed data
            void* compressed_data = malloc(compressed_size);
            if (!compressed_data) {
                // Log error and continue with other columns
                std::cerr << "Error: Failed to allocate memory for compressed data" << std::endl;
                fclose(compressed_file);
                decompressed_data.push_back(nullptr);
                decompressed_sizes.push_back(0);
                continue;
            }
            
            // Read compressed data
            size_t bytes_read = fread(compressed_data, 1, compressed_size, compressed_file);
            fclose(compressed_file);
            
            if (bytes_read != compressed_size) {
                // Log error and continue with other columns
                std::cerr << "Error: Failed to read compressed data: " << file_path << std::endl;
                free(compressed_data);
                decompressed_data.push_back(nullptr);
                decompressed_sizes.push_back(0);
                continue;
            }
            
            // Get the decompressed size (stored in the LZMA header or from metadata)
            size_t decompressed_size = lzma_get_decompressed_size(compressed_data, compressed_size);
            
            if (decompressed_size == 0) {
                // If header doesn't contain size or error occurred, use a reasonable estimate
                // In a real implementation, this would be stored in metadata
                decompressed_size = compressed_size * 4; // Estimate: compression ratio of 4:1
            }
            
            // Allocate memory for decompressed data
            void* decompressed_buffer = malloc(decompressed_size);
            if (!decompressed_buffer) {
                // Log error and continue with other columns
                std::cerr << "Error: Failed to allocate memory for decompressed data" << std::endl;
                free(compressed_data);
                decompressed_data.push_back(nullptr);
                decompressed_sizes.push_back(0);
                continue;
            }
            
            // Decompress data
            int decomp_result = lzma_decompress_buffer(
                compressed_data, compressed_size,
                decompressed_buffer, &decompressed_size
            );
            
            // Free compressed data as it's no longer needed
            free(compressed_data);
            
            if (decomp_result != 0) {
                // Log error and continue with other columns
                std::cerr << "Error: Failed to decompress data: " << file_path 
                          << " (error code: " << decomp_result << ")" << std::endl;
                free(decompressed_buffer);
                decompressed_data.push_back(nullptr);
                decompressed_sizes.push_back(0);
                continue;
            }
            
            // Store decompressed data
            decompressed_data.push_back(decompressed_buffer);
            decompressed_sizes.push_back(decompressed_size);
        }
        
        // Store the column files in the output vector
        *data->column_files = files;
        
        // Create a result structure to pass back decompressed data
        struct DecompressionResult {
            std::vector<void*> data;
            std::vector<size_t> sizes;
            int row_group_id;
        };
        
        DecompressionResult* decomp_result = new DecompressionResult{
            decompressed_data,
            decompressed_sizes,
            data->row_group_id
        };
        
        *result = decomp_result;
        
        return 0;  // Success
    }
    
    // Compress a parquet file
    FrameworkError compressParquetFile(
        const std::string& input_path,
        const std::string& output_directory,
        const CompressionOptions& options,
        ProgressCallback progress_callback
    ) {
        // Make sure the output directory exists
        if (!fs::exists(output_directory)) {
            try {
                fs::create_directories(output_directory);
            } catch (const std::exception& e) {
                setError("Failed to create output directory: " + std::string(e.what()));
                return FrameworkError::PERMISSION_DENIED;
            }
        }
        
        // Open the parquet file
        ParquetReaderContext* reader_context = parquet_reader_open(input_path.c_str());
        if (!reader_context) {
            setError("Failed to open parquet file: " + input_path);
            return FrameworkError::FILE_NOT_FOUND;
        }
        
        // Initialize the parquet file structure
        ParquetFile* file = parquet_file_init(input_path.c_str());
        if (!file) {
            parquet_reader_close(reader_context);
            setError("Failed to initialize parquet file structure");
            return FrameworkError::MEMORY_ERROR;
        }
        
        // Load the parquet file structure
        ParquetReaderError reader_error = parquet_reader_get_structure(reader_context, file);
        if (reader_error != PARQUET_READER_OK) {
            parquet_file_free(file);
            parquet_reader_close(reader_context);
            setError("Failed to load parquet file structure: " + 
                     std::string(parquet_reader_get_error(reader_context)));
            return FrameworkError::PARQUET_ERROR;
        }
        
        if (progress_callback) {
            progress_callback("Parquet file structure loaded", -1, file->row_group_count, 10);
        }
        
        // Generate metadata for the parquet file
        MetadataGeneratorOptions generator_options;
        metadata_generator_init_options(&generator_options);
        generator_options.generate_base_metadata = options.generate_base_metadata;
        generator_options.generate_custom_metadata = options.generate_custom_metadata;
        if (options.generate_custom_metadata && !options.custom_metadata_config.empty()) {
            generator_options.custom_metadata_config_path = options.custom_metadata_config.c_str();
        }
        
        Metadata* file_metadata = nullptr;
        MetadataGeneratorError metadata_error = metadata_generator_generate(
            file, reader_context, &generator_options, &file_metadata);
        
        if (metadata_error != METADATA_GEN_OK) {
            parquet_file_free(file);
            parquet_reader_close(reader_context);
            setError("Failed to generate metadata: " + 
                     std::string(metadata_generator_get_error()));
            return FrameworkError::METADATA_ERROR;
        }
        
        if (progress_callback) {
            progress_callback("Metadata generated", -1, file->row_group_count, 20);
        }
        
        // Save the file metadata
        std::string metadata_path = output_directory + "/" + 
                                  fs::path(input_path).filename().string() + ".meta";
        
        metadata_error = metadata_generator_save_metadata(file_metadata, metadata_path.c_str());
        if (metadata_error != METADATA_GEN_OK) {
            metadata_generator_free_metadata(file_metadata);
            parquet_file_free(file);
            parquet_reader_close(reader_context);
            setError("Failed to save metadata: " + 
                     std::string(metadata_generator_get_error()));
            return FrameworkError::METADATA_ERROR;
        }
        
        if (progress_callback) {
            progress_callback("Metadata saved", -1, file->row_group_count, 30);
        }
        
        // Set the maximum number of parallel tasks
        if (options.parallel_tasks > 0) {
            parallel_processor_set_max_tasks(options.parallel_tasks);
        }
        
        // Set up task data for parallel processing
        std::vector<CompressionTaskData> task_data(file->row_group_count);
        std::vector<void*> task_data_ptrs(file->row_group_count);
        
        for (int i = 0; i < file->row_group_count; i++) {
            task_data[i].file = file;
            task_data[i].row_group_id = i;
            task_data[i].output_directory = &output_directory;
            task_data[i].compression_level = static_cast<LzmaCompressionLevel>(options.compression_level);
            task_data_ptrs[i] = &task_data[i];
        }
        
        // Process the row groups in parallel
        void** task_results = nullptr;
        ParallelProcessorError parallel_error = parallel_processor_process_row_groups(
            file,
            compressRowGroup,
            task_data_ptrs.data(),
            nullptr,
            &task_results
        );
        
        if (parallel_error != PARALLEL_PROCESSOR_OK) {
            metadata_generator_free_metadata(file_metadata);
            parquet_file_free(file);
            parquet_reader_close(reader_context);
            setError("Failed to process row groups: " + 
                     std::string(parallel_processor_get_error()));
            return FrameworkError::PARALLEL_PROCESSING_ERROR;
        }
        
        if (progress_callback) {
            progress_callback("File compression completed", -1, file->row_group_count, 90);
        }
        
        // Free the task results
        parallel_processor_free_results(task_results, file->row_group_count);
        
        // Clean up
        metadata_generator_free_metadata(file_metadata);
        parquet_file_free(file);
        parquet_reader_close(reader_context);
        
        if (progress_callback) {
            progress_callback("Compression process completed", -1, file->row_group_count, 100);
        }
        
        return FrameworkError::OK;
    }
    
    // Decompress a previously compressed parquet file
    FrameworkError decompressParquetFile(
        const std::string& metadata_path,
        const std::string& output_directory,
        const DecompressionOptions& options,
        ProgressCallback progress_callback
    ) {
        // Make sure the output directory exists
        if (!fs::exists(output_directory)) {
            try {
                fs::create_directories(output_directory);
            } catch (const std::exception& e) {
                setError("Failed to create output directory: " + std::string(e.what()));
                return FrameworkError::PERMISSION_DENIED;
            }
        }
        
        // Load the metadata file
        Metadata* file_metadata = nullptr;
        MetadataGeneratorError metadata_error = metadata_generator_load_metadata(
            metadata_path.c_str(), &file_metadata);
        
        if (metadata_error != METADATA_GEN_OK) {
            setError("Failed to load metadata: " + 
                     std::string(metadata_generator_get_error()));
            return FrameworkError::METADATA_ERROR;
        }
        
        if (progress_callback) {
            progress_callback("Metadata loaded", -1, getMetadataChildCount(file_metadata), 10);
        }
        
        // Make sure it's a file-level metadata
        if (getMetadataType(file_metadata) != METADATA_TYPE_FILE) {
            metadata_generator_free_metadata(file_metadata);
            setError("Invalid metadata type: expected file-level metadata");
            return FrameworkError::METADATA_ERROR;
        }
        
        // Get the input directory (where the compressed files are)
        std::string input_directory = fs::path(metadata_path).parent_path().string();
        
        // Set the maximum number of parallel tasks
        if (options.parallel_tasks > 0) {
            parallel_processor_set_max_tasks(options.parallel_tasks);
        }
        
        // Set up task data for parallel processing
        int childCount = getMetadataChildCount(file_metadata);
        std::vector<DecompressionTaskData> task_data(childCount);
        std::vector<void*> task_data_ptrs(childCount);
        std::vector<std::vector<std::string>> column_files(childCount);
        
        for (int i = 0; i < childCount; i++) {
            task_data[i].file_metadata = file_metadata;
            task_data[i].row_group_id = i;
            task_data[i].output_directory = &output_directory;
            task_data[i].column_files = &column_files[i];
            task_data_ptrs[i] = &task_data[i];
        }
        
        // Process the row groups in parallel
        void** task_results = nullptr;
        
        // Create a proper ParquetFile structure from the metadata
        ParquetFile parquet_file;
        memset(&parquet_file, 0, sizeof(ParquetFile));
        
        // Set file path - use the original file path from metadata
        parquet_file.file_path = file_metadata->file_path ? strdup(file_metadata->file_path) : strdup("unknown.parquet");
        
        // Set row group count
        parquet_file.row_group_count = childCount;
        
        // Allocate memory for row groups
        parquet_file.row_groups = (ParquetRowGroup*)malloc(childCount * sizeof(ParquetRowGroup));
        if (!parquet_file.row_groups) {
            metadata_generator_free_metadata(file_metadata);
            setError("Failed to allocate memory for row groups");
            return FrameworkError::MEMORY_ERROR;
        }
        
        // Initialize row groups with data from metadata
        memset(parquet_file.row_groups, 0, childCount * sizeof(ParquetRowGroup));
        uint64_t total_rows = 0;
        
        for (int i = 0; i < childCount; i++) {
            ParquetRowGroup* row_group = &parquet_file.row_groups[i];
            row_group->row_group_index = i;
            
            // Get row group metadata
            Metadata* row_group_metadata = getChildMetadata(file_metadata, i);
            if (row_group_metadata) {
                // Get number of columns
                int column_count = getMetadataChildCount(row_group_metadata);
                row_group->column_count = column_count;
                
                // Get row count if available in metadata
                const MetadataItem* rowCountItem = nullptr;
                for (uint32_t j = 0; j < row_group_metadata->file_metadata.basic_metadata_count; j++) {
                    if (strcmp(row_group_metadata->file_metadata.basic_metadata[j].name, "row_count") == 0) {
                        rowCountItem = &row_group_metadata->file_metadata.basic_metadata[j];
                        break;
                    }
                }
                
                if (rowCountItem && rowCountItem->type == METADATA_TYPE_NUMERIC) {
                    row_group->num_rows = static_cast<uint64_t>(rowCountItem->value.numeric.mode_value);
                    total_rows += row_group->num_rows;
                }
            }
        }
        
        // Set total rows for the file
        parquet_file.total_rows = total_rows;
        
        ParallelProcessorError parallel_error = parallel_processor_process_row_groups(
            &parquet_file,
            decompressRowGroup,
            task_data_ptrs.data(),
            nullptr,
            &task_results
        );
        
        if (parallel_error != PARALLEL_PROCESSOR_OK) {
            // Clean up allocated resources
            parquet_file_free(&parquet_file);
            metadata_generator_free_metadata(file_metadata);
            setError("Failed to process row groups: " + 
                     std::string(parallel_processor_get_error()));
            return FrameworkError::PARALLEL_PROCESSING_ERROR;
        }
        
        if (progress_callback) {
            progress_callback("Column files decompressed", -1, childCount, 50);
        }
        
        // Free the task results
        parallel_processor_free_results(task_results, childCount);
        
        // Reconstruct the parquet file from metadata and row group data
        std::string output_path = output_directory + "/" + 
                               fs::path(getMetadataName(file_metadata)).filename().string();
        
        // Replace .meta extension with .parquet
        output_path = output_path.substr(0, output_path.length() - 5) + ".parquet";
        
        // Collect all column file paths for reconstruction
        std::vector<char*> file_paths;
        for (const auto& group_files : column_files) {
            for (const auto& file : group_files) {
                file_paths.push_back(strdup(file.c_str()));
            }
        }
        
        // Convert vector to array for C API
        char** column_files_array = nullptr;
        if (!file_paths.empty()) {
            column_files_array = file_paths.data();
        }
        
        // Use the parquet writer to reconstruct the file
        ParquetWriterError writer_error = parquet_writer_reconstruct_file(
            &parquet_file,         // File structure matches the signature
            output_path.c_str(),   // Output path is correct
            column_files_array     // Array of column file paths
        );
        
        // Free the column file path strings
        for (char* path : file_paths) {
            free(path);
        }
        
        // Clean up
        parquet_file_free(&parquet_file);
        
        if (writer_error != PARQUET_WRITER_OK) {
            metadata_generator_free_metadata(file_metadata);
            setError("Failed to reconstruct parquet file");
            return FrameworkError::WRITER_ERROR;
        }
        
        if (progress_callback) {
            progress_callback("Parquet file reconstructed", -1, childCount, 90);
        }
        
        // Clean up
        metadata_generator_free_metadata(file_metadata);
        
        // Clean up temporary column files
        for (uint32_t i = 0; i < childCount; i++) {
            for (const auto& column_file : column_files[i]) {
                std::error_code ec;
                fs::remove(column_file, ec);
            }
        }
        
        if (progress_callback) {
            progress_callback("Decompression process completed", -1, childCount, 100);
        }
        
        return FrameworkError::OK;
    }
    
    // Query metadata for specific patterns or values
    FrameworkError queryMetadata(
        const std::string& metadata_directory,
        const std::string& query,
        MetadataQueryResult* results
    ) {
        if (!results) {
            setError("Invalid parameter: results cannot be null");
            return FrameworkError::INVALID_PARAMETER;
        }
        
        // Initialize the results
        results->success = false;
        results->message = "";
        results->matching_files.clear();
        results->matching_row_groups.clear();
        results->matching_columns.clear();
        
        // Make sure the metadata directory exists
        if (!fs::exists(metadata_directory)) {
            setError("Metadata directory not found: " + metadata_directory);
            results->message = "Metadata directory not found";
            return FrameworkError::FILE_NOT_FOUND;
        }
        
        // Parse the SQL query
        SQLQueryInfo* query_info = (SQLQueryInfo*)malloc(sizeof(SQLQueryInfo));
        if (!query_info) {
            setError("Failed to allocate memory for SQL query info");
            results->message = "Memory allocation failed";
            return FrameworkError::MEMORY_ERROR;
        }
        
        // Initialize query info
        memset(query_info, 0, sizeof(SQLQueryInfo));
        
        // Parse the query
        if (!parse_sql_query(query.c_str(), query_info)) {
            std::string error_msg = "Failed to parse SQL query";
            const char* sql_error = get_sql_error_message();
            if (sql_error) {
                error_msg += ": ";
                error_msg += sql_error;
            }
            setError(error_msg);
            results->message = error_msg;
            free_sql_query_info(query_info);
            return FrameworkError::INVALID_QUERY;
        }
        
        // Check if the table name is valid
        if (!query_info->from_table || strcmp(query_info->from_table, "metadata") != 0) {
            std::string error_msg = "Invalid table name in query: ";
            error_msg += query_info->from_table ? query_info->from_table : "null";
            setError(error_msg);
            results->message = error_msg;
            free_sql_query_info(query_info);
            return FrameworkError::INVALID_QUERY;
        }
        
        // Scan the directory for metadata files
        std::vector<std::string> metadata_files;
        try {
            for (const auto& entry : fs::directory_iterator(metadata_directory)) {
                if (entry.is_regular_file() && entry.path().extension() == ".meta") {
                    metadata_files.push_back(entry.path().string());
                }
            }
        } catch (const std::exception& e) {
            std::string error_msg = "Failed to scan metadata directory: " + std::string(e.what());
            setError(error_msg);
            results->message = error_msg;
            free_sql_query_info(query_info);
            return FrameworkError::PERMISSION_DENIED;
        }
        
        // If no metadata files found
        if (metadata_files.empty()) {
            std::string error_msg = "No metadata files found in directory";
            setError(error_msg);
            results->message = error_msg;
            free_sql_query_info(query_info);
            return FrameworkError::FILE_NOT_FOUND;
        }
        
        // Process each metadata file
        for (const auto& meta_file : metadata_files) {
            // Load the metadata file
            Metadata* metadata = nullptr;
            JsonSerializationError json_error = json_serialization_load_from_binary(
                meta_file.c_str(),
                &metadata
            );
            
            if (json_error != JSON_SERIALIZATION_OK || !metadata) {
                // Skip files that can't be loaded
                continue;
            }
            
            // Convert Metadata to MetadataCollection for the SQL executor
            MetadataCollection collection;
            memset(&collection, 0, sizeof(MetadataCollection));
            
            // Create at least one container for file-level metadata
            collection.count = 1;
            collection.items = (MetadataContainer*)malloc(sizeof(MetadataContainer));
            if (!collection.items) {
                metadata_generator_free_metadata(metadata);
                continue;
            }
            
            // Initialize the container
            MetadataContainer* container = &collection.items[0];
            memset(container, 0, sizeof(MetadataContainer));
            
            // Add file-level metadata to the container
            // This is a simplified approach - a more complete implementation would
            // convert all metadata items to key-value pairs
            int pair_count = 3; // file_path, file_name, file_size
            
            container->count = pair_count;
            container->keys = (char**)malloc(pair_count * sizeof(char*));
            container->values = (char**)malloc(pair_count * sizeof(char*));
            
            if (!container->keys || !container->values) {
                if (container->keys) free(container->keys);
                if (container->values) free(container->values);
                free(collection.items);
                metadata_generator_free_metadata(metadata);
                continue;
            }
            
            // Fill in the metadata key-value pairs
            container->keys[0] = strdup("file_path");
            container->values[0] = strdup(metadata->file_path ? metadata->file_path : "");
            
            // Extract file name from path
            std::string file_name = fs::path(meta_file).stem().string();
            container->keys[1] = strdup("file_name");
            container->values[1] = strdup(file_name.c_str());
            
            // Get actual file size
            std::error_code ec;
            uintmax_t file_size = fs::file_size(meta_file, ec);
            if (ec) {
                // If we can't get the file size, use the stat function as a fallback
                struct stat file_stat;
                if (stat(meta_file.c_str(), &file_stat) == 0) {
                    file_size = file_stat.st_size;
                } else {
                    file_size = 0; // Use 0 only as last resort
                }
            }
            
            container->keys[2] = strdup("file_size");
            container->values[2] = strdup(std::to_string(file_size).c_str());
            
            // Execute the SQL query against this metadata
            SQLResultSet result_set;
            memset(&result_set, 0, sizeof(SQLResultSet));
            
            if (execute_sql_query(query_info, &collection, &result_set)) {
                // If we got results, add them to the output
                if (result_set.row_count > 0) {
                    // Get the file name without path and extension
                    std::string file_name = fs::path(meta_file).stem().string();
                    
                    // Add file to matching files if it's not already there
                    if (std::find(results->matching_files.begin(), 
                                results->matching_files.end(), 
                                file_name) == results->matching_files.end()) {
                        results->matching_files.push_back(file_name);
                    }
                    
                    // Process each row in the result set
                    for (int i = 0; i < result_set.row_count; i++) {
                        MetadataRow* row = &result_set.rows[i];
                        
                        // Check if this row has row group and column information
                        std::string row_group_name;
                        std::string column_name;
                        
                        for (int j = 0; j < row->count; j++) {
                            if (row->columns[j] && row->values[j]) {
                                if (strcmp(row->columns[j], "row_group") == 0) {
                                    row_group_name = row->values[j];
                                } else if (strcmp(row->columns[j], "column") == 0) {
                                    column_name = row->values[j];
                                }
                            }
                        }
                        
                        // Add row group if we have it
                        if (!row_group_name.empty()) {
                            std::string row_group_id = file_name + ":" + row_group_name;
                            if (std::find(results->matching_row_groups.begin(), 
                                        results->matching_row_groups.end(), 
                                        row_group_id) == results->matching_row_groups.end()) {
                                results->matching_row_groups.push_back(row_group_id);
                            }
                            
                            // Add column if we have it
                            if (!column_name.empty()) {
                                std::string column_id = row_group_id + ":" + column_name;
                                if (std::find(results->matching_columns.begin(), 
                                            results->matching_columns.end(), 
                                            column_id) == results->matching_columns.end()) {
                                    results->matching_columns.push_back(column_id);
                                }
                            }
                        }
                    }
                }
                
                // Free the result set
                free_sql_result_set(&result_set);
            }
            
            // Free container resources
            for (int i = 0; i < container->count; i++) {
                if (container->keys[i]) free(container->keys[i]);
                if (container->values[i]) free(container->values[i]);
            }
            free(container->keys);
            free(container->values);
            
            // Free collection resources
            free(collection.items);
            
            // Free the metadata
            metadata_generator_free_metadata(metadata);
        }
        
        // Clean up
        free_sql_query_info(query_info);
        
        // Set success flag based on whether we found any matches
        results->success = !results->matching_files.empty();
        
        // Set appropriate message
        if (results->success) {
            results->message = "Query executed successfully";
        } else {
            results->message = "No matching metadata found";
        }
        
        return FrameworkError::OK;
    }
    
    // List all available metadata in a directory
    FrameworkError listMetadataFiles(
        const std::string& metadata_directory,
        std::vector<std::string>* file_list
    ) {
        if (!file_list) {
            setError("Invalid parameter: file_list cannot be null");
            return FrameworkError::INVALID_PARAMETER;
        }
        
        // Clear the file list
        file_list->clear();
        
        // Make sure the metadata directory exists
        if (!fs::exists(metadata_directory)) {
            setError("Metadata directory not found: " + metadata_directory);
            return FrameworkError::FILE_NOT_FOUND;
        }
        
        // Scan the directory for metadata files
        try {
            for (const auto& entry : fs::directory_iterator(metadata_directory)) {
                if (entry.is_regular_file() && entry.path().extension() == ".meta") {
                    file_list->push_back(entry.path().filename().string());
                }
            }
        } catch (const std::exception& e) {
            setError("Failed to scan metadata directory: " + std::string(e.what()));
            return FrameworkError::PERMISSION_DENIED;
        }
        
        return FrameworkError::OK;
    }

    /**
     * generateColumnMetadata method implementation
     * 
     * This function analyzes column data and generates appropriate metadata based on the column type.
     */
    bool generateColumnMetadata(const ParquetColumn* column, 
                               const void* column_data, 
                               size_t column_size,
                               int column_index,
                               Metadata* metadata) {
        if (!column || !column_data || column_size == 0 || !metadata) {
            setError("Invalid parameters for metadata generation");
            return false;
        }
        
        // Add a metadata item for this column to the metadata structure
        ColumnMetadata* col_metadata = nullptr;
        
        // Check if column metadata already exists
        for (uint32_t i = 0; i < metadata->column_metadata_count; i++) {
            if (metadata->column_metadata[i].column_index == column_index) {
                col_metadata = &metadata->column_metadata[i];
                break;
            }
        }
        
        // If not found, create new column metadata
        if (!col_metadata) {
            // Allocate or reallocate array of column metadata
            uint32_t new_count = metadata->column_metadata_count + 1;
            ColumnMetadata* new_array = (ColumnMetadata*)realloc(
                metadata->column_metadata, 
                new_count * sizeof(ColumnMetadata)
            );
            
            if (!new_array) {
                setError("Failed to allocate memory for column metadata");
                return false;
            }
            
            metadata->column_metadata = new_array;
            col_metadata = &metadata->column_metadata[metadata->column_metadata_count];
            metadata->column_metadata_count = new_count;
            
            // Initialize the new column metadata
            memset(col_metadata, 0, sizeof(ColumnMetadata));
            col_metadata->column_index = column_index;
            strncpy(col_metadata->column_name, column->name, MAX_METADATA_ITEM_NAME_LENGTH - 1);
            col_metadata->column_name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
        }
        
        // Generate metadata based on column type
        bool success = false;
        
        switch (column->type) {
            case PARQUET_TIMESTAMP:
                success = generateTimestampMetadata(column, column_data, column_size, col_metadata);
                break;
                
            case PARQUET_STRING:
            case PARQUET_BYTE_ARRAY:
                success = generateStringMetadata(column, column_data, column_size, col_metadata);
                break;
                
            case PARQUET_INT32:
            case PARQUET_INT64:
            case PARQUET_FLOAT:
            case PARQUET_DOUBLE:
                success = generateNumericMetadata(column, column_data, column_size, col_metadata);
                break;
                
            case PARQUET_BOOLEAN:
            case PARQUET_INT96:
            case PARQUET_FIXED_LEN_BYTE_ARRAY:
                success = generateCategoricalMetadata(column, column_data, column_size, col_metadata);
                break;
                
            default:
                // Unknown type, skip metadata generation
                setError("Unknown column type for metadata generation");
                return false;
        }
        
        return success;
    }

    /**
     * generateTimestampMetadata method implementation
     * 
     * This function generates timestamp metadata for a column.
     */
    bool generateTimestampMetadata(const ParquetColumn* column, 
                                  const void* column_data, 
                                  size_t column_size,
                                  ColumnMetadata* metadata) {
        if (!column || !column_data || column_size == 0 || !metadata) {
            setError("Invalid parameters for timestamp metadata generation");
            return false;
        }
        
        // Create a new metadata item for timestamp
        MetadataItem* item = addColumnMetadataItem(metadata, "timestamp", METADATA_TYPE_TIMESTAMP);
        if (!item) {
            setError("Failed to add timestamp metadata item");
            return false;
        }
        
        // Initialize timestamp metadata with defaults
        TimestampMetadata* ts_metadata = &item->value.timestamp;
        ts_metadata->min_timestamp = 0;
        ts_metadata->max_timestamp = 0;
        ts_metadata->count = 0;
        
        // Parse timestamp data
        const time_t* timestamps = (const time_t*)column_data;
        size_t count = column_size / sizeof(time_t);
        
        if (count == 0) {
            // No data, keep default values
            return true;
        }
        
        // Set initial min/max values from the first timestamp
        ts_metadata->min_timestamp = timestamps[0];
        ts_metadata->max_timestamp = timestamps[0];
        ts_metadata->count = count;
        
        // Process the rest of the timestamps
        for (size_t i = 1; i < count; i++) {
            if (timestamps[i] < ts_metadata->min_timestamp) {
                ts_metadata->min_timestamp = timestamps[i];
            }
            
            if (timestamps[i] > ts_metadata->max_timestamp) {
                ts_metadata->max_timestamp = timestamps[i];
            }
        }
        
        return true;
    }

    /**
     * generateStringMetadata method implementation
     * 
     * This function generates string metadata for a column.
     */
    bool generateStringMetadata(const ParquetColumn* column, 
                               const void* column_data, 
                               size_t column_size,
                               ColumnMetadata* metadata) {
        if (!column || !column_data || column_size == 0 || !metadata) {
            setError("Invalid parameters for string metadata generation");
            return false;
        }
        
        // Create a new metadata item for string
        MetadataItem* item = addColumnMetadataItem(metadata, "string", METADATA_TYPE_STRING);
        if (!item) {
            setError("Failed to add string metadata item");
            return false;
        }
        
        // Initialize string metadata with defaults
        StringMetadata* str_metadata = &item->value.string;
        memset(str_metadata, 0, sizeof(StringMetadata));
        
        // Parse string data
        // For simplicity, assume data is in the format:
        // uint32_t length1, char[length1], uint32_t length2, char[length2], ...
        
        const uint8_t* data = (const uint8_t*)column_data;
        size_t offset = 0;
        
        // Structure to track string frequency
        struct StringCount {
            char value[MAX_STRING_LENGTH];
            uint32_t count;
        };
        
        // Map to store string frequencies
        std::unordered_map<std::string, uint32_t> string_counts;
        
        // Special strings to look for (error-related)
        const char* special_strings[] = {
            "error", "exception", "fail", "bug", "crash",
            "invalid", "fatal", "critical", "warning", "issue"
        };
        std::unordered_map<std::string, uint32_t> special_counts;
        
        // Process all strings
        uint64_t total_length = 0;
        uint64_t string_count = 0;
        
        while (offset + sizeof(uint32_t) <= column_size) {
            // Get string length
            uint32_t length = 0;
            memcpy(&length, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            
            // Check if we have enough data for the string
            if (offset + length > column_size) {
                break;
            }
            
            // Get the string
            char* str = new char[length + 1];
            memcpy(str, data + offset, length);
            str[length] = '\0';
            offset += length;
            
            // Convert to std::string for easier handling
            std::string s(str);
            delete[] str;
            
            // Update total length and count
            total_length += length;
            string_count++;
            
            // Update frequency map
            string_counts[s]++;
            
            // Check for special strings
            std::string lower_s = s;
            std::transform(lower_s.begin(), lower_s.end(), lower_s.begin(), ::tolower);
            
            for (const char* special : special_strings) {
                if (lower_s.find(special) != std::string::npos) {
                    special_counts[s]++;
                    break;
                }
            }
        }
        
        // Calculate average string length
        str_metadata->avg_string_length = (string_count > 0) ? 
                                          total_length / string_count : 0;
        str_metadata->total_string_count = string_count;
        
        // Get top N frequent strings
        std::vector<std::pair<std::string, uint32_t>> sorted_strings;
        for (const auto& entry : string_counts) {
            sorted_strings.push_back(entry);
        }
        
        // Sort by frequency (descending)
        std::sort(sorted_strings.begin(), sorted_strings.end(),
                 [](const std::pair<std::string, uint32_t>& a, 
                    const std::pair<std::string, uint32_t>& b) {
                     return a.second > b.second;
                 });
        
        // Store top N most frequent strings
        str_metadata->high_freq_count = std::min((uint32_t)sorted_strings.size(), 
                                               (uint32_t)MAX_HIGH_FREQ_STRINGS);
        
        for (uint32_t i = 0; i < str_metadata->high_freq_count; i++) {
            strncpy(str_metadata->high_freq_strings[i].string, 
                   sorted_strings[i].first.c_str(), 
                   MAX_STRING_LENGTH - 1);
            str_metadata->high_freq_strings[i].string[MAX_STRING_LENGTH - 1] = '\0';
            str_metadata->high_freq_strings[i].count = sorted_strings[i].second;
            str_metadata->high_freq_counts[i] = sorted_strings[i].second;  // Update both for backward compatibility
        }
        
        // Get special strings
        std::vector<std::pair<std::string, uint32_t>> sorted_special;
        for (const auto& entry : special_counts) {
            sorted_special.push_back(entry);
        }
        
        // Sort by frequency (descending)
        std::sort(sorted_special.begin(), sorted_special.end(),
                 [](const std::pair<std::string, uint32_t>& a, 
                    const std::pair<std::string, uint32_t>& b) {
                     return a.second > b.second;
                 });
        
        // Store special strings
        str_metadata->special_string_count = std::min((uint32_t)sorted_special.size(), 
                                                    (uint32_t)MAX_SPECIAL_STRINGS);
        
        for (uint32_t i = 0; i < str_metadata->special_string_count; i++) {
            strncpy(str_metadata->special_strings[i], 
                   sorted_special[i].first.c_str(), 
                   MAX_STRING_LENGTH - 1);
            str_metadata->special_strings[i][MAX_STRING_LENGTH - 1] = '\0';
            str_metadata->special_string_counts[i] = sorted_special[i].second;
        }
        
        return true;
    }

    /**
     * generateNumericMetadata method implementation
     * 
     * This function generates numeric metadata for a column.
     */
    bool generateNumericMetadata(const ParquetColumn* column, 
                                const void* column_data, 
                                size_t column_size,
                                ColumnMetadata* metadata) {
        if (!column || !column_data || column_size == 0 || !metadata) {
            setError("Invalid parameters for numeric metadata generation");
            return false;
        }
        
        // Create a new metadata item for numeric
        MetadataItem* item = addColumnMetadataItem(metadata, "numeric", METADATA_TYPE_NUMERIC);
        if (!item) {
            setError("Failed to add numeric metadata item");
            return false;
        }
        
        // Initialize numeric metadata with defaults
        NumericMetadata* num_metadata = &item->value.numeric;
        memset(num_metadata, 0, sizeof(NumericMetadata));
        
        // Process data based on column type
        std::vector<double> values;
        uint32_t null_count = 0;
        
        switch (column->type) {
            case PARQUET_INT32: {
                const int32_t* data = (const int32_t*)column_data;
                size_t count = column_size / sizeof(int32_t);
                
                for (size_t i = 0; i < count; i++) {
                    if (data[i] == INT32_MIN) {
                        // Assume INT32_MIN represents null
                        null_count++;
                    } else {
                        values.push_back(static_cast<double>(data[i]));
                    }
                }
                break;
            }
            
            case PARQUET_INT64: {
                const int64_t* data = (const int64_t*)column_data;
                size_t count = column_size / sizeof(int64_t);
                
                for (size_t i = 0; i < count; i++) {
                    if (data[i] == INT64_MIN) {
                        // Assume INT64_MIN represents null
                        null_count++;
                    } else {
                        values.push_back(static_cast<double>(data[i]));
                    }
                }
                break;
            }
            
            case PARQUET_FLOAT: {
                const float* data = (const float*)column_data;
                size_t count = column_size / sizeof(float);
                
                for (size_t i = 0; i < count; i++) {
                    if (std::isnan(data[i])) {
                        // NaN represents null
                        null_count++;
                    } else {
                        values.push_back(static_cast<double>(data[i]));
                    }
                }
                break;
            }
            
            case PARQUET_DOUBLE: {
                const double* data = (const double*)column_data;
                size_t count = column_size / sizeof(double);
                
                for (size_t i = 0; i < count; i++) {
                    if (std::isnan(data[i])) {
                        // NaN represents null
                        null_count++;
                    } else {
                        values.push_back(data[i]);
                    }
                }
                break;
            }
            
            default:
                setError("Unsupported column type for numeric metadata");
                return false;
        }
        
        // Set null count
        num_metadata->null_count = null_count;
        
        // Set total count
        num_metadata->total_count = values.size() + null_count;
        
        // If no values, return with defaults
        if (values.empty()) {
            return true;
        }
        
        // Calculate min and max
        num_metadata->min_value = *std::min_element(values.begin(), values.end());
        num_metadata->max_value = *std::max_element(values.begin(), values.end());
        
        // Calculate average
        double sum = std::accumulate(values.begin(), values.end(), 0.0);
        num_metadata->avg_value = sum / values.size();
        
        // Calculate mode
        std::unordered_map<double, uint64_t> value_counts;
        for (double value : values) {
            value_counts[value]++;
        }
        
        double mode_value = 0.0;
        uint64_t mode_count = 0;
        
        for (const auto& entry : value_counts) {
            if (entry.second > mode_count) {
                mode_value = entry.first;
                mode_count = entry.second;
            }
        }
        
        num_metadata->mode_value = mode_value;
        num_metadata->mode_count = mode_count;
        
        return true;
    }

    /**
     * generateCategoricalMetadata method implementation
     * 
     * This function generates categorical metadata for a column.
     */
    bool generateCategoricalMetadata(const ParquetColumn* column, 
                                    const void* column_data, 
                                    size_t column_size,
                                    ColumnMetadata* metadata) {
        if (!column || !column_data || column_size == 0 || !metadata) {
            setError("Invalid parameters for categorical metadata generation");
            return false;
        }
        
        // Create a new metadata item for categorical
        MetadataItem* item = addColumnMetadataItem(metadata, "categorical", METADATA_TYPE_CATEGORICAL);
        if (!item) {
            setError("Failed to add categorical metadata item");
            return false;
        }
        
        // Initialize categorical metadata with defaults
        CategoricalMetadata* cat_metadata = &item->value.categorical;
        memset(cat_metadata, 0, sizeof(CategoricalMetadata));
        
        // Map to store category frequencies
        std::unordered_map<std::string, uint32_t> category_counts;
        
        // Process data based on column type
        switch (column->type) {
            case PARQUET_BOOLEAN: {
                const bool* data = (const bool*)column_data;
                size_t count = column_size / sizeof(bool);
                
                for (size_t i = 0; i < count; i++) {
                    std::string category = data[i] ? "true" : "false";
                    category_counts[category]++;
                }
                break;
            }
            
            case PARQUET_INT96: {
                // Int96 is typically used for timestamps in older Parquet files
                // We'll treat each unique value as a category
                const uint8_t* data = (const uint8_t*)column_data;
                size_t count = column_size / 12; // Int96 is 12 bytes
                
                for (size_t i = 0; i < count; i++) {
                    // Convert Int96 to hex string for categorization
                    std::stringstream ss;
                    ss << std::hex;
                    for (size_t j = 0; j < 12; j++) {
                        ss << std::setw(2) << std::setfill('0') 
                           << static_cast<int>(data[i * 12 + j]);
                    }
                    category_counts[ss.str()]++;
                }
                break;
            }
            
            case PARQUET_FIXED_LEN_BYTE_ARRAY: {
                // For fixed-length byte arrays, we need to know the length
                // For simplicity, let's assume each element is 16 bytes (e.g., UUID)
                const int fixed_len = 16;
                const uint8_t* data = (const uint8_t*)column_data;
                size_t count = column_size / fixed_len;
                
                for (size_t i = 0; i < count; i++) {
                    // Convert to hex string for categorization
                    std::stringstream ss;
                    ss << std::hex;
                    for (size_t j = 0; j < fixed_len; j++) {
                        ss << std::setw(2) << std::setfill('0') 
                           << static_cast<int>(data[i * fixed_len + j]);
                    }
                    category_counts[ss.str()]++;
                }
                break;
            }
            
            default:
                setError("Unsupported column type for categorical metadata");
                return false;
        }
        
        // Set total category count
        cat_metadata->total_category_count = category_counts.size();
        
        // Set total value count
        uint64_t total_values = 0;
        for (const auto& entry : category_counts) {
            total_values += entry.second;
        }
        cat_metadata->total_value_count = total_values;
        
        // Sort categories by frequency
        std::vector<std::pair<std::string, uint32_t>> sorted_categories;
        for (const auto& entry : category_counts) {
            sorted_categories.push_back(entry);
        }
        
        // Sort by frequency (descending)
        std::sort(sorted_categories.begin(), sorted_categories.end(),
                 [](const std::pair<std::string, uint32_t>& a, 
                    const std::pair<std::string, uint32_t>& b) {
                     return a.second > b.second;
                 });
        
        // Store top N categories
        cat_metadata->high_freq_category_count = std::min((uint32_t)sorted_categories.size(), 
                                                        (uint32_t)MAX_HIGH_FREQ_CATEGORIES);
        
        for (uint32_t i = 0; i < cat_metadata->high_freq_category_count; i++) {
            strncpy(cat_metadata->categories[i], 
                   sorted_categories[i].first.c_str(), 
                   MAX_STRING_LENGTH - 1);
            cat_metadata->categories[i][MAX_STRING_LENGTH - 1] = '\0';
            cat_metadata->category_counts[i] = sorted_categories[i].second;
        }
        
        return true;
    }

    /**
     * addColumnMetadataItem helper method
     * 
     * This function adds a metadata item to a column's metadata list.
     */
    MetadataItem* addColumnMetadataItem(ColumnMetadata* column_metadata, 
                                       const char* name, 
                                       MetadataType type) {
        if (!column_metadata || !name) {
            return nullptr;
        }
        
        // Allocate or reallocate array of metadata items
        uint32_t new_count = column_metadata->metadata_count + 1;
        MetadataItem* new_array = (MetadataItem*)realloc(
            column_metadata->metadata, 
            new_count * sizeof(MetadataItem)
        );
        
        if (!new_array) {
            return nullptr;
        }
        
        column_metadata->metadata = new_array;
        MetadataItem* item = &column_metadata->metadata[column_metadata->metadata_count];
        column_metadata->metadata_count = new_count;
        
        // Initialize the new metadata item
        memset(item, 0, sizeof(MetadataItem));
        strncpy(item->name, name, MAX_METADATA_ITEM_NAME_LENGTH - 1);
        item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
        item->type = type;
        
        return item;
    }

    // Helper methods for Metadata access
    int getMetadataChildCount(const Metadata* metadata) {
        if (!metadata) return 0;
        return reinterpret_cast<const MetadataFields*>(metadata)->child_count;
    }
    
    const char* getMetadataName(const Metadata* metadata) {
        if (!metadata) return "";
        return reinterpret_cast<const MetadataFields*>(metadata)->name;
    }
    
    int getMetadataType(const Metadata* metadata) {
        if (!metadata) return 0;
        return reinterpret_cast<const MetadataFields*>(metadata)->type;
    }
    
    Metadata* getChildMetadata(const Metadata* metadata, int index) {
        if (!metadata) return nullptr;
        const MetadataFields* fields = reinterpret_cast<const MetadataFields*>(metadata);
        if (index < 0 || index >= fields->child_count) return nullptr;
        return reinterpret_cast<Metadata*>(fields->child_metadata[index]);
    }
};

// Constructor
InfParquet::InfParquet() 
    : pImpl(std::make_unique<Impl>()) {
}

// Destructor
InfParquet::~InfParquet() {
}

// Compress a parquet file - match header signature
bool InfParquet::compressParquetFile(
    const std::string& input_file,
    const std::string& output_dir,
    int compression_level,
    int threads,
    bool use_basic_metadata
) {
    CompressionOptions options;
    options.compression_level = compression_level;
    options.parallel_tasks = threads;
    options.generate_base_metadata = use_basic_metadata;
    
    FrameworkError result = pImpl->compressParquetFile(
        input_file, output_dir, options, 
        [this](const std::string& op, int rg, int total, int percent) -> bool {
            // Handle progress callback if needed
            return true;
        }
    );
    
    return result == FrameworkError::OK;
}

// Decompress a previously compressed parquet file - match header signature
bool InfParquet::decompressParquetFile(
    const std::string& input_dir,
    const std::string& output_file,
    int threads
) {
    DecompressionOptions options;
    options.output_directory = output_file;
    options.parallel_tasks = threads;
    
    FrameworkError result = pImpl->decompressParquetFile(
        input_dir, output_file, options, 
        [this](const std::string& op, int rg, int total, int percent) -> bool {
            // Handle progress callback if needed
            return true;
        }
    );
    
    return result == FrameworkError::OK;
}

// Query metadata for specific patterns or values - match header signature
std::string InfParquet::queryMetadata(
    const std::string& input_dir,
    const std::string& query
) {
    MetadataQueryResult results;
    
    FrameworkError result = pImpl->queryMetadata(input_dir, query, &results);
    
    if (result != FrameworkError::OK) {
        return "Query failed: " + pImpl->last_error;
    }
    
    // Convert results to string
    std::stringstream ss;
    ss << "Query results for: " << query << "\n";
    
    if (results.matching_files.empty() && 
        results.matching_row_groups.empty() && 
        results.matching_columns.empty()) {
        ss << "No matches found.";
    } else {
        if (!results.matching_files.empty()) {
            ss << "Matching files (" << results.matching_files.size() << "):\n";
            for (const auto& file : results.matching_files) {
                ss << "  - " << file << "\n";
            }
        }
        
        if (!results.matching_row_groups.empty()) {
            ss << "Matching row groups (" << results.matching_row_groups.size() << "):\n";
            for (const auto& rg : results.matching_row_groups) {
                ss << "  - " << rg << "\n";
            }
        }
        
        if (!results.matching_columns.empty()) {
            ss << "Matching columns (" << results.matching_columns.size() << "):\n";
            for (const auto& col : results.matching_columns) {
                ss << "  - " << col << "\n";
            }
        }
    }
    
    return ss.str();
}

// Rename listMetadataFiles to match header file
std::string InfParquet::listMetadata(const std::string& input_dir) {
    std::vector<std::string> file_list;
    
    FrameworkError result = pImpl->listMetadataFiles(input_dir, &file_list);
    
    if (result != FrameworkError::OK) {
        return "Listing failed: " + pImpl->last_error;
    }
    
    // Convert results to string
    std::stringstream ss;
    ss << "Metadata files in " << input_dir << ":\n";
    
    if (file_list.empty()) {
        ss << "No metadata files found.";
    } else {
        for (const auto& file : file_list) {
            ss << "  - " << file << "\n";
        }
    }
    
    return ss.str();
}

// Get the last error message
std::string InfParquet::getLastError() const {
    return pImpl->last_error;
}

void InfParquet::setProgressCallback(ProgressCallback callback) {
    pImpl->progress_callback = callback;
}

bool InfParquet::loadCustomMetadataFromJson(const std::string& json_file) {
    if (json_file.empty()) {
        pImpl->setError("JSON file path is empty");
        return false;
    }

    // Check if file exists
    if (!fs::exists(json_file)) {
        pImpl->setError("JSON file not found: " + json_file);
        return false;
    }

    // Read the file and parse custom metadata
    std::vector<std::string> names;
    std::vector<std::string> queries;
    
    JsonHelperError err = json_parse_custom_metadata_config(json_file.c_str(), names, queries);
    if (err != JSON_HELPER_OK) {
        pImpl->setError("Failed to parse custom metadata config: " + std::string(json_helper_get_last_error()));
        return false;
    }
    
    // Add custom metadata items
    for (size_t i = 0; i < names.size(); i++) {
        if (!addCustomMetadata(names[i], queries[i])) {
            // Error message already set by addCustomMetadata
            return false;
        }
    }
    
    return true;
}

/**
 * Add a custom metadata item based on an SQL query
 * 
 * name: Name of the custom metadata item
 * sql_query: SQL query to generate the metadata
 * 
 * Return: true on success, false on failure
 */
bool InfParquet::addCustomMetadata(const std::string& name, const std::string& sql_query) {
    if (name.empty() || sql_query.empty()) {
        pImpl->setError("Custom metadata name and SQL query cannot be empty");
        return false;
    }
    
    // Here we would add the custom metadata to the framework
    // This is a simplified implementation
    if (pImpl->verbose) {
        std::cout << "Adding custom metadata: " << name << " with query: " << sql_query << std::endl;
    }
    
    // For now just return success
    return true;
}

void InfParquet::setVerbose(bool verbose) {
    pImpl->verbose = verbose;
}

} // namespace infparquet 