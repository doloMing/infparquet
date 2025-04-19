/**
 * metadata_generator.c
 * 
 * This file implements functionality for generating metadata from Parquet files
 * based on column content including the four basic types of metadata:
 * timestamp, string, numeric, and categorical metadata.
 */

#include "metadata/metadata_generator.h"
#include "metadata/metadata_types.h"
#include "metadata/custom_metadata.h"
#include "metadata/json_serialization.h"
#include "core/parquet_structure.h"
#include "core/parquet_reader.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <math.h>

/* External functions implemented in metadata_parser.cpp */
extern char* metadataToJson(const Metadata* metadata);
extern Metadata* metadataFromJson(const char* json_str);

/* Constants */
#define MAX_METADATA_STRING_LENGTH 256
#define MAX_SPECIAL_STRING_ARRAY 64

/* Static global for error messages */
static char s_error_message[256];

/* Metadata types for internal use */
#define METADATA_TYPE_FILE 1
#define METADATA_TYPE_ROW_GROUP 2
#define METADATA_TYPE_COLUMN 3

/* Parquet types for compatibility */
#define PARQUET_TYPE_BOOLEAN      PARQUET_BOOLEAN
#define PARQUET_TYPE_INT32        PARQUET_INT32
#define PARQUET_TYPE_INT64        PARQUET_INT64
#define PARQUET_TYPE_FLOAT        PARQUET_FLOAT
#define PARQUET_TYPE_DOUBLE       PARQUET_DOUBLE
#define PARQUET_TYPE_BYTE_ARRAY   PARQUET_BYTE_ARRAY
#define PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY PARQUET_FIXED_LEN_BYTE_ARRAY
#define PARQUET_TYPE_INT96        PARQUET_INT96

/* 
 * Using BaseMetadata and ExtendedMetadata structures from metadata_types.h 
 * to avoid duplicate definitions
 */

/**
 * Initialize default metadata generator options
 * 
 * This function sets up default options for metadata generation.
 * 
 * options: Pointer to options structure to initialize
 */
void metadata_generator_init_options(MetadataGeneratorOptions* options) {
    if (!options) {
        return;
    }
    
    options->generate_base_metadata = 1;  // Generate base metadata by default
    options->generate_custom_metadata = 0;  // Don't generate custom metadata by default
    options->custom_metadata_config_path = NULL;
    options->max_high_freq_strings = MAX_HIGH_FREQ_STRINGS;
    options->max_special_strings = MAX_SPECIAL_STRINGS;
    options->max_high_freq_categories = MAX_HIGH_FREQ_CATEGORIES;
}

/**
 * Process timestamp data and extract timestamp metadata
 * 
 * buffer: Buffer containing timestamp data
 * size: Size of the buffer
 * value_count: Number of values in the buffer
 * metadata: Metadata structure to fill
 */
static void process_timestamp_data(const void* buffer, size_t size, uint64_t value_count, BaseMetadata* metadata) {
    if (!buffer || size == 0 || !metadata || value_count == 0) {
        return;
    }
    
    metadata->timestamp_metadata.has_timestamps = 1;
    
    // For INT96, which is a common format for timestamps in Parquet
    // Note: This is a simplified implementation - real code would need
    // to handle the specific timestamp encoding used in your Parquet files
    if (size >= sizeof(int64_t) * value_count) {
        const int64_t* timestamps = (const int64_t*)buffer;
        
        // Initialize with first value
        int64_t min_ts = timestamps[0];
        int64_t max_ts = timestamps[0];
        
        // Find min and max timestamps
        for (uint64_t i = 1; i < value_count; i++) {
            if (timestamps[i] < min_ts) {
                min_ts = timestamps[i];
            }
            if (timestamps[i] > max_ts) {
                max_ts = timestamps[i];
            }
        }
        
        // Convert to time_t (Unix timestamp in seconds)
        // This depends on your specific timestamp encoding
        metadata->timestamp_metadata.min_timestamp = (time_t)(min_ts / 1000000000); // nanoseconds to seconds
        metadata->timestamp_metadata.max_timestamp = (time_t)(max_ts / 1000000000);
    }
}

/**
 * Process numeric data and extract numeric metadata
 * 
 * buffer: Buffer containing numeric data
 * size: Size of the buffer
 * type: Parquet type of the data
 * value_count: Number of values in the buffer
 * metadata: Metadata structure to fill
 */
static void process_numeric_data(const void* buffer, size_t size, ParquetValueType type, uint64_t value_count, BaseMetadata* metadata) {
    if (!buffer || size == 0 || !metadata || value_count == 0) {
        return;
    }
    
    metadata->numeric_metadata.has_numeric_data = 1;
    
    // Variables for statistical calculations
    double min_val = 0;
    double max_val = 0;
    double sum = 0;
    
    // For tracking mode (most frequent value)
    struct {
        double value;
        int count;
    } mode = {0, 0};
    
    // Different handling based on data type
    switch (type) {
        case PARQUET_TYPE_BOOLEAN: {
            const bool* bool_data = (const bool*)buffer;
            if (value_count > 0) {
                min_val = max_val = bool_data[0] ? 1.0 : 0.0;
                sum = min_val;
                mode.value = min_val;
                mode.count = 1;
                
                int true_count = bool_data[0] ? 1 : 0;
                int false_count = bool_data[0] ? 0 : 1;
                
                for (uint64_t i = 1; i < value_count; i++) {
                    double val = bool_data[i] ? 1.0 : 0.0;
                    if (val < min_val) min_val = val;
                    if (val > max_val) max_val = val;
                    sum += val;
                    
                    if (bool_data[i]) true_count++; else false_count++;
                }
                
                // Determine mode (most frequent value)
                if (true_count > false_count) {
                    mode.value = 1.0;
                    mode.count = true_count;
                } else {
                    mode.value = 0.0;
                    mode.count = false_count;
                }
            }
            break;
        }
        case PARQUET_TYPE_INT32: {
            const int32_t* int_data = (const int32_t*)buffer;
            if (value_count > 0) {
                min_val = max_val = (double)int_data[0];
                sum = min_val;
                
                // Simple frequency tracking for mode
                // In a real implementation, you would use a more efficient algorithm
                // or data structure for large datasets
                int* frequency = (int*)calloc(value_count, sizeof(int));
                if (frequency) {
                    frequency[0] = 1;
                    mode.value = min_val;
                    mode.count = 1;
                    
                    for (uint64_t i = 1; i < value_count; i++) {
                        double val = (double)int_data[i];
                        if (val < min_val) min_val = val;
                        if (val > max_val) max_val = val;
                        sum += val;
                        
                        // Update frequency count
                        bool found = false;
                        for (uint64_t j = 0; j < i; j++) {
                            if (int_data[j] == int_data[i]) {
                                frequency[j]++;
                                if (frequency[j] > mode.count) {
                                    mode.value = (double)int_data[j];
                                    mode.count = frequency[j];
                                }
                                found = true;
                                break;
                            }
                        }
                        
                        if (!found) {
                            frequency[i] = 1;
                        }
                    }
                    
                    free(frequency);
                }
            }
            break;
        }
        case PARQUET_TYPE_INT64: {
            const int64_t* int_data = (const int64_t*)buffer;
            if (value_count > 0) {
                min_val = max_val = (double)int_data[0];
                sum = min_val;
                
                // For mode calculation
                int64_t mode_val = int_data[0];
                int mode_count = 1;
                int current_count = 1;
                
                // Sort-based mode calculation
                // For large datasets, consider using a more efficient algorithm
                int64_t* sorted_data = (int64_t*)malloc(value_count * sizeof(int64_t));
                if (sorted_data) {
                    memcpy(sorted_data, int_data, value_count * sizeof(int64_t));
                    // Simple bubble sort - use a more efficient sort for large datasets
                    for (uint64_t i = 0; i < value_count - 1; i++) {
                        for (uint64_t j = 0; j < value_count - i - 1; j++) {
                            if (sorted_data[j] > sorted_data[j + 1]) {
                                int64_t temp = sorted_data[j];
                                sorted_data[j] = sorted_data[j + 1];
                                sorted_data[j + 1] = temp;
                            }
                        }
                    }
                    
                    // Find mode from sorted array
                    for (uint64_t i = 1; i < value_count; i++) {
                        if (sorted_data[i] == sorted_data[i - 1]) {
                            current_count++;
                        } else {
                            if (current_count > mode_count) {
                                mode_count = current_count;
                                mode_val = sorted_data[i - 1];
                            }
                            current_count = 1;
                        }
                    }
                    
                    // Check the last run
                    if (current_count > mode_count) {
                        mode_val = sorted_data[value_count - 1];
                        mode_count = current_count;
                    }
                    
                    free(sorted_data);
                    
                    mode.value = (double)mode_val;
                    mode.count = mode_count;
                }
                
                // Find min, max, sum
                for (uint64_t i = 1; i < value_count; i++) {
                    double val = (double)int_data[i];
                    if (val < min_val) min_val = val;
                    if (val > max_val) max_val = val;
                    sum += val;
                }
            }
            break;
        }
        case PARQUET_TYPE_FLOAT: {
            const float* float_data = (const float*)buffer;
            if (value_count > 0) {
                min_val = max_val = (double)float_data[0];
                sum = min_val;
                
                // Simple approximation for mode - most accurate approach would
                // depend on the specific data distribution
                double bucket_size = 0.01;  // Adjust based on data range
                int max_buckets = 1000;
                int* buckets = (int*)calloc(max_buckets, sizeof(int));
                
                if (buckets) {
                    for (uint64_t i = 0; i < value_count; i++) {
                        double val = (double)float_data[i];
                        if (val < min_val) min_val = val;
                        if (val > max_val) max_val = val;
                        sum += val;
                        
                        // Place in a bucket for mode approximation
                        int bucket = (int)((val - min_val) / bucket_size);
                        if (bucket >= 0 && bucket < max_buckets) {
                            buckets[bucket]++;
                        }
                    }
                    
                    // Find most frequent bucket
                    int max_bucket = 0;
                    for (int i = 1; i < max_buckets; i++) {
                        if (buckets[i] > buckets[max_bucket]) {
                            max_bucket = i;
                        }
                    }
                    
                    mode.value = min_val + (max_bucket * bucket_size);
                    mode.count = buckets[max_bucket];
                    
                    free(buckets);
                } else {
                    // Fallback if memory allocation fails
                    for (uint64_t i = 1; i < value_count; i++) {
                        double val = (double)float_data[i];
                        if (val < min_val) min_val = val;
                        if (val > max_val) max_val = val;
                        sum += val;
                    }
                }
            }
            break;
        }
        case PARQUET_TYPE_DOUBLE: {
            const double* double_data = (const double*)buffer;
            if (value_count > 0) {
                min_val = max_val = double_data[0];
                sum = min_val;
                
                // For large datasets, consider a more efficient algorithm
                int* frequency = (int*)calloc(value_count, sizeof(int));
                if (frequency) {
                    frequency[0] = 1;
                    mode.value = min_val;
                    mode.count = 1;
                    
                    for (uint64_t i = 1; i < value_count; i++) {
                        double val = double_data[i];
                        if (val < min_val) min_val = val;
                        if (val > max_val) max_val = val;
                        sum += val;
                        
                        // Simple frequency counting
                        bool found = false;
                        for (uint64_t j = 0; j < i; j++) {
                            if (double_data[j] == double_data[i]) {
                                frequency[j]++;
                                if (frequency[j] > mode.count) {
                                    mode.value = double_data[j];
                                    mode.count = frequency[j];
                                }
                                found = true;
                                break;
                            }
                        }
                        
                        if (!found) {
                            frequency[i] = 1;
                        }
                    }
                    
                    free(frequency);
                } else {
                    // Fallback if memory allocation fails
                    for (uint64_t i = 1; i < value_count; i++) {
                        double val = double_data[i];
                        if (val < min_val) min_val = val;
                        if (val > max_val) max_val = val;
                        sum += val;
                    }
                }
            }
            break;
        }
        default:
            break;
    }
    
    // Set the metadata values
    metadata->numeric_metadata.min_value = min_val;
    metadata->numeric_metadata.max_value = max_val;
    metadata->numeric_metadata.mean_value = (value_count > 0) ? (sum / value_count) : 0.0;
    metadata->numeric_metadata.mode_value = mode.value;
}

/**
 * Process string data and extract string metadata
 * 
 * buffer: Buffer containing string data
 * size: Size of the buffer
 * value_count: Number of values in the buffer
 * metadata: Metadata structure to fill
 */
static void process_string_data(const void* buffer, size_t size, uint64_t value_count, BaseMetadata* metadata) {
    if (!buffer || size == 0 || !metadata || value_count == 0) {
        return;
    }
    
    // Note: This is a simplified implementation - in reality, string data in Parquet
    // could be stored in various formats depending on the encoding

    // For simplicity, we'll assume the buffer contains a series of null-terminated strings
    // In a real implementation, you would parse the actual Parquet data format
    
    // Define special strings to look for
    const char* special_strings[] = {
        "error", "warning", "exception", "fail", "critical",
        "bug", "crash", "fatal", "issue", "problem"
    };
    const int num_special_strings = sizeof(special_strings) / sizeof(special_strings[0]);
    
    // For tracking string frequencies
    typedef struct {
        char string[MAX_STRING_LENGTH];
        int count;
    } StringFreq;
    
    StringFreq* string_freqs = (StringFreq*)malloc(MAX_HIGH_FREQ_STRINGS * sizeof(StringFreq));
    if (!string_freqs) {
        return;
    }
    
    memset(string_freqs, 0, MAX_HIGH_FREQ_STRINGS * sizeof(StringFreq));
    
    // For tracking special string occurrences
    int* special_counts = (int*)calloc(num_special_strings, sizeof(int));
    if (!special_counts) {
        free(string_freqs);
        return;
    }
    
    // Process the string data
    const char* str_data = (const char*)buffer;
    size_t pos = 0;
    int str_count = 0;
    uint64_t total_length = 0;
    
    // Process up to value_count strings or until end of buffer
    while (pos < size && str_count < value_count) {
        // Assume each string is null-terminated
        const char* current_str = &str_data[pos];
        size_t str_len = strlen(current_str);
        
        if (str_len > 0) {
            // Update string length statistics
            total_length += str_len;
            
            // Check for special strings
            for (int i = 0; i < num_special_strings; i++) {
                if (strstr(current_str, special_strings[i]) != NULL) {
                    special_counts[i]++;
                }
            }
            
            // Update frequency count
            bool found = false;
            for (int i = 0; i < MAX_HIGH_FREQ_STRINGS && string_freqs[i].count > 0; i++) {
                if (strcmp(string_freqs[i].string, current_str) == 0) {
                    string_freqs[i].count++;
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                // Find the least frequent string to replace
                int min_idx = 0;
                for (int i = 1; i < MAX_HIGH_FREQ_STRINGS; i++) {
                    if (string_freqs[i].count < string_freqs[min_idx].count) {
                        min_idx = i;
                    }
                }
                
                // Replace if this string has been seen more times than the least frequent
                if (string_freqs[min_idx].count < 1) {
                    strncpy(string_freqs[min_idx].string, current_str, MAX_STRING_LENGTH - 1);
                    string_freqs[min_idx].string[MAX_STRING_LENGTH - 1] = '\0';
                    string_freqs[min_idx].count = 1;
                }
            }
        }
        
        // Move to next string
        pos += str_len + 1;
        str_count++;
    }
    
    // Sort string frequencies (bubble sort for simplicity)
    for (int i = 0; i < MAX_HIGH_FREQ_STRINGS - 1; i++) {
        for (int j = 0; j < MAX_HIGH_FREQ_STRINGS - i - 1; j++) {
            if (string_freqs[j].count < string_freqs[j + 1].count) {
                StringFreq temp = string_freqs[j];
                string_freqs[j] = string_freqs[j + 1];
                string_freqs[j + 1] = temp;
            }
        }
    }
    
    // Count non-empty frequencies
    int freq_count = 0;
    for (int i = 0; i < MAX_HIGH_FREQ_STRINGS; i++) {
        if (string_freqs[i].count > 0) {
            freq_count++;
        } else {
            break;
        }
    }
    
    // Store in metadata
    metadata->string_metadata.count = freq_count;
    for (int i = 0; i < freq_count; i++) {
        strncpy(metadata->string_metadata.high_frequency_strings[i], string_freqs[i].string, MAX_STRING_LENGTH - 1);
        metadata->string_metadata.high_frequency_strings[i][MAX_STRING_LENGTH - 1] = '\0';
        metadata->string_metadata.frequencies[i] = string_freqs[i].count;
    }
    
    // Sort special string counts - using a constant size array
    int special_indices[MAX_SPECIAL_STRING_ARRAY];
    for (int i = 0; i < num_special_strings && i < MAX_SPECIAL_STRING_ARRAY; i++) {
        special_indices[i] = i;
    }
    
    for (int i = 0; i < num_special_strings - 1 && i < MAX_SPECIAL_STRING_ARRAY - 1; i++) {
        for (int j = 0; j < num_special_strings - i - 1 && j < MAX_SPECIAL_STRING_ARRAY - i - 1; j++) {
            if (special_counts[special_indices[j]] < special_counts[special_indices[j + 1]]) {
                int temp = special_indices[j];
                special_indices[j] = special_indices[j + 1];
                special_indices[j + 1] = temp;
            }
        }
    }
    
    // Store special strings in metadata
    int special_count = 0;
    for (int i = 0; i < num_special_strings && special_count < MAX_SPECIAL_STRINGS && i < MAX_SPECIAL_STRING_ARRAY; i++) {
        int idx = special_indices[i];
        if (special_counts[idx] > 0) {
            strncpy(metadata->string_metadata.special_strings[special_count], special_strings[idx], MAX_STRING_LENGTH - 1);
            metadata->string_metadata.special_strings[special_count][MAX_STRING_LENGTH - 1] = '\0';
            special_count++;
        }
    }
    
    metadata->string_metadata.special_string_count = special_count;
    
    // Clean up
    free(string_freqs);
    free(special_counts);
}

/**
 * Generate base metadata for a column
 * 
 * This function analyzes column data and generates basic metadata.
 * 
 * reader_context: Context for reading the parquet file
 * file: Parquet file structure
 * row_group_id: ID of the row group containing the column
 * column_id: ID of the column to analyze
 * base_metadata: Pointer to store the generated base metadata
 * returns: Error code (METADATA_GEN_OK on success)
 */
static MetadataGeneratorError generate_column_base_metadata(
    ParquetReaderContext* reader_context,
    const ParquetFile* file,
    int row_group_id,
    int column_id,
    BaseMetadata* base_metadata
) {
    if (!reader_context || !file || !base_metadata) {
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    // Get the row group and column
    if (row_group_id < 0 || row_group_id >= file->row_group_count) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid row group ID: %d", row_group_id);
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    const ParquetRowGroup* row_group = &file->row_groups[row_group_id];
    
    if (column_id < 0 || column_id >= row_group->column_count) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid column ID: %d", column_id);
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    const ParquetColumn* column = &row_group->columns[column_id];
    
    // Read the column data using parquet reader
    void* buffer = NULL;
    size_t buffer_size = 0;
    
    ParquetReaderError read_error = parquet_reader_read_column(
        reader_context, row_group_id, column_id, &buffer, &buffer_size);
    
    if (read_error != PARQUET_READER_OK) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to read column data: %s", 
                parquet_reader_get_error(reader_context));
        return METADATA_GEN_PARQUET_ERROR;
    }
    
    // Clear the base metadata structure
    memset(base_metadata, 0, sizeof(BaseMetadata));
    
    // Process data based on column type
    switch (column->type) {
        case PARQUET_TYPE_INT96:  // Timestamp
            process_timestamp_data(buffer, buffer_size, column->total_values, base_metadata);
            break;
            
        case PARQUET_TYPE_BOOLEAN:
        case PARQUET_TYPE_INT32:
        case PARQUET_TYPE_INT64:
        case PARQUET_TYPE_FLOAT:
        case PARQUET_TYPE_DOUBLE:
            process_numeric_data(buffer, buffer_size, column->type, column->total_values, base_metadata);
            break;
            
        case PARQUET_TYPE_BYTE_ARRAY:
        case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY:
            process_string_data(buffer, buffer_size, column->total_values, base_metadata);
            break;
            
        default:
            // For unknown types, don't set any metadata
            break;
    }
    
    // Free the buffer
    parquet_reader_free_buffer(buffer);
    
    return METADATA_GEN_OK;
}

/**
 * Generate metadata for a column
 * 
 * This function creates a metadata structure for a column.
 * 
 * reader_context: Context for reading the parquet file
 * file: Parquet file structure
 * row_group_id: ID of the row group containing the column
 * column_id: ID of the column to analyze
 * options: Options controlling metadata generation
 * column_metadata: Pointer to store the generated column metadata
 * returns: Error code (METADATA_GEN_OK on success)
 */
static MetadataGeneratorError generate_column_metadata(
    ParquetReaderContext* reader_context,
    const ParquetFile* file,
    int row_group_id,
    int column_id,
    const MetadataGeneratorOptions* options,
    ColumnMetadata** column_metadata
) {
    if (!reader_context || !file || !options || !column_metadata) {
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    // Get the row group and column
    if (row_group_id < 0 || row_group_id >= file->row_group_count) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid row group ID: %d", row_group_id);
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    const ParquetRowGroup* row_group = &file->row_groups[row_group_id];
    
    if (column_id < 0 || column_id >= row_group->column_count) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid column ID: %d", column_id);
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    const ParquetColumn* column = &row_group->columns[column_id];
    
    // Allocate memory for the column metadata
    ColumnMetadata* metadata = (ColumnMetadata*)malloc(sizeof(ColumnMetadata));
    if (!metadata) {
        snprintf(s_error_message, sizeof(s_error_message), "Failed to allocate memory for column metadata");
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Clear the metadata structure
    memset(metadata, 0, sizeof(ColumnMetadata));
    
    // Set basic metadata properties
    metadata->column_index = column_id;
    strncpy(metadata->column_name, column->name, MAX_METADATA_ITEM_NAME_LENGTH - 1);
    metadata->column_name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';  // Ensure null-termination
    
    // Allocate base_metadata memory
    metadata->base_metadata = (BaseMetadata*)malloc(sizeof(BaseMetadata));
    if (!metadata->base_metadata) {
        free(metadata);
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Initialize base_metadata
    memset(metadata->base_metadata, 0, sizeof(BaseMetadata));
    
    // Generate base metadata if requested
    if (options->generate_base_metadata) {
        // Generate the base metadata
        MetadataGeneratorError error = generate_column_base_metadata(
            reader_context, file, row_group_id, column_id, metadata->base_metadata
        );
        
        if (error != METADATA_GEN_OK) {
            free(metadata);
            return error;
        }
    }
    
    // Return the column metadata
    *column_metadata = metadata;
    return METADATA_GEN_OK;
}

/**
 * Generate metadata for a row group
 * 
 * This function analyzes row group data and generates metadata.
 * 
 * reader_context: Context for reading the parquet file
 * file: Parquet file structure
 * row_group_id: ID of the row group to analyze
 * options: Generator options
 * out_metadata: Pointer to store the generated metadata
 * returns: Error code (METADATA_GEN_OK on success)
 */
static MetadataGeneratorError generate_row_group_metadata(
    ParquetReaderContext* reader_context,
    const ParquetFile* file,
    int row_group_id,
    const MetadataGeneratorOptions* options,
    RowGroupMetadata** out_metadata
) {
    if (!reader_context || !file || !options || !out_metadata) {
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    // Get the row group
    if (row_group_id < 0 || row_group_id >= file->row_group_count) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid row group ID: %d", row_group_id);
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    const ParquetRowGroup* row_group = &file->row_groups[row_group_id];
    
    // Allocate memory for the row group metadata
    RowGroupMetadata* metadata = (RowGroupMetadata*)malloc(sizeof(RowGroupMetadata));
    if (!metadata) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for row group metadata");
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Initialize the row group metadata
    metadata->row_group_index = row_group_id;
    
    // Allocate base_metadata memory
    metadata->base_metadata = (BaseMetadata*)malloc(sizeof(BaseMetadata));
    if (!metadata->base_metadata) {
        free(metadata);
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Initialize base_metadata
    memset(metadata->base_metadata, 0, sizeof(BaseMetadata));
    
    // Set column count
    metadata->column_count = row_group->column_count;
    
    // Allocate memory for column metadata
    metadata->columns = NULL;
    if (metadata->column_count > 0) {
        metadata->columns = (ColumnMetadata**)malloc(metadata->column_count * sizeof(ColumnMetadata*));
        if (!metadata->columns) {
            snprintf(s_error_message, sizeof(s_error_message),
                    "Failed to allocate memory for column metadata array");
            free(metadata);
            return METADATA_GEN_MEMORY_ERROR;
        }
        memset(metadata->columns, 0, metadata->column_count * sizeof(ColumnMetadata*));
    }
    
    // Aggregate timestamps across columns
    bool has_timestamps = false;
    time_t min_timestamp = 0;
    time_t max_timestamp = 0;
    
    // Aggregate numeric data across columns
    double global_min = 0;
    double global_max = 0;
    double sum_of_means = 0;
    int numeric_columns = 0;
    
    // Aggregate string data across columns
    typedef struct {
        char string[MAX_STRING_LENGTH];
        int count;
    } GlobalStringFreq;
    
    GlobalStringFreq* global_strings = (GlobalStringFreq*)calloc(
        options->max_high_freq_strings * 2, sizeof(GlobalStringFreq));
    
    if (!global_strings && row_group->column_count > 0) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for global string tracking");
        for (int i = 0; i < metadata->column_count; i++) {
            if (metadata->columns[i]) {
                free(metadata->columns[i]);
            }
        }
        free(metadata->columns);
        free(metadata);
        free(global_strings);
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Initialize global string tracking
    int global_string_count = 0;
    
    // Generate metadata for each column
    MetadataGeneratorError error = METADATA_GEN_OK;
    for (int i = 0; i < row_group->column_count; i++) {
        error = generate_column_metadata(reader_context, file, row_group_id, i, options, &metadata->columns[i]);
        if (error != METADATA_GEN_OK) {
            // Clean up
            for (int j = 0; j < i; j++) {
                if (metadata->columns[j]) {
                    free(metadata->columns[j]);
                }
            }
            free(metadata->columns);
            free(metadata);
            free(global_strings);
            return error;
        }
        
        // Aggregate timestamp metadata
        if (metadata->columns[i]->base_metadata->timestamp_metadata.has_timestamps) {
            if (!has_timestamps) {
                // First column with timestamps
                has_timestamps = true;
                min_timestamp = metadata->columns[i]->base_metadata->timestamp_metadata.min_timestamp;
                max_timestamp = metadata->columns[i]->base_metadata->timestamp_metadata.max_timestamp;
            } else {
                // Update min/max timestamps
                if (metadata->columns[i]->base_metadata->timestamp_metadata.min_timestamp < min_timestamp) {
                    min_timestamp = metadata->columns[i]->base_metadata->timestamp_metadata.min_timestamp;
                }
                if (metadata->columns[i]->base_metadata->timestamp_metadata.max_timestamp > max_timestamp) {
                    max_timestamp = metadata->columns[i]->base_metadata->timestamp_metadata.max_timestamp;
                }
            }
        }
        
        // Aggregate numeric metadata
        if (metadata->columns[i]->base_metadata->numeric_metadata.has_numeric_data) {
            double col_min = metadata->columns[i]->base_metadata->numeric_metadata.min_value;
            double col_max = metadata->columns[i]->base_metadata->numeric_metadata.max_value;
            double col_mean = metadata->columns[i]->base_metadata->numeric_metadata.mean_value;
            
            if (numeric_columns == 0) {
                // First numeric column
                global_min = col_min;
                global_max = col_max;
            } else {
                // Update min/max
                if (col_min < global_min) global_min = col_min;
                if (col_max > global_max) global_max = col_max;
            }
            
            sum_of_means += col_mean;
            numeric_columns++;
        }
        
        // Aggregate string metadata
        for (int j = 0; j < metadata->columns[i]->base_metadata->string_metadata.count; j++) {
            const char* str = metadata->columns[i]->base_metadata->string_metadata.high_frequency_strings[j];
            int freq = metadata->columns[i]->base_metadata->string_metadata.frequencies[j];
            
            // Check if this string is already in our global list
            bool found = false;
            for (int k = 0; k < global_string_count; k++) {
                if (strcmp(global_strings[k].string, str) == 0) {
                    global_strings[k].count += freq;
                    found = true;
                    break;
                }
            }
            
            // Add to global list if not found and we have space
            if (!found && global_string_count < options->max_high_freq_strings * 2) {
                strncpy(global_strings[global_string_count].string, str, MAX_STRING_LENGTH - 1);
                global_strings[global_string_count].string[MAX_STRING_LENGTH - 1] = '\0';
                global_strings[global_string_count].count = freq;
                global_string_count++;
            }
        }
    }
    
    // Set aggregated timestamp metadata
    metadata->base_metadata->timestamp_metadata.has_timestamps = has_timestamps;
    if (has_timestamps) {
        metadata->base_metadata->timestamp_metadata.min_timestamp = min_timestamp;
        metadata->base_metadata->timestamp_metadata.max_timestamp = max_timestamp;
    }
    
    // Set aggregated numeric metadata
    metadata->base_metadata->numeric_metadata.has_numeric_data = (numeric_columns > 0);
    if (numeric_columns > 0) {
        metadata->base_metadata->numeric_metadata.min_value = global_min;
        metadata->base_metadata->numeric_metadata.max_value = global_max;
        metadata->base_metadata->numeric_metadata.mean_value = sum_of_means / numeric_columns;
        // We don't aggregate mode as it doesn't make sense to average
        metadata->base_metadata->numeric_metadata.mode_value = 0;
    }
    
    // Sort global strings by frequency
    for (int i = 0; i < global_string_count - 1; i++) {
        for (int j = 0; j < global_string_count - i - 1; j++) {
            if (global_strings[j].count < global_strings[j + 1].count) {
                GlobalStringFreq temp = global_strings[j];
                global_strings[j] = global_strings[j + 1];
                global_strings[j + 1] = temp;
            }
        }
    }
    
    // Set aggregated string metadata
    int count = (global_string_count < options->max_high_freq_strings) ? 
                global_string_count : options->max_high_freq_strings;
    
    metadata->base_metadata->string_metadata.count = count;
    for (int i = 0; i < count; i++) {
        strncpy(metadata->base_metadata->string_metadata.high_frequency_strings[i], 
                global_strings[i].string, MAX_STRING_LENGTH - 1);
        metadata->base_metadata->string_metadata.high_frequency_strings[i][MAX_STRING_LENGTH - 1] = '\0';
        metadata->base_metadata->string_metadata.frequencies[i] = global_strings[i].count;
    }
    
    // Clean up
    if (global_strings) {
        free(global_strings);
    }
    
    // Set the output parameter
    *out_metadata = metadata;
    
    return METADATA_GEN_OK;
}

/**
 * Generate metadata for a parquet file
 * 
 * This function analyzes a parquet file and generates metadata at all levels
 * (file, row group, and column) according to the specified options.
 * 
 * file: Parquet file structure containing the file organization
 * reader_context: Context for reading the parquet file data
 * options: Options controlling metadata generation
 * file_metadata: Pointer to store the generated file-level metadata
 * returns: Error code (METADATA_GEN_OK on success)
 */
MetadataGeneratorError metadata_generator_generate(
    const ParquetFile* file,
    ParquetReaderContext* reader_context,
    const MetadataGeneratorOptions* options,
    Metadata** file_metadata
) {
    if (!file || !reader_context || !options || !file_metadata) {
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    // Allocate memory for the file metadata
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)malloc(sizeof(struct ExtendedMetadata));
    if (!ext_metadata) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to allocate memory for file metadata");
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Clear the metadata structure
    memset(ext_metadata, 0, sizeof(struct ExtendedMetadata));
    
    // Set basic metadata properties
    ext_metadata->type = METADATA_TYPE_FILE;
    ext_metadata->id = 0;  // File metadata always has ID 0
    
    // Use the file path as the name
    if (file->file_path) {
        strncpy(ext_metadata->name, file->file_path, MAX_METADATA_STRING_LENGTH - 1);
        ext_metadata->name[MAX_METADATA_STRING_LENGTH - 1] = '\0';  // Ensure null-termination
    }
    
    // Initialize base metadata
    if (options->generate_base_metadata) {
        // Allocate memory for base metadata
        ext_metadata->base_metadata = (BaseMetadata*)malloc(sizeof(BaseMetadata));
        if (!ext_metadata->base_metadata) {
            free(ext_metadata);
            snprintf(s_error_message, sizeof(s_error_message), 
                    "Failed to allocate memory for base metadata");
            return METADATA_GEN_MEMORY_ERROR;
        }
        
        // Initialize base metadata
        memset(ext_metadata->base_metadata, 0, sizeof(BaseMetadata));
    }
    
    // Generate row group metadata if requested
    if (options->generate_base_metadata) {
        // Allocate memory for child metadata pointers
        ext_metadata->child_count = file->row_group_count;
        ext_metadata->child_metadata = (struct ExtendedMetadata**)malloc(ext_metadata->child_count * sizeof(struct ExtendedMetadata*));
        if (!ext_metadata->child_metadata) {
            if (ext_metadata->base_metadata) {
                free(ext_metadata->base_metadata);
            }
            free(ext_metadata);
            snprintf(s_error_message, sizeof(s_error_message), 
                    "Failed to allocate memory for row group metadata pointers");
            return METADATA_GEN_MEMORY_ERROR;
        }
        
        // Initialize memory to NULL
        memset(ext_metadata->child_metadata, 0, ext_metadata->child_count * sizeof(struct ExtendedMetadata*));
        
        // Aggregate timestamps across row groups
        bool has_timestamps = false;
        time_t global_min_timestamp = 0;
        time_t global_max_timestamp = 0;
        
        // Aggregate numeric data across row groups
        bool has_numeric_data = false;
        double global_min = 0.0;
        double global_max = 0.0;
        double sum_of_means = 0.0;
        int numeric_row_groups = 0;
        
        // Aggregate string data across row groups
        typedef struct {
            char string[MAX_STRING_LENGTH];
            int count;
        } GlobalStringFreq;
        
        GlobalStringFreq* global_strings = (GlobalStringFreq*)calloc(
            options->max_high_freq_strings * 2, sizeof(GlobalStringFreq));
        
        if (!global_strings && file->row_group_count > 0) {
            if (ext_metadata->base_metadata) {
                free(ext_metadata->base_metadata);
            }
            free(ext_metadata->child_metadata);
            free(ext_metadata);
            snprintf(s_error_message, sizeof(s_error_message),
                    "Failed to allocate memory for global string tracking");
            return METADATA_GEN_MEMORY_ERROR;
        }
        
        // Initialize global string tracking
        int global_string_count = 0;
        
        // Generate metadata for each row group
        for (int i = 0; i < file->row_group_count; i++) {
            RowGroupMetadata* row_group_metadata = NULL;
            MetadataGeneratorError error = generate_row_group_metadata(
                reader_context, file, i, options, &row_group_metadata
            );
            
            if (error != METADATA_GEN_OK) {
                // Free previously generated row group metadata
                for (int j = 0; j < i; j++) {
                    if (ext_metadata->child_metadata[j]) {
                        // Convert ExtendedMetadata to RowGroupMetadata for cleanup
                        RowGroupMetadata* rg_meta = (RowGroupMetadata*)ext_metadata->child_metadata[j];
                        
                        // Free the column metadata
                        for (int k = 0; k < rg_meta->column_count; k++) {
                            if (rg_meta->columns[k]) {
                                free(rg_meta->columns[k]);
                            }
                        }
                        
                        // Free the columns array
                        if (rg_meta->columns) {
                            free(rg_meta->columns);
                        }
                        
                        // Free the row group metadata
                        free(rg_meta);
                    }
                }
                if (ext_metadata->base_metadata) {
                    free(ext_metadata->base_metadata);
                }
                free(ext_metadata->child_metadata);
                if (global_strings) {
                    free(global_strings);
                }
                free(ext_metadata);
                return error;
            }
            
            // Store row group metadata
            ext_metadata->child_metadata[i] = (struct ExtendedMetadata*)row_group_metadata;
            
            // Aggregate timestamp metadata
            if (row_group_metadata->base_metadata->timestamp_metadata.has_timestamps) {
                if (!has_timestamps) {
                    // First row group with timestamps
                    has_timestamps = true;
                    global_min_timestamp = row_group_metadata->base_metadata->timestamp_metadata.min_timestamp;
                    global_max_timestamp = row_group_metadata->base_metadata->timestamp_metadata.max_timestamp;
                } else {
                    // Update min/max timestamps
                    if (row_group_metadata->base_metadata->timestamp_metadata.min_timestamp < global_min_timestamp) {
                        global_min_timestamp = row_group_metadata->base_metadata->timestamp_metadata.min_timestamp;
                    }
                    if (row_group_metadata->base_metadata->timestamp_metadata.max_timestamp > global_max_timestamp) {
                        global_max_timestamp = row_group_metadata->base_metadata->timestamp_metadata.max_timestamp;
                    }
                }
            }
            
            // Aggregate numeric metadata
            if (row_group_metadata->base_metadata->numeric_metadata.has_numeric_data) {
                double rg_min = row_group_metadata->base_metadata->numeric_metadata.min_value;
                double rg_max = row_group_metadata->base_metadata->numeric_metadata.max_value;
                double rg_mean = row_group_metadata->base_metadata->numeric_metadata.mean_value;
                
                if (!has_numeric_data) {
                    // First numeric row group
                    has_numeric_data = true;
                    global_min = rg_min;
                    global_max = rg_max;
                } else {
                    // Update min/max
                    if (rg_min < global_min) global_min = rg_min;
                    if (rg_max > global_max) global_max = rg_max;
                }
                
                sum_of_means += rg_mean;
                numeric_row_groups++;
            }
            
            // Aggregate string metadata
            for (int j = 0; j < row_group_metadata->base_metadata->string_metadata.count; j++) {
                const char* str = row_group_metadata->base_metadata->string_metadata.high_frequency_strings[j];
                int freq = row_group_metadata->base_metadata->string_metadata.frequencies[j];
                
                // Check if this string is already in our global list
                bool found = false;
                for (int k = 0; k < global_string_count; k++) {
                    if (strcmp(global_strings[k].string, str) == 0) {
                        global_strings[k].count += freq;
                        found = true;
                        break;
                    }
                }
                
                // Add to global list if not found and we have space
                if (!found && global_string_count < options->max_high_freq_strings * 2) {
                    strncpy(global_strings[global_string_count].string, str, MAX_STRING_LENGTH - 1);
                    global_strings[global_string_count].string[MAX_STRING_LENGTH - 1] = '\0';
                    global_strings[global_string_count].count = freq;
                    global_string_count++;
                }
            }
        }
        
        // Sort global strings by frequency
        for (int i = 0; i < global_string_count - 1; i++) {
            for (int j = 0; j < global_string_count - i - 1; j++) {
                if (global_strings[j].count < global_strings[j + 1].count) {
                    GlobalStringFreq temp = global_strings[j];
                    global_strings[j] = global_strings[j + 1];
                    global_strings[j + 1] = temp;
                }
            }
        }
        
        // Store aggregated metadata in file metadata
        if (ext_metadata->base_metadata) {
            // Store timestamp metadata
            ext_metadata->base_metadata->timestamp_metadata.has_timestamps = has_timestamps;
            if (has_timestamps) {
                ext_metadata->base_metadata->timestamp_metadata.min_timestamp = global_min_timestamp;
                ext_metadata->base_metadata->timestamp_metadata.max_timestamp = global_max_timestamp;
            }
            
            // Store numeric metadata
            ext_metadata->base_metadata->numeric_metadata.has_numeric_data = has_numeric_data;
            if (has_numeric_data) {
                ext_metadata->base_metadata->numeric_metadata.min_value = global_min;
                ext_metadata->base_metadata->numeric_metadata.max_value = global_max;
                ext_metadata->base_metadata->numeric_metadata.mean_value = 
                    (numeric_row_groups > 0) ? (sum_of_means / numeric_row_groups) : 0.0;
                // Mode is not aggregated as it doesn't make sense to average
                ext_metadata->base_metadata->numeric_metadata.mode_value = 0.0;
            }
            
            // Store string metadata
            int high_freq_count = (global_string_count < options->max_high_freq_strings) ? 
                                  global_string_count : options->max_high_freq_strings;
            
            ext_metadata->base_metadata->string_metadata.count = high_freq_count;
            for (int i = 0; i < high_freq_count; i++) {
                strncpy(ext_metadata->base_metadata->string_metadata.high_frequency_strings[i],
                        global_strings[i].string, MAX_STRING_LENGTH - 1);
                ext_metadata->base_metadata->string_metadata.high_frequency_strings[i][MAX_STRING_LENGTH - 1] = '\0';
                ext_metadata->base_metadata->string_metadata.frequencies[i] = global_strings[i].count;
            }
            
            // Initialize metadata items array if needed
            if (!ext_metadata->base_metadata->items) {
                ext_metadata->base_metadata->items = (MetadataItem**)malloc(sizeof(MetadataItem*) * 3); // For timestamp, numeric, string
                if (ext_metadata->base_metadata->items) {
                    ext_metadata->base_metadata->item_count = 0;
                    memset(ext_metadata->base_metadata->items, 0, sizeof(MetadataItem*) * 3);
                    
                    // Add timestamp metadata item if we have timestamps
                    if (has_timestamps) {
                        MetadataItem* timestamp_item = (MetadataItem*)malloc(sizeof(MetadataItem));
                        if (timestamp_item) {
                            snprintf(timestamp_item->name, MAX_METADATA_ITEM_NAME_LENGTH, "TimestampRange");
                            timestamp_item->type = METADATA_TYPE_TIMESTAMP;
                            timestamp_item->value.timestamp.min_timestamp = global_min_timestamp;
                            timestamp_item->value.timestamp.max_timestamp = global_max_timestamp;
                            timestamp_item->value.timestamp.count = 0; // We don't track this across row groups
                            
                            ext_metadata->base_metadata->items[ext_metadata->base_metadata->item_count++] = timestamp_item;
                        }
                    }
                    
                    // Add numeric metadata item if we have numeric data
                    if (has_numeric_data) {
                        MetadataItem* numeric_item = (MetadataItem*)malloc(sizeof(MetadataItem));
                        if (numeric_item) {
                            snprintf(numeric_item->name, MAX_METADATA_ITEM_NAME_LENGTH, "NumericStats");
                            numeric_item->type = METADATA_TYPE_NUMERIC;
                            numeric_item->value.numeric.min_value = global_min;
                            numeric_item->value.numeric.max_value = global_max;
                            numeric_item->value.numeric.avg_value = 
                                (numeric_row_groups > 0) ? (sum_of_means / numeric_row_groups) : 0.0;
                            numeric_item->value.numeric.mode_value = 0.0; // Not aggregated
                            numeric_item->value.numeric.mode_count = 0;
                            numeric_item->value.numeric.total_count = 0; // We don't track this across row groups
                            numeric_item->value.numeric.null_count = 0;  // We don't track this across row groups
                            
                            ext_metadata->base_metadata->items[ext_metadata->base_metadata->item_count++] = numeric_item;
                        }
                    }
                    
                    // Add string metadata item if we have strings
                    if (high_freq_count > 0) {
                        MetadataItem* string_item = (MetadataItem*)malloc(sizeof(MetadataItem));
                        if (string_item) {
                            snprintf(string_item->name, MAX_METADATA_ITEM_NAME_LENGTH, "StringStats");
                            string_item->type = METADATA_TYPE_STRING;
                            
                            // Copy high frequency strings
                            string_item->value.string.high_freq_count = high_freq_count;
                            for (int i = 0; i < high_freq_count; i++) {
                                strncpy(string_item->value.string.high_frequency_strings[i],
                                        global_strings[i].string, MAX_STRING_LENGTH - 1);
                                string_item->value.string.high_frequency_strings[i][MAX_STRING_LENGTH - 1] = '\0';
                                string_item->value.string.high_freq_counts[i] = global_strings[i].count;
                            }
                            
                            // We don't aggregate special strings across row groups
                            string_item->value.string.special_string_count = 0;
                            string_item->value.string.total_string_count = 0; // We don't track this across row groups
                            string_item->value.string.avg_string_length = 0;  // We don't track this across row groups
                            
                            ext_metadata->base_metadata->items[ext_metadata->base_metadata->item_count++] = string_item;
                        }
                    }
                }
            }
        }
        
        // Free global strings
        if (global_strings) {
            free(global_strings);
        }
    }
    
    // Generate custom metadata if requested
    if (options->generate_custom_metadata && options->custom_metadata_config_path) {
        MetadataGeneratorError error = generate_custom_metadata(
            ext_metadata, file, options->custom_metadata_config_path
        );
        
        if (error != METADATA_GEN_OK) {
            // Free all allocated memory
            if (ext_metadata->base_metadata) {
                // Free metadata items
                if (ext_metadata->base_metadata->items) {
                    for (uint32_t i = 0; i < ext_metadata->base_metadata->item_count; i++) {
                        if (ext_metadata->base_metadata->items[i]) {
                            free(ext_metadata->base_metadata->items[i]);
                        }
                    }
                    free(ext_metadata->base_metadata->items);
                }
                free(ext_metadata->base_metadata);
            }
            
            // Free row group metadata
            for (int i = 0; i < ext_metadata->child_count; i++) {
                if (ext_metadata->child_metadata[i]) {
                    RowGroupMetadata* rg_meta = (RowGroupMetadata*)ext_metadata->child_metadata[i];
                    
                    // Free column metadata
                    for (int j = 0; j < rg_meta->column_count; j++) {
                        if (rg_meta->columns[j]) {
                            free(rg_meta->columns[j]);
                        }
                    }
                    
                    free(rg_meta->columns);
                    free(rg_meta);
                }
            }
            
            free(ext_metadata->child_metadata);
            free(ext_metadata);
            return error;
        }
    }
    
    // Set the output parameter
    *file_metadata = (Metadata*)ext_metadata;
    
    return METADATA_GEN_OK;
}

/**
 * Free metadata and associated resources
 * 
 * This function releases all memory allocated for metadata structures.
 * 
 * metadata: Pointer to metadata structure to free
 */
void metadata_generator_free_metadata(Metadata* metadata) {
    if (!metadata) {
        return;
    }
    
    // Cast to extended metadata
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)metadata;
    
    // Free base metadata
    if (ext_metadata->base_metadata) {
        free(ext_metadata->base_metadata);
    }
    
    // Free custom metadata items
    if (ext_metadata->custom_metadata_count > 0 && ext_metadata->custom_metadata) {
        custom_metadata_free_items(ext_metadata->custom_metadata, ext_metadata->custom_metadata_count);
    }
    
    // Free child metadata
    if (ext_metadata->child_metadata) {
        for (int i = 0; i < ext_metadata->child_count; i++) {
            if (ext_metadata->child_metadata[i]) {
                if (ext_metadata->type == METADATA_TYPE_FILE) {
                    // Row group metadata - we need to free internal structures
                    RowGroupMetadata* rg_meta = (RowGroupMetadata*)ext_metadata->child_metadata[i];
                    
                    // Free column metadata
                    if (rg_meta->columns) {
                        for (int j = 0; j < rg_meta->column_count; j++) {
                            if (rg_meta->columns[j]) {
                                free(rg_meta->columns[j]);
                            }
                        }
                        free(rg_meta->columns);
                    }
                    
                    // Free the row group metadata
                    free(rg_meta);
                } else if (ext_metadata->type == METADATA_TYPE_ROW_GROUP) {
                    // Column metadata
                    free(ext_metadata->child_metadata[i]);
                } else {
                    // Other metadata types
                    free(ext_metadata->child_metadata[i]);
                }
            }
        }
        free(ext_metadata->child_metadata);
    }
    
    // Free the metadata structure
    free(ext_metadata);
}

/**
 * Save metadata to a file
 * 
 * This function serializes metadata to a binary file.
 * The metadata is stored in a compact binary format that can be
 * efficiently loaded using metadata_generator_load_metadata.
 * 
 * metadata: Pointer to metadata structure to save
 * file_path: Path where the metadata will be saved
 * returns: Error code (METADATA_GEN_OK on success)
 */
MetadataGeneratorError metadata_generator_save_metadata(
    const Metadata* metadata,
    const char* file_path
) {
    if (!metadata || !file_path) {
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    // For now, we'll just serialize to a simple binary format
    FILE* file = fopen(file_path, "wb");
    if (!file) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to open output file: %s", file_path);
        return METADATA_GEN_FILE_ERROR;
    }
    
    // Cast to extended metadata
    const struct ExtendedMetadata* ext_metadata = (const struct ExtendedMetadata*)metadata;
    
    // Write header information
    fwrite(&ext_metadata->type, sizeof(int), 1, file);
    fwrite(&ext_metadata->id, sizeof(int), 1, file);
    fwrite(ext_metadata->name, sizeof(char), MAX_METADATA_STRING_LENGTH, file);
    
    // Write child count
    fwrite(&ext_metadata->child_count, sizeof(int), 1, file);
    
    // In a real implementation, we would recursively serialize all child metadata
    // and custom metadata as well
    
    fclose(file);
    return METADATA_GEN_OK;
}

/**
 * Load metadata from a file
 * 
 * This function loads metadata from a binary file created
 * with metadata_generator_save_metadata.
 * 
 * file_path: Path to the metadata file
 * metadata: Pointer to store the loaded metadata
 * returns: Error code (METADATA_GEN_OK on success)
 */
MetadataGeneratorError metadata_generator_load_metadata(
    const char* file_path,
    Metadata** metadata
) {
    if (!file_path || !metadata) {
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    // Open the input file
    FILE* file = fopen(file_path, "rb");
    if (!file) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to open input file: %s", file_path);
        return METADATA_GEN_FILE_ERROR;
    }
    
    // Allocate memory for the metadata
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)malloc(sizeof(struct ExtendedMetadata));
    if (!ext_metadata) {
        fclose(file);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to allocate memory for metadata");
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Clear the metadata structure
    memset(ext_metadata, 0, sizeof(struct ExtendedMetadata));
    
    // Read header information
    if (fread(&ext_metadata->type, sizeof(int), 1, file) != 1 ||
        fread(&ext_metadata->id, sizeof(int), 1, file) != 1 ||
        fread(ext_metadata->name, sizeof(char), MAX_METADATA_STRING_LENGTH, file) != MAX_METADATA_STRING_LENGTH) {
        free(ext_metadata);
        fclose(file);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to read metadata header");
        return METADATA_GEN_FILE_ERROR;
    }
    
    // Read child count
    if (fread(&ext_metadata->child_count, sizeof(int), 1, file) != 1) {
        free(ext_metadata);
        fclose(file);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to read metadata child count");
        return METADATA_GEN_FILE_ERROR;
    }
    
    // In a real implementation, we would recursively deserialize all child metadata
    // and custom metadata as well
    
    fclose(file);
    
    *metadata = (Metadata*)ext_metadata;
    return METADATA_GEN_OK;
}

/**
 * Get the last error message from the metadata generator
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any metadata generator function.
 * 
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* metadata_generator_get_error(void) {
    if (s_error_message[0] == '\0') {
        return NULL;
    }
    
    return s_error_message;
}

/**
 * Creates a new empty metadata structure
 * 
 * This function allocates memory for a new Metadata structure and initializes
 * its fields with default values. The caller is responsible for freeing the memory
 * using releaseMetadata function.
 * 
 * file_path: Path to the original Parquet file
 * 
 * Return: Pointer to a new Metadata structure, or NULL if allocation fails
 */
Metadata* createMetadata(const char* file_path) {
    if (!file_path) {
        return NULL;
    }
    
    // Create an extended metadata structure
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)malloc(sizeof(struct ExtendedMetadata));
    if (!ext_metadata) {
        return NULL;
    }
    
    // Initialize fields with default values
    memset(ext_metadata, 0, sizeof(struct ExtendedMetadata));
    ext_metadata->type = METADATA_TYPE_FILE;
    ext_metadata->id = 0;
    
    // Copy file path to name field
    strncpy(ext_metadata->name, file_path, MAX_METADATA_STRING_LENGTH - 1);
    ext_metadata->name[MAX_METADATA_STRING_LENGTH - 1] = '\0';
    
    return (Metadata*)ext_metadata;
}

/**
 * Releases memory allocated for a metadata structure
 * 
 * This function frees all memory allocated for the Metadata structure, including
 * its file, row group, and column metadata. After calling this function, the pointer
 * should not be used anymore.
 * 
 * metadata: Pointer to a Metadata structure to be freed
 */
void metadata_release(Metadata* metadata) {
    if (!metadata) return;
    
    // Free file path
    if (metadata->file_path) {
        free(metadata->file_path);
        metadata->file_path = NULL;
    }
    
    // Free basic metadata items
    if (metadata->file_metadata.basic_metadata) {
        free(metadata->file_metadata.basic_metadata);
        metadata->file_metadata.basic_metadata = NULL;
    }
    
    // Free custom metadata items
    if (metadata->file_metadata.custom_metadata) {
        for (uint32_t i = 0; i < metadata->file_metadata.custom_metadata_count; i++) {
            CustomMetadataItem* item = &metadata->file_metadata.custom_metadata[i];
            if (item->result_matrix) {
                free(item->result_matrix);
                item->result_matrix = NULL;
            }
        }
        free(metadata->file_metadata.custom_metadata);
        metadata->file_metadata.custom_metadata = NULL;
    }
    
    // Free row group metadata
    if (metadata->row_group_metadata) {
        for (uint32_t i = 0; i < metadata->row_group_metadata_count; i++) {
            RowGroupMetadata* row_group = &metadata->row_group_metadata[i];
            
            // Free metadata items
            if (row_group->metadata) {
                free(row_group->metadata);
                row_group->metadata = NULL;
            }
            
            // Free base metadata
            if (row_group->base_metadata) {
                if (row_group->base_metadata->metadata) {
                    free(row_group->base_metadata->metadata);
                }
                free(row_group->base_metadata);
                row_group->base_metadata = NULL;
            }
            
            // Free column metadata
            if (row_group->columns) {
                for (uint32_t j = 0; j < row_group->column_count; j++) {
                    if (row_group->columns[j]) {
                        // Free column metadata items
                        if (row_group->columns[j]->metadata) {
                            free(row_group->columns[j]->metadata);
                        }
                        
                        // Free column base metadata
                        if (row_group->columns[j]->base_metadata) {
                            if (row_group->columns[j]->base_metadata->metadata) {
                                free(row_group->columns[j]->base_metadata->metadata);
                            }
                            free(row_group->columns[j]->base_metadata);
                        }
                        
                        free(row_group->columns[j]);
                    }
                }
                free(row_group->columns);
                row_group->columns = NULL;
            }
        }
        free(metadata->row_group_metadata);
        metadata->row_group_metadata = NULL;
    }
    
    // Free column metadata
    if (metadata->column_metadata) {
        for (uint32_t i = 0; i < metadata->column_metadata_count; i++) {
            ColumnMetadata* column = &metadata->column_metadata[i];
            
            // Free metadata items
            if (column->metadata) {
                free(column->metadata);
                column->metadata = NULL;
            }
            
            // Free base metadata
            if (column->base_metadata) {
                if (column->base_metadata->metadata) {
                    free(column->base_metadata->metadata);
                }
                free(column->base_metadata);
                column->base_metadata = NULL;
            }
        }
        free(metadata->column_metadata);
        metadata->column_metadata = NULL;
    }
    
    // Free the metadata structure itself
    free(metadata);
}

/**
 * Alias for metadata_release for backward compatibility
 */
void releaseMetadata(Metadata* metadata) {
    metadata_release(metadata);
}

/**
 * Adds a basic metadata item to the file metadata
 * 
 * This function creates a new basic metadata item and adds it to the file metadata.
 * 
 * metadata: Pointer to the Metadata structure
 * name: Name of the metadata item
 * type: Type of the metadata
 * 
 * Return: Pointer to the newly created metadata item, or NULL if addition fails
 */
MetadataItem* addBasicFileMetadataItem(Metadata* metadata, const char* name, MetadataType type) {
    if (!metadata || !name) {
        return NULL;
    }
    
    // Cast to extended metadata
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)metadata;
    
    // Create a new metadata item
    MetadataItem* item = (MetadataItem*)malloc(sizeof(MetadataItem));
    if (!item) {
        return NULL;
    }
    
    // Initialize the item
    strncpy(item->name, name, MAX_METADATA_ITEM_NAME_LENGTH - 1);
    item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
    item->type = type;
    
    // Initialize value based on type
    switch (type) {
        case METADATA_TYPE_TIMESTAMP:
            item->value.timestamp.count = 0;
            item->value.timestamp.min_timestamp = 0;
            item->value.timestamp.max_timestamp = 0;
            item->timestamp_value = 0;
            break;
            
        case METADATA_TYPE_STRING:
            item->value.string.high_freq_count = 0;
            item->value.string.special_string_count = 0;
            item->value.string.total_string_count = 0;
            item->value.string.avg_string_length = 0;
            break;
            
        case METADATA_TYPE_NUMERIC:
            item->value.numeric.min_value = 0.0;
            item->value.numeric.max_value = 0.0;
            item->value.numeric.avg_value = 0.0;
            item->value.numeric.mode_value = 0.0;
            item->value.numeric.mode_count = 0;
            item->value.numeric.total_count = 0;
            item->numeric_value = 0.0;
            break;
            
        case METADATA_TYPE_CATEGORICAL:
            item->value.categorical.high_freq_category_count = 0;
            item->value.categorical.total_category_count = 0;
            item->value.categorical.total_value_count = 0;
            break;
            
        default:
            break;
    }
    
    // Add the item to the base_metadata
    if (!ext_metadata->base_metadata->items) {
        // First item
        ext_metadata->base_metadata->items = (MetadataItem**)malloc(sizeof(MetadataItem*));
        if (!ext_metadata->base_metadata->items) {
            free(item);
            return NULL;
        }
        ext_metadata->base_metadata->item_count = 1;
        ext_metadata->base_metadata->items[0] = item;
    } else {
        // Additional items - expand the array
        uint32_t new_count = ext_metadata->base_metadata->item_count + 1;
        MetadataItem** new_items = (MetadataItem**)realloc(
            ext_metadata->base_metadata->items,
            new_count * sizeof(MetadataItem*)
        );
        
        if (!new_items) {
            free(item);
            return NULL;
        }
        
        ext_metadata->base_metadata->items = new_items;
        ext_metadata->base_metadata->items[ext_metadata->base_metadata->item_count] = item;
        ext_metadata->base_metadata->item_count = new_count;
    }
    
    return item;
}

/**
 * Adds a custom metadata item to the file metadata
 * 
 * This function creates a new custom metadata item based on an SQL query and adds it to the file metadata.
 * 
 * metadata: Pointer to the Metadata structure
 * name: Name of the custom metadata item
 * sql_query: SQL query used to generate this metadata
 * 
 * Return: Pointer to the newly created custom metadata item, or NULL if addition fails
 */
CustomMetadataItem* addCustomFileMetadataItem(Metadata* metadata, const char* name, const char* sql_query) {
    if (!metadata || !name || !sql_query) {
        return NULL;
    }
    
    // Cast to extended metadata
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)metadata;
    
    // Check if we've reached the maximum number of custom metadata items
    if (ext_metadata->custom_metadata_count >= MAX_CUSTOM_METADATA_ITEMS) {
        return NULL;
    }
    
    // Allocate or expand the custom metadata array
    if (!ext_metadata->custom_metadata) {
        ext_metadata->custom_metadata = (CustomMetadataItem*)malloc(sizeof(CustomMetadataItem));
        if (!ext_metadata->custom_metadata) {
            return NULL;
        }
        ext_metadata->custom_metadata_count = 0;
    } else {
        CustomMetadataItem* new_array = (CustomMetadataItem*)realloc(
            ext_metadata->custom_metadata, 
            (ext_metadata->custom_metadata_count + 1) * sizeof(CustomMetadataItem)
        );
        if (!new_array) {
            return NULL;
        }
        ext_metadata->custom_metadata = new_array;
    }
    
    // Get the new custom metadata item
    CustomMetadataItem* item = &ext_metadata->custom_metadata[ext_metadata->custom_metadata_count];
    
    // Initialize the item
    strncpy(item->name, name, MAX_METADATA_ITEM_NAME_LENGTH - 1);
    item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';  // Ensure null-termination
    
    strncpy(item->sql_query, sql_query, MAX_STRING_LENGTH - 1);
    item->sql_query[MAX_STRING_LENGTH - 1] = '\0';  // Ensure null-termination
    
    item->result_matrix = NULL;
    item->row_group_count = 0;
    item->column_count = 0;
    
    // Increment the count
    ext_metadata->custom_metadata_count++;
    
    return item;
}

/**
 * Adds a metadata item to a row group
 * 
 * This function creates a new metadata item and adds it to the specified row group.
 * 
 * metadata: Pointer to the Metadata structure
 * row_group_index: Index of the row group
 * name: Name of the metadata item
 * type: Type of the metadata
 * 
 * Return: Pointer to the newly created metadata item, or NULL if addition fails
 */
MetadataItem* addRowGroupMetadataItem(Metadata* metadata, uint32_t row_group_index, 
                                     const char* name, MetadataType type) {
    if (!metadata || !name) {
        return NULL;
    }
    
    // Cast to extended metadata
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)metadata;
    
    // Check if the metadata is for a file (only files have row groups)
    if (ext_metadata->type != METADATA_TYPE_FILE) {
        return NULL;
    }
    
    // If the row group index is beyond the current child count, resize the array
    if (row_group_index >= ext_metadata->child_count) {
        // Allocate a new, larger array
        uint32_t new_count = row_group_index + 1;
        
        if (ext_metadata->child_count == 0) {
            // First allocation
            ext_metadata->child_metadata = (struct ExtendedMetadata**)malloc(
                new_count * sizeof(struct ExtendedMetadata*));
            
            if (!ext_metadata->child_metadata) {
                return NULL;
            }
            
            // Clear the array
            memset(ext_metadata->child_metadata, 0, new_count * sizeof(struct ExtendedMetadata*));
        } else {
            // Resize the existing array
            struct ExtendedMetadata** new_array = (struct ExtendedMetadata**)realloc(
                ext_metadata->child_metadata, 
                new_count * sizeof(struct ExtendedMetadata*));
            
            if (!new_array) {
                return NULL;
            }
            
            // Clear the new portion of the array
            memset(new_array + ext_metadata->child_count, 0, 
                  (new_count - ext_metadata->child_count) * sizeof(struct ExtendedMetadata*));
            
            // Update the array pointer
            ext_metadata->child_metadata = new_array;
        }
        
        // Update the child count
        ext_metadata->child_count = new_count;
    }
    
    // If the row group doesn't exist yet, create it
    if (ext_metadata->child_metadata[row_group_index] == NULL) {
        // Create a new row group metadata
        ext_metadata->child_metadata[row_group_index] = (struct ExtendedMetadata*)malloc(
            sizeof(struct ExtendedMetadata));
        
        if (!ext_metadata->child_metadata[row_group_index]) {
            return NULL;
        }
        
        // Initialize the row group metadata
        struct ExtendedMetadata* row_group_metadata = ext_metadata->child_metadata[row_group_index];
        memset(row_group_metadata, 0, sizeof(struct ExtendedMetadata));
        
        // Set basic properties
        row_group_metadata->type = METADATA_TYPE_ROW_GROUP;
        row_group_metadata->id = row_group_index;
        snprintf(row_group_metadata->name, MAX_METADATA_STRING_LENGTH, 
                "RowGroup_%u", row_group_index);
    }
    
    // Get the row group metadata
    struct ExtendedMetadata* row_group_metadata = ext_metadata->child_metadata[row_group_index];
    
    // Create a new metadata item
    MetadataItem* item = (MetadataItem*)malloc(sizeof(MetadataItem));
    if (!item) {
        return NULL;
    }
    
    // Initialize the metadata item
    memset(item, 0, sizeof(MetadataItem));
    strncpy(item->name, name, MAX_METADATA_ITEM_NAME_LENGTH - 1);
    item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';  // Ensure null-termination
    item->type = type;
    
    // Initialize the metadata value based on type
    switch (type) {
        case METADATA_TYPE_TIMESTAMP:
            item->value.timestamp.min_timestamp = 0;
            item->value.timestamp.max_timestamp = 0;
            item->value.timestamp.count = 0;
            break;
            
        case METADATA_TYPE_STRING:
            item->value.string.high_freq_count = 0;
            item->value.string.special_string_count = 0;
            item->value.string.total_string_count = 0;
            item->value.string.avg_string_length = 0;
            break;
            
        case METADATA_TYPE_NUMERIC:
            item->value.numeric.min_value = 0.0;
            item->value.numeric.max_value = 0.0;
            item->value.numeric.avg_value = 0.0;
            item->value.numeric.mode_value = 0.0;
            item->value.numeric.mode_count = 0;
            item->value.numeric.total_count = 0;
            item->value.numeric.null_count = 0;
            break;
            
        case METADATA_TYPE_CATEGORICAL:
            item->value.categorical.high_freq_category_count = 0;
            item->value.categorical.total_category_count = 0;
            item->value.categorical.total_value_count = 0;
            break;
            
        default:
            break;
    }
    
    // Add the item to the row group metadata
    if (!row_group_metadata->base_metadata) {
        // Create the base metadata if it doesn't exist
        row_group_metadata->base_metadata = (BaseMetadata*)malloc(sizeof(BaseMetadata));
        if (!row_group_metadata->base_metadata) {
            free(item);
            return NULL;
        }
        memset(row_group_metadata->base_metadata, 0, sizeof(BaseMetadata));
    }
    
    // Add the item to the base metadata
    if (!row_group_metadata->base_metadata->items) {
        // First item
        row_group_metadata->base_metadata->items = (MetadataItem**)malloc(sizeof(MetadataItem*));
        if (!row_group_metadata->base_metadata->items) {
            free(item);
            return NULL;
        }
        row_group_metadata->base_metadata->item_count = 1;
        row_group_metadata->base_metadata->items[0] = item;
    } else {
        // Additional items - expand the array
        uint32_t new_count = row_group_metadata->base_metadata->item_count + 1;
        MetadataItem** new_items = (MetadataItem**)realloc(
            row_group_metadata->base_metadata->items,
            new_count * sizeof(MetadataItem*)
        );
        
        if (!new_items) {
            free(item);
            return NULL;
        }
        
        row_group_metadata->base_metadata->items = new_items;
        row_group_metadata->base_metadata->items[row_group_metadata->base_metadata->item_count] = item;
        row_group_metadata->base_metadata->item_count = new_count;
    }
    
    return item;
}

/**
 * Adds a metadata item to a column
 * 
 * This function creates a new metadata item and adds it to the specified column.
 * 
 * metadata: Pointer to the Metadata structure
 * column_index: Index of the column
 * column_name: Name of the column
 * name: Name of the metadata item
 * type: Type of the metadata
 * 
 * Return: Pointer to the newly created metadata item, or NULL if addition fails
 */
MetadataItem* addColumnMetadataItem(Metadata* metadata, uint32_t column_index, const char* column_name,
                                   const char* name, MetadataType type) {
    if (!metadata || !column_name || !name) {
        return NULL;
    }
    
    // Cast to extended metadata
    struct ExtendedMetadata* ext_metadata = (struct ExtendedMetadata*)metadata;
    
    // This function should be used primarily on row group metadata
    if (ext_metadata->type != METADATA_TYPE_ROW_GROUP) {
        return NULL;
    }
    
    // Ensure we have enough column metadata objects
    if (column_index >= ext_metadata->child_count) {
        // Need to allocate or expand the child metadata array
        uint32_t new_count = column_index + 1;
        
        if (ext_metadata->child_metadata == NULL) {
            // First allocation
            ext_metadata->child_metadata = (struct ExtendedMetadata**)malloc(
                new_count * sizeof(struct ExtendedMetadata*));
            
            if (!ext_metadata->child_metadata) {
                return NULL;
            }
            
            // Initialize all to NULL
            memset(ext_metadata->child_metadata, 0, new_count * sizeof(struct ExtendedMetadata*));
        } else {
            // Expand existing array
            struct ExtendedMetadata** new_array = (struct ExtendedMetadata**)realloc(
                ext_metadata->child_metadata,
                new_count * sizeof(struct ExtendedMetadata*));
            
            if (!new_array) {
                return NULL;
            }
            
            // Initialize new elements to NULL
            for (uint32_t i = ext_metadata->child_count; i < new_count; i++) {
                new_array[i] = NULL;
            }
            
            ext_metadata->child_metadata = new_array;
        }
        
        // Update the count
        ext_metadata->child_count = new_count;
    }
    
    // Check if the column metadata exists
    if (ext_metadata->child_metadata[column_index] == NULL) {
        // Create a new column metadata
        ext_metadata->child_metadata[column_index] = (struct ExtendedMetadata*)malloc(
            sizeof(struct ExtendedMetadata));
        
        if (!ext_metadata->child_metadata[column_index]) {
            return NULL;
        }
        
        // Initialize the column metadata
        struct ExtendedMetadata* column_metadata = ext_metadata->child_metadata[column_index];
        memset(column_metadata, 0, sizeof(struct ExtendedMetadata));
        
        // Set basic properties
        column_metadata->type = METADATA_TYPE_COLUMN;
        column_metadata->id = column_index;
        strncpy(column_metadata->name, column_name, MAX_METADATA_STRING_LENGTH - 1);
        column_metadata->name[MAX_METADATA_STRING_LENGTH - 1] = '\0';  // Ensure null-termination
    }
    
    // Get the column metadata
    struct ExtendedMetadata* column_metadata = ext_metadata->child_metadata[column_index];
    
    // Create a new metadata item
    MetadataItem* item = (MetadataItem*)malloc(sizeof(MetadataItem));
    if (!item) {
        return NULL;
    }
    
    // Initialize the metadata item
    memset(item, 0, sizeof(MetadataItem));
    strncpy(item->name, name, MAX_METADATA_ITEM_NAME_LENGTH - 1);
    item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';  // Ensure null-termination
    item->type = type;
    
    // Initialize the metadata value based on type
    switch (type) {
        case METADATA_TYPE_TIMESTAMP:
            item->value.timestamp.min_timestamp = 0;
            item->value.timestamp.max_timestamp = 0;
            item->value.timestamp.count = 0;
            break;
            
        case METADATA_TYPE_STRING:
            item->value.string.high_freq_count = 0;
            item->value.string.special_string_count = 0;
            item->value.string.total_string_count = 0;
            item->value.string.avg_string_length = 0;
            break;
            
        case METADATA_TYPE_NUMERIC:
            item->value.numeric.min_value = 0.0;
            item->value.numeric.max_value = 0.0;
            item->value.numeric.avg_value = 0.0;
            item->value.numeric.mode_value = 0.0;
            item->value.numeric.mode_count = 0;
            item->value.numeric.total_count = 0;
            item->value.numeric.null_count = 0;
            break;
            
        case METADATA_TYPE_CATEGORICAL:
            item->value.categorical.high_freq_category_count = 0;
            item->value.categorical.total_category_count = 0;
            item->value.categorical.total_value_count = 0;
            break;
            
        default:
            break;
    }
    
    // Add the item to the column metadata
    if (!column_metadata->base_metadata) {
        // Create the base metadata if it doesn't exist
        column_metadata->base_metadata = (BaseMetadata*)malloc(sizeof(BaseMetadata));
        if (!column_metadata->base_metadata) {
            free(item);
            return NULL;
        }
        memset(column_metadata->base_metadata, 0, sizeof(BaseMetadata));
    }
    
    // Add the item to the base metadata
    if (!column_metadata->base_metadata->items) {
        // First item
        column_metadata->base_metadata->items = (MetadataItem**)malloc(sizeof(MetadataItem*));
        if (!column_metadata->base_metadata->items) {
            free(item);
            return NULL;
        }
        column_metadata->base_metadata->item_count = 1;
        column_metadata->base_metadata->items[0] = item;
    } else {
        // Additional items - expand the array
        uint32_t new_count = column_metadata->base_metadata->item_count + 1;
        MetadataItem** new_items = (MetadataItem**)realloc(
            column_metadata->base_metadata->items,
            new_count * sizeof(MetadataItem*)
        );
        
        if (!new_items) {
            free(item);
            return NULL;
        }
        
        column_metadata->base_metadata->items = new_items;
        column_metadata->base_metadata->items[column_metadata->base_metadata->item_count] = item;
        column_metadata->base_metadata->item_count = new_count;
    }
    
    return item;
}

/**
 * Generate custom metadata from configuration file
 * 
 * Loads and evaluates custom metadata based on the specified configuration file.
 * 
 * metadata: The file metadata to add custom metadata to
 * file: The parquet file structure
 * config_path: Path to the custom metadata configuration file
 * returns: Error code (METADATA_GEN_OK on success)
 */
static MetadataGeneratorError generate_custom_metadata(
    struct ExtendedMetadata* metadata,
    const ParquetFile* file,
    const char* config_path
) {
    if (!metadata || !file || !config_path) {
        return METADATA_GEN_INVALID_PARAMETER;
    }
    
    // Allocate for custom metadata array
    CustomMetadataItem* custom_items = (CustomMetadataItem*)malloc(MAX_CUSTOM_METADATA_ITEMS * sizeof(CustomMetadataItem));
    if (!custom_items) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to allocate memory for custom metadata");
        return METADATA_GEN_MEMORY_ERROR;
    }
    
    // Parse custom metadata configuration
    uint32_t custom_count = 0;
    CustomMetadataError custom_error = custom_metadata_parse_config(
        config_path,
        custom_items,
        &custom_count
    );
    
    if (custom_error != CUSTOM_METADATA_OK) {
        free(custom_items);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to parse custom metadata configuration: %s", 
                custom_metadata_get_error());
        return METADATA_GEN_CUSTOM_METADATA_ERROR;
    }
    
    // Store the custom items
    metadata->custom_metadata = custom_items;
    metadata->custom_metadata_count = custom_count;
    
    // Create a reader context if needed for evaluation
    ParquetReaderContext* reader_context = NULL;
    bool created_context = false;
    
    if (!reader_context) {
        reader_context = parquet_reader_open(file);
        if (!reader_context) {
            custom_metadata_free_items(custom_items, custom_count);
            free(custom_items);
            metadata->custom_metadata = NULL;
            metadata->custom_metadata_count = 0;
            snprintf(s_error_message, sizeof(s_error_message), 
                    "Failed to create reader context for custom metadata evaluation");
            return METADATA_GEN_PARQUET_ERROR;
        }
        created_context = true;
    }
    
    // Evaluate custom metadata
    custom_error = custom_metadata_evaluate(
        file,
        reader_context,
        metadata->custom_metadata,
        metadata->custom_metadata_count
    );
    
    // Clean up reader context if we created it
    if (created_context && reader_context) {
        parquet_reader_close(reader_context);
    }
    
    if (custom_error != CUSTOM_METADATA_OK) {
        custom_metadata_free_items(metadata->custom_metadata, metadata->custom_metadata_count);
        free(metadata->custom_metadata);
        metadata->custom_metadata = NULL;
        metadata->custom_metadata_count = 0;
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to evaluate custom metadata: %s", 
                custom_metadata_get_error());
        return METADATA_GEN_CUSTOM_METADATA_ERROR;
    }
    
    return METADATA_GEN_OK;
}

/**
 * Generate file level metadata for a parquet file
 * 
 * Aggregates metadata from all row groups in the parquet file and 
 * generates file level statistics and summaries.
 * 
 * file: The parquet file to generate metadata for
 * config_path: Optional path to custom metadata configuration file (can be NULL)
 * progress_callback: Optional callback function to report progress
 * progress_data: User data to pass to the progress callback
 * returns: The generated file metadata, or NULL on error
 */
Metadata* metadata_generator_generate_file_metadata(
    const ParquetFile* file,
    const char* config_path,
    ProgressCallback progress_callback,
    void* progress_data
) {
    // Parameter validation
    if (!file) {
        snprintf(s_error_message, sizeof(s_error_message), "Invalid parameter: file is NULL");
        return NULL;
    }
    
    // Get basic file information
    uint32_t row_group_count = parquet_file_get_row_group_count(file);
    uint32_t column_count = parquet_file_get_column_count(file);
    
    // Create extended metadata object for the file
    struct ExtendedMetadata* ext_metadata = malloc(sizeof(struct ExtendedMetadata));
    if (!ext_metadata) {
        snprintf(s_error_message, sizeof(s_error_message), "Failed to allocate memory for file metadata");
        return NULL;
    }
    
    // Initialize the metadata object
    memset(ext_metadata, 0, sizeof(struct ExtendedMetadata));
    ext_metadata->metadata.type = METADATA_TYPE_FILE;
    ext_metadata->metadata.id = 0; // File ID placeholder
    strncpy(ext_metadata->metadata.name, parquet_file_get_path(file), MAX_METADATA_STRING_LENGTH - 1);
    ext_metadata->metadata.name[MAX_METADATA_STRING_LENGTH - 1] = '\0'; // Ensure null-termination
    
    // Allocate memory for child metadata pointers (row group + aggregated column metadata)
    ext_metadata->child_metadata = malloc(sizeof(struct ExtendedMetadata*) * (row_group_count + column_count));
    if (!ext_metadata->child_metadata) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to allocate memory for child metadata pointers");
        free(ext_metadata);
        return NULL;
    }
    
    ext_metadata->child_count = 0; // Will be incremented as we add children
    
    // Structures to store aggregated metadata for each column
    typedef struct {
        // For numeric data
        double min_double;
        double max_double;
        double sum_double;
        uint64_t count_double;
        
        int64_t min_int;
        int64_t max_int;
        int64_t sum_int;
        uint64_t count_int;
        
        // For boolean data
        uint64_t true_count;
        uint64_t false_count;
        
        // For string data
        uint32_t min_length;
        uint32_t max_length;
        uint64_t total_length;
        uint64_t string_count;
        
        // String frequency tracking
        struct {
            char string[MAX_STRING_LENGTH];
            uint32_t count;
        } high_freq_strings[MAX_HIGH_FREQ_STRINGS];
        uint32_t high_freq_string_count;
        
        // For all types
        bool has_null;
        int type; // Using int instead of ParquetPhysicalType
        bool initialized;
    } AggregatedColumnMetadata;
    
    // Allocate and initialize aggregated column metadata
    AggregatedColumnMetadata* agg_column_metadata = (AggregatedColumnMetadata*)malloc(
        column_count * sizeof(AggregatedColumnMetadata));
    if (!agg_column_metadata) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to allocate memory for aggregated column metadata");
        free(ext_metadata->child_metadata);
        free(ext_metadata);
        return NULL;
    }
    
    // Initialize aggregated column metadata
    memset(agg_column_metadata, 0, column_count * sizeof(AggregatedColumnMetadata));
    for (uint32_t i = 0; i < column_count; i++) {
        agg_column_metadata[i].min_double = 1e300; // Very large value
        agg_column_metadata[i].max_double = -1e300; // Very small value
        agg_column_metadata[i].min_int = INT64_MAX;
        agg_column_metadata[i].max_int = INT64_MIN;
        agg_column_metadata[i].min_length = UINT32_MAX;
        agg_column_metadata[i].type = parquet_file_get_column_type(file, i);
        agg_column_metadata[i].initialized = false;
    }
    
    // Process all row groups and generate row group metadata
    uint64_t total_rows = 0;
    uint64_t total_size = 0;
    int progress_percent = 0;
    
    for (uint32_t i = 0; i < row_group_count; i++) {
        // Report progress
        if (progress_callback) {
            int new_progress = (int)((i * 100) / row_group_count);
            if (new_progress != progress_percent) {
                progress_percent = new_progress;
                progress_callback(progress_percent, progress_data);
            }
        }
        
        // Generate row group metadata and add it as a child
        Metadata* row_group_metadata = metadata_generator_generate_row_group_metadata(file, i, NULL, NULL);
        if (!row_group_metadata) {
            // Error message set by generate_row_group_metadata
            for (uint32_t j = 0; j < ext_metadata->child_count; j++) {
                metadata_release(ext_metadata->child_metadata[j]);
            }
            free(agg_column_metadata);
            free(ext_metadata->child_metadata);
            free(ext_metadata);
            return NULL;
        }
        
        // Add the row group metadata as a child
        ext_metadata->child_metadata[ext_metadata->child_count++] = row_group_metadata;
        
        // Get the row group metadata as an extended metadata
        struct ExtendedMetadata* rg_metadata = (struct ExtendedMetadata*)row_group_metadata;
        
        // Accumulate statistics - using the id field to store row count as a temporary workaround
        total_rows += rg_metadata->metadata.id;
        total_size += parquet_file_get_row_group_size(file, i);
        
        // Aggregate column metadata from this row group
        for (uint32_t c = 0; c < rg_metadata->child_count; c++) {
            struct ExtendedMetadata* col_metadata = (struct ExtendedMetadata*)rg_metadata->child_metadata[c];
            
            // Find the column index for this column name
            uint32_t col_index = UINT32_MAX;
            for (uint32_t col = 0; col < column_count; col++) {
                if (strcmp(parquet_file_get_column_name(file, col), col_metadata->metadata.name) == 0) {
                    col_index = col;
                    break;
                }
            }
            
            if (col_index == UINT32_MAX) {
                // Column not found, which should not happen
                continue;
            }
            
            // Mark column as initialized
            agg_column_metadata[col_index].initialized = true;
            
            // Aggregate metadata based on column type
            int col_type = col_metadata->metadata.type;
            
            // Check if we have base_metadata and use that for calculations
            if (col_metadata->base_metadata) {
                BaseMetadata* base = col_metadata->base_metadata;
                
                // Process timestamp metadata
                if (base->timestamp_metadata.has_timestamps) {
                    // Set has_null flag
                    if (!agg_column_metadata[col_index].has_null && 
                        base->timestamp_metadata.null_count > 0) {
                        agg_column_metadata[col_index].has_null = true;
                    }
                    
                    // Update min timestamp if lower than current tracked min
                    if (base->timestamp_metadata.min_timestamp < agg_column_metadata[col_index].min_double || 
                        agg_column_metadata[col_index].min_double == 1e300) {  // Initial value check
                        agg_column_metadata[col_index].min_double = (double)base->timestamp_metadata.min_timestamp;
                    }
                    
                    // Update max timestamp if higher than current tracked max
                    if (base->timestamp_metadata.max_timestamp > agg_column_metadata[col_index].max_double || 
                        agg_column_metadata[col_index].max_double == -1e300) {  // Initial value check
                        agg_column_metadata[col_index].max_double = (double)base->timestamp_metadata.max_timestamp;
                    }
                    
                    // Accumulate count
                    agg_column_metadata[col_index].count_double += base->timestamp_metadata.count;
                }
                
                // Process numeric metadata
                if (base->numeric_metadata.has_numeric_data) {
                    // Set has_null flag
                    if (!agg_column_metadata[col_index].has_null && 
                        base->numeric_metadata.null_count > 0) {
                        agg_column_metadata[col_index].has_null = true;
                    }
                    
                    // Use the base metadata values based on column type
                    if (col_type == PARQUET_TYPE_INT32 || col_type == PARQUET_TYPE_INT64) {
                        // Update min if lower than current tracked min
                        if (base->numeric_metadata.min_value < agg_column_metadata[col_index].min_int) {
                            agg_column_metadata[col_index].min_int = (int64_t)base->numeric_metadata.min_value;
                        }
                        
                        // Update max if higher than current tracked max
                        if (base->numeric_metadata.max_value > agg_column_metadata[col_index].max_int) {
                            agg_column_metadata[col_index].max_int = (int64_t)base->numeric_metadata.max_value;
                        }
                        
                        // Accumulate sum and count
                        uint64_t row_count = base->numeric_metadata.total_count;
                        agg_column_metadata[col_index].sum_int += (int64_t)(base->numeric_metadata.avg_value * row_count);
                        agg_column_metadata[col_index].count_int += row_count;
                    }
                    else if (col_type == PARQUET_TYPE_FLOAT || col_type == PARQUET_TYPE_DOUBLE) {
                        // Update min if lower than current tracked min
                        if (base->numeric_metadata.min_value < agg_column_metadata[col_index].min_double) {
                            agg_column_metadata[col_index].min_double = base->numeric_metadata.min_value;
                        }
                        
                        // Update max if higher than current tracked max
                        if (base->numeric_metadata.max_value > agg_column_metadata[col_index].max_double) {
                            agg_column_metadata[col_index].max_double = base->numeric_metadata.max_value;
                        }
                        
                        // Accumulate sum and count
                        uint64_t row_count = base->numeric_metadata.total_count;
                        agg_column_metadata[col_index].sum_double += base->numeric_metadata.avg_value * row_count;
                        agg_column_metadata[col_index].count_double += row_count;
                    }
                    else if (col_type == PARQUET_TYPE_BOOLEAN) {
                        // For boolean data, track true/false counts
                        double true_ratio = base->numeric_metadata.avg_value;
                        uint64_t row_count = base->numeric_metadata.total_count;
                        uint64_t true_count = (uint64_t)(true_ratio * row_count);
                        uint64_t false_count = row_count - true_count;
                        
                        agg_column_metadata[col_index].true_count += true_count;
                        agg_column_metadata[col_index].false_count += false_count;
                    }
                }
                
                // Process string metadata
                if (base->string_metadata.has_string_data) {
                    // Set has_null flag
                    if (!agg_column_metadata[col_index].has_null && 
                        base->string_metadata.null_count > 0) {
                        agg_column_metadata[col_index].has_null = true;
                    }
                    
                    // Update min length if lower
                    if (base->string_metadata.min_length < agg_column_metadata[col_index].min_length || 
                        agg_column_metadata[col_index].min_length == UINT32_MAX) {  // Initial value check
                        agg_column_metadata[col_index].min_length = base->string_metadata.min_length;
                    }
                    
                    // Update max length if higher
                    if (base->string_metadata.max_length > agg_column_metadata[col_index].max_length) {
                        agg_column_metadata[col_index].max_length = base->string_metadata.max_length;
                    }
                    
                    // Accumulate string length and count
                    agg_column_metadata[col_index].total_length += base->string_metadata.total_length;
                    agg_column_metadata[col_index].string_count += base->string_metadata.total_count;
                    
                    // Merge high frequency strings - improved for efficiency
                    for (uint32_t s = 0; s < base->string_metadata.high_freq_count; s++) {
                        const char* curr_string = base->string_metadata.high_freq_strings[s].string;
                        uint32_t curr_count = base->string_metadata.high_freq_strings[s].count;
                        
                        // Skip empty strings
                        if (!curr_string[0]) {
                            continue;
                        }
                        
                        // Check if this string is already in our list
                        bool found = false;
                        uint32_t found_idx = 0;
                        
                        for (uint32_t j = 0; j < agg_column_metadata[col_index].high_freq_string_count; j++) {
                            if (strcmp(agg_column_metadata[col_index].high_freq_strings[j].string, curr_string) == 0) {
                                // Found the string, update count
                                agg_column_metadata[col_index].high_freq_strings[j].count += curr_count;
                                found = true;
                                found_idx = j;
                                break;
                            }
                        }
                        
                        // If found and not at beginning, bubble up if count warrants it
                        if (found && found_idx > 0) {
                            uint32_t count = agg_column_metadata[col_index].high_freq_strings[found_idx].count;
                            uint32_t j = found_idx;
                            
                            // Bubble up if this string now has a higher count than strings before it
                            while (j > 0 && count > agg_column_metadata[col_index].high_freq_strings[j-1].count) {
                                // Swap with previous entry
                                char temp_str[MAX_STRING_LENGTH];
                                uint32_t temp_count = agg_column_metadata[col_index].high_freq_strings[j-1].count;
                                
                                strncpy(temp_str, agg_column_metadata[col_index].high_freq_strings[j-1].string, MAX_STRING_LENGTH-1);
                                temp_str[MAX_STRING_LENGTH-1] = '\0';
                                
                                strncpy(agg_column_metadata[col_index].high_freq_strings[j-1].string, 
                                        agg_column_metadata[col_index].high_freq_strings[j].string, MAX_STRING_LENGTH-1);
                                agg_column_metadata[col_index].high_freq_strings[j-1].string[MAX_STRING_LENGTH-1] = '\0';
                                agg_column_metadata[col_index].high_freq_strings[j-1].count = count;
                                
                                strncpy(agg_column_metadata[col_index].high_freq_strings[j].string, temp_str, MAX_STRING_LENGTH-1);
                                agg_column_metadata[col_index].high_freq_strings[j].string[MAX_STRING_LENGTH-1] = '\0';
                                agg_column_metadata[col_index].high_freq_strings[j].count = temp_count;
                                
                                j--;
                            }
                        }
                        
                        // If not found and we have space, add it
                        if (!found && agg_column_metadata[col_index].high_freq_string_count < MAX_HIGH_FREQ_STRINGS) {
                            uint32_t idx = agg_column_metadata[col_index].high_freq_string_count++;
                            strncpy(agg_column_metadata[col_index].high_freq_strings[idx].string, 
                                    curr_string, MAX_STRING_LENGTH - 1);
                            agg_column_metadata[col_index].high_freq_strings[idx].string[MAX_STRING_LENGTH - 1] = '\0';
                            agg_column_metadata[col_index].high_freq_strings[idx].count = curr_count;
                            
                            // Insert sort to maintain order by count
                            while (idx > 0 && agg_column_metadata[col_index].high_freq_strings[idx].count > 
                                  agg_column_metadata[col_index].high_freq_strings[idx-1].count) {
                                // Swap with previous entry
                                char temp_str[MAX_STRING_LENGTH];
                                uint32_t temp_count = agg_column_metadata[col_index].high_freq_strings[idx-1].count;
                                
                                strncpy(temp_str, agg_column_metadata[col_index].high_freq_strings[idx-1].string, MAX_STRING_LENGTH-1);
                                temp_str[MAX_STRING_LENGTH-1] = '\0';
                                
                                strncpy(agg_column_metadata[col_index].high_freq_strings[idx-1].string, 
                                        agg_column_metadata[col_index].high_freq_strings[idx].string, MAX_STRING_LENGTH-1);
                                agg_column_metadata[col_index].high_freq_strings[idx-1].string[MAX_STRING_LENGTH-1] = '\0';
                                agg_column_metadata[col_index].high_freq_strings[idx-1].count = 
                                    agg_column_metadata[col_index].high_freq_strings[idx].count;
                                
                                strncpy(agg_column_metadata[col_index].high_freq_strings[idx].string, temp_str, MAX_STRING_LENGTH-1);
                                agg_column_metadata[col_index].high_freq_strings[idx].string[MAX_STRING_LENGTH-1] = '\0';
                                agg_column_metadata[col_index].high_freq_strings[idx].count = temp_count;
                                
                                idx--;
                            }
                        }
                        // If not found but no space, find the lowest count and replace if this one is higher
                        else if (!found) {
                            uint32_t min_idx = 0;
                            uint32_t min_count = UINT32_MAX;
                            
                            // Find the string with the lowest count
                            for (uint32_t j = 0; j < agg_column_metadata[col_index].high_freq_string_count; j++) {
                                if (agg_column_metadata[col_index].high_freq_strings[j].count < min_count) {
                                    min_count = agg_column_metadata[col_index].high_freq_strings[j].count;
                                    min_idx = j;
                                }
                            }
                            
                            // Replace if this string has a higher count
                            if (curr_count > min_count) {
                                strncpy(agg_column_metadata[col_index].high_freq_strings[min_idx].string, 
                                        curr_string, MAX_STRING_LENGTH - 1);
                                agg_column_metadata[col_index].high_freq_strings[min_idx].string[MAX_STRING_LENGTH - 1] = '\0';
                                agg_column_metadata[col_index].high_freq_strings[min_idx].count = curr_count;
                                
                                // Resort the array to maintain order by frequency
                                for (uint32_t j = 0; j < agg_column_metadata[col_index].high_freq_string_count - 1; j++) {
                                    for (uint32_t k = 0; k < agg_column_metadata[col_index].high_freq_string_count - j - 1; k++) {
                                        if (agg_column_metadata[col_index].high_freq_strings[k].count < 
                                            agg_column_metadata[col_index].high_freq_strings[k + 1].count) {
                                            // Swap
                                            char temp_str[MAX_STRING_LENGTH];
                                            uint32_t temp_count = agg_column_metadata[col_index].high_freq_strings[k].count;
                                            
                                            strncpy(temp_str, agg_column_metadata[col_index].high_freq_strings[k].string, MAX_STRING_LENGTH-1);
                                            temp_str[MAX_STRING_LENGTH-1] = '\0';
                                            
                                            strncpy(agg_column_metadata[col_index].high_freq_strings[k].string, 
                                                    agg_column_metadata[col_index].high_freq_strings[k+1].string, MAX_STRING_LENGTH-1);
                                            agg_column_metadata[col_index].high_freq_strings[k].string[MAX_STRING_LENGTH-1] = '\0';
                                            agg_column_metadata[col_index].high_freq_strings[k].count = 
                                                agg_column_metadata[col_index].high_freq_strings[k+1].count;
                                            
                                            strncpy(agg_column_metadata[col_index].high_freq_strings[k+1].string, temp_str, MAX_STRING_LENGTH-1);
                                            agg_column_metadata[col_index].high_freq_strings[k+1].string[MAX_STRING_LENGTH-1] = '\0';
                                            agg_column_metadata[col_index].high_freq_strings[k+1].count = temp_count;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                // Process categorical metadata if present
                if (base->categorical_metadata.has_categorical_data) {
                    // This is just a marker that we need to process categorical data
                    // The actual categorical processing would be implemented here
                }
            }
        }
    }
    
    // Now generate aggregated metadata for each column
    for (uint32_t i = 0; i < column_count; i++) {
        if (!agg_column_metadata[i].initialized) {
            continue; // Skip columns that weren't present in any row group
        }
        
        // Create column metadata
        struct ExtendedMetadata* col_metadata = malloc(sizeof(struct ExtendedMetadata));
        if (!col_metadata) {
            // Error handling - cleanup and return
            for (uint32_t j = 0; j < ext_metadata->child_count; j++) {
                metadata_release(ext_metadata->child_metadata[j]);
            }
            free(agg_column_metadata);
            free(ext_metadata->child_metadata);
            free(ext_metadata);
            snprintf(s_error_message, sizeof(s_error_message), 
                    "Failed to allocate memory for aggregated column metadata");
            return NULL;
        }
        
        // Initialize column metadata
        memset(col_metadata, 0, sizeof(struct ExtendedMetadata));
        col_metadata->metadata.type = METADATA_TYPE_COLUMN;
        col_metadata->metadata.id = i; // Column ID
        strncpy(col_metadata->metadata.name, parquet_file_get_column_name(file, i), 
                MAX_METADATA_STRING_LENGTH - 1);
        col_metadata->metadata.name[MAX_METADATA_STRING_LENGTH - 1] = '\0'; // Ensure null-termination
        
        // Allocate and initialize base metadata
        col_metadata->base_metadata = malloc(sizeof(BaseMetadata));
        if (!col_metadata->base_metadata) {
            free(col_metadata);
            // Error handling - cleanup and return
            for (uint32_t j = 0; j < ext_metadata->child_count; j++) {
                metadata_release(ext_metadata->child_metadata[j]);
            }
            free(agg_column_metadata);
            free(ext_metadata->child_metadata);
            free(ext_metadata);
            snprintf(s_error_message, sizeof(s_error_message), 
                    "Failed to allocate memory for column base metadata");
            return NULL;
        }
        
        memset(col_metadata->base_metadata, 0, sizeof(BaseMetadata));
        
        // Fill in aggregated metadata based on column type
        int col_type = agg_column_metadata[i].type;
        
        if (col_type == PARQUET_TYPE_INT32 || col_type == PARQUET_TYPE_INT64 ||
            col_type == PARQUET_TYPE_FLOAT || col_type == PARQUET_TYPE_DOUBLE ||
            col_type == PARQUET_TYPE_BOOLEAN) {
            
            col_metadata->base_metadata->numeric_metadata.has_numeric_data = true;
            
            if (col_type == PARQUET_TYPE_INT32 || col_type == PARQUET_TYPE_INT64) {
                col_metadata->base_metadata->numeric_metadata.min_value = (double)agg_column_metadata[i].min_int;
                col_metadata->base_metadata->numeric_metadata.max_value = (double)agg_column_metadata[i].max_int;
                // Calculate average (avoid divide by zero)
                if (agg_column_metadata[i].count_int > 0) {
                    col_metadata->base_metadata->numeric_metadata.avg_value = 
                        (double)agg_column_metadata[i].sum_int / agg_column_metadata[i].count_int;
                }
                col_metadata->base_metadata->numeric_metadata.total_count = agg_column_metadata[i].count_int;
            }
            else if (col_type == PARQUET_TYPE_FLOAT || col_type == PARQUET_TYPE_DOUBLE) {
                col_metadata->base_metadata->numeric_metadata.min_value = agg_column_metadata[i].min_double;
                col_metadata->base_metadata->numeric_metadata.max_value = agg_column_metadata[i].max_double;
                // Calculate average (avoid divide by zero)
                if (agg_column_metadata[i].count_double > 0) {
                    col_metadata->base_metadata->numeric_metadata.avg_value = 
                        agg_column_metadata[i].sum_double / agg_column_metadata[i].count_double;
                }
                col_metadata->base_metadata->numeric_metadata.total_count = agg_column_metadata[i].count_double;
            }
            else if (col_type == PARQUET_TYPE_BOOLEAN) {
                uint64_t total = agg_column_metadata[i].true_count + agg_column_metadata[i].false_count;
                if (total > 0) {
                    col_metadata->base_metadata->numeric_metadata.avg_value = 
                        (double)agg_column_metadata[i].true_count / total;
                }
                col_metadata->base_metadata->numeric_metadata.min_value = 0.0;
                col_metadata->base_metadata->numeric_metadata.max_value = 1.0;
                col_metadata->base_metadata->numeric_metadata.total_count = total;
            }
        }
        
        // Handle string metadata
        if (col_type == PARQUET_TYPE_BYTE_ARRAY || col_type == PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY) {
            col_metadata->base_metadata->string_metadata.has_string_data = true;
            col_metadata->base_metadata->string_metadata.min_length = agg_column_metadata[i].min_length;
            col_metadata->base_metadata->string_metadata.max_length = agg_column_metadata[i].max_length;
            col_metadata->base_metadata->string_metadata.total_length = agg_column_metadata[i].total_length;
            col_metadata->base_metadata->string_metadata.total_count = agg_column_metadata[i].string_count;
            
            // Sort high-frequency strings by count (descending)
            for (uint32_t j = 0; j < agg_column_metadata[i].high_freq_string_count; j++) {
                for (uint32_t k = j + 1; k < agg_column_metadata[i].high_freq_string_count; k++) {
                    if (agg_column_metadata[i].high_freq_strings[k].count > 
                        agg_column_metadata[i].high_freq_strings[j].count) {
                        // Swap if the current count is less than the next count
                        struct {
                            char string[MAX_STRING_LENGTH];
                            uint32_t count;
                        } temp;
                        
                        // Copy data from j to temp
                        strncpy(temp.string, agg_column_metadata[i].high_freq_strings[j].string, MAX_STRING_LENGTH - 1);
                        temp.string[MAX_STRING_LENGTH - 1] = '\0';
                        temp.count = agg_column_metadata[i].high_freq_strings[j].count;
                        
                        // Copy data from k to j
                        strncpy(agg_column_metadata[i].high_freq_strings[j].string, 
                                agg_column_metadata[i].high_freq_strings[k].string, MAX_STRING_LENGTH - 1);
                        agg_column_metadata[i].high_freq_strings[j].string[MAX_STRING_LENGTH - 1] = '\0';
                        agg_column_metadata[i].high_freq_strings[j].count = agg_column_metadata[i].high_freq_strings[k].count;
                        
                        // Copy data from temp to k
                        strncpy(agg_column_metadata[i].high_freq_strings[k].string, temp.string, MAX_STRING_LENGTH - 1);
                        agg_column_metadata[i].high_freq_strings[k].string[MAX_STRING_LENGTH - 1] = '\0';
                        agg_column_metadata[i].high_freq_strings[k].count = temp.count;
                    }
                }
            }
            
            // Copy sorted high-frequency strings to the base metadata
            col_metadata->base_metadata->string_metadata.high_freq_count = 
                agg_column_metadata[i].high_freq_string_count > MAX_HIGH_FREQ_STRINGS ? 
                MAX_HIGH_FREQ_STRINGS : agg_column_metadata[i].high_freq_string_count;
                
            for (uint32_t j = 0; j < col_metadata->base_metadata->string_metadata.high_freq_count; j++) {
                strncpy(col_metadata->base_metadata->string_metadata.high_freq_strings[j].string,
                        agg_column_metadata[i].high_freq_strings[j].string, MAX_STRING_LENGTH - 1);
                col_metadata->base_metadata->string_metadata.high_freq_strings[j].string[MAX_STRING_LENGTH - 1] = '\0';
                col_metadata->base_metadata->string_metadata.high_freq_strings[j].count = 
                    agg_column_metadata[i].high_freq_strings[j].count;
            }
            
            // Calculate average string length (avoid divide by zero)
            if (agg_column_metadata[i].string_count > 0) {
                col_metadata->base_metadata->string_metadata.avg_length = 
                    (float)agg_column_metadata[i].total_length / agg_column_metadata[i].string_count;
            }
        }
        
        // Add the column metadata as a child
        ext_metadata->child_metadata[ext_metadata->child_count++] = col_metadata;
    }
    
    // Add basic file metadata items
    BaseMetadata* base = malloc(sizeof(BaseMetadata));
    if (!base) {
        // Error handling - cleanup and return
        for (uint32_t j = 0; j < ext_metadata->child_count; j++) {
            metadata_release(ext_metadata->child_metadata[j]);
        }
        free(agg_column_metadata);
        free(ext_metadata->child_metadata);
        free(ext_metadata);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to allocate memory for file base metadata");
        return NULL;
    }
    
    memset(base, 0, sizeof(BaseMetadata));
    ext_metadata->base_metadata = base;
    
    // Initialize metadata items array
    if (!base->items) {
        base->items = (MetadataItem**)malloc(sizeof(MetadataItem*) * 10); // Allocate for multiple items
        if (!base->items) {
            free(base);
            for (uint32_t j = 0; j < ext_metadata->child_count; j++) {
                metadata_release(ext_metadata->child_metadata[j]);
            }
            free(agg_column_metadata);
            free(ext_metadata->child_metadata);
            free(ext_metadata);
            snprintf(s_error_message, sizeof(s_error_message), 
                    "Failed to allocate memory for file metadata items");
            return NULL;
        }
        memset(base->items, 0, sizeof(MetadataItem*) * 10);
        base->item_count = 0;
    }
    
    // Add metadata items to describe the file
    MetadataItem* item;
    
    // Add row count item
    item = addBasicFileMetadataItem(&ext_metadata->metadata, "row_count", METADATA_TYPE_NUMERIC);
    if (item) {
        item->numeric_value = (double)total_rows;
        
        // Add to base metadata items
        MetadataItem* row_count_item = (MetadataItem*)malloc(sizeof(MetadataItem));
        if (row_count_item) {
            strncpy(row_count_item->name, "row_count", MAX_METADATA_ITEM_NAME_LENGTH-1);
            row_count_item->name[MAX_METADATA_ITEM_NAME_LENGTH-1] = '\0';
            row_count_item->type = METADATA_TYPE_NUMERIC;
            row_count_item->value.numeric.min_value = (double)total_rows;
            row_count_item->value.numeric.max_value = (double)total_rows;
            row_count_item->value.numeric.avg_value = (double)total_rows;
            row_count_item->value.numeric.total_count = 1;
            
            if (base->item_count < 10) {
                base->items[base->item_count++] = row_count_item;
            }
        }
    }
    
    // Add file size item
    item = addBasicFileMetadataItem(&ext_metadata->metadata, "file_size", METADATA_TYPE_NUMERIC);
    if (item) {
        double file_size = (double)parquet_file_get_size(file);
        item->numeric_value = file_size;
        
        // Add to base metadata items
        MetadataItem* file_size_item = (MetadataItem*)malloc(sizeof(MetadataItem));
        if (file_size_item) {
            strncpy(file_size_item->name, "file_size", MAX_METADATA_ITEM_NAME_LENGTH-1);
            file_size_item->name[MAX_METADATA_ITEM_NAME_LENGTH-1] = '\0';
            file_size_item->type = METADATA_TYPE_NUMERIC;
            file_size_item->value.numeric.min_value = file_size;
            file_size_item->value.numeric.max_value = file_size;
            file_size_item->value.numeric.avg_value = file_size;
            file_size_item->value.numeric.total_count = 1;
            
            if (base->item_count < 10) {
                base->items[base->item_count++] = file_size_item;
            }
        }
    }
    
    // Add row group count item
    item = addBasicFileMetadataItem(&ext_metadata->metadata, "row_group_count", METADATA_TYPE_NUMERIC);
    if (item) {
        item->numeric_value = (double)row_group_count;
    }
    
    // Add column count item
    item = addBasicFileMetadataItem(&ext_metadata->metadata, "column_count", METADATA_TYPE_NUMERIC);
    if (item) {
        item->numeric_value = (double)column_count;
    }
    
    // Add average rows per row group
    if (row_group_count > 0) {
        item = addBasicFileMetadataItem(&ext_metadata->metadata, "avg_rows_per_row_group", METADATA_TYPE_NUMERIC);
        if (item) {
            item->numeric_value = (double)total_rows / row_group_count;
        }
    }
    
    // Add creation time item
    item = addBasicFileMetadataItem(&ext_metadata->metadata, "creation_time", METADATA_TYPE_TIMESTAMP);
    if (item) {
        // Get file creation time (this would be implementation-specific)
        time_t now;
        time(&now);
        item->timestamp_value = (uint64_t)now;
        
        // Add to base metadata items
        MetadataItem* time_item = (MetadataItem*)malloc(sizeof(MetadataItem));
        if (time_item) {
            strncpy(time_item->name, "creation_time", MAX_METADATA_ITEM_NAME_LENGTH-1);
            time_item->name[MAX_METADATA_ITEM_NAME_LENGTH-1] = '\0';
            time_item->type = METADATA_TYPE_TIMESTAMP;
            time_item->value.timestamp.min_timestamp = now;
            time_item->value.timestamp.max_timestamp = now;
            time_item->value.timestamp.count = 1;
            
            if (base->item_count < 10) {
                base->items[base->item_count++] = time_item;
            }
        }
    }
    
    // Add schema information (could be expanded)
    item = addBasicFileMetadataItem(&ext_metadata->metadata, "schema_version", METADATA_TYPE_NUMERIC);
    if (item) {
        item->numeric_value = 1.0; // Placeholder for schema version
    }
    
    // Process custom metadata if config file provided
    if (config_path) {
        if (progress_callback) {
            progress_callback(95, progress_data); // Report progress at 95%
        }
        
        // Generate custom metadata from the config file
        MetadataGeneratorError custom_error = generate_custom_metadata(ext_metadata, file, config_path);
        if (custom_error != METADATA_GEN_OK) {
            // Error message already set by generate_custom_metadata
            for (uint32_t j = 0; j < ext_metadata->child_count; j++) {
                metadata_release(ext_metadata->child_metadata[j]);
            }
            free(agg_column_metadata);
            free(ext_metadata->base_metadata);
            free(ext_metadata->child_metadata);
            free(ext_metadata);
            return NULL;
        }
        
        // Process each custom metadata item
        for (int i = 0; i < ext_metadata->custom_metadata_count; i++) {
            // Add the custom metadata as a metadata item
            item = addBasicFileMetadataItem(&ext_metadata->metadata, 
                                           ext_metadata->custom_metadata[i].name, 
                                           METADATA_TYPE_CUSTOM);
            if (item) {
                // Store reference to the custom metadata in the item
                // (we can't store the actual data due to the union structure)
                item->numeric_value = (double)i; // Store index as numeric value
            }
        }
    }
    
    // Report 100% progress if we have a callback
    if (progress_callback) {
        progress_callback(100, progress_data);
    }
    
    // Free aggregated column metadata
    free(agg_column_metadata);
    
    // Return the file metadata
    return (Metadata*)ext_metadata;
}

/**
 * Generate metadata for a row group
 * 
 * This function analyzes a row group in a Parquet file and generates metadata for it.
 * 
 * @param file Pointer to ParquetFile structure containing file information
 * @param row_group_id ID of the row group to generate metadata for
 * @param options Options for metadata generation (can be NULL for default options)
 * @param progress_callback Optional callback function to report progress (can be NULL)
 * 
 * @return The generated row group metadata, or NULL on error
 */
Metadata* metadata_generator_generate_row_group_metadata(
    const ParquetFile* file,
    int row_group_id,
    const MetadataGeneratorOptions* options,
    ProgressCallback progress_callback)
{
    // Validate parameters
    if (!file || row_group_id < 0 || row_group_id >= file->row_group_count) {
        set_error("Invalid parameters for row group metadata generation");
        return NULL;
    }
    
    // Get the row group
    const ParquetRowGroup* row_group = &file->row_groups[row_group_id];
    
    // Initialize metadata structure
    Metadata* metadata = (Metadata*)calloc(1, sizeof(Metadata));
    if (!metadata) {
        set_error("Failed to allocate memory for row group metadata");
        return NULL;
    }
    
    // Set file path
    if (file->file_path) {
        metadata->file_path = strdup(file->file_path);
        if (!metadata->file_path) {
            free(metadata);
            set_error("Failed to allocate memory for file path");
            return NULL;
        }
    }
    
    // Initialize basic row group metadata
    char row_group_name[256];
    snprintf(row_group_name, sizeof(row_group_name), "Row Group %d", row_group_id);
    
    // Add basic metadata items
    MetadataItem* rowCountItem = addBasicFileMetadataItem(metadata, "row_count", METADATA_TYPE_NUMERIC);
    if (rowCountItem) {
        rowCountItem->value.numeric.has_numeric_data = true;
        rowCountItem->value.numeric.min_value = (double)row_group->num_rows;
        rowCountItem->value.numeric.max_value = (double)row_group->num_rows;
        rowCountItem->value.numeric.avg_value = (double)row_group->num_rows;
        rowCountItem->value.numeric.mode_value = (double)row_group->num_rows;
        rowCountItem->value.numeric.mode_count = 1;
        rowCountItem->value.numeric.total_count = 1;
        rowCountItem->value.numeric.null_count = 0;
    }
    
    MetadataItem* colCountItem = addBasicFileMetadataItem(metadata, "column_count", METADATA_TYPE_NUMERIC);
    if (colCountItem) {
        colCountItem->value.numeric.has_numeric_data = true;
        colCountItem->value.numeric.min_value = (double)row_group->column_count;
        colCountItem->value.numeric.max_value = (double)row_group->column_count;
        colCountItem->value.numeric.avg_value = (double)row_group->column_count;
        colCountItem->value.numeric.mode_value = (double)row_group->column_count;
        colCountItem->value.numeric.mode_count = 1;
        colCountItem->value.numeric.total_count = 1;
        colCountItem->value.numeric.null_count = 0;
    }
    
    MetadataItem* rowGroupIndexItem = addBasicFileMetadataItem(metadata, "row_group_index", METADATA_TYPE_NUMERIC);
    if (rowGroupIndexItem) {
        rowGroupIndexItem->value.numeric.has_numeric_data = true;
        rowGroupIndexItem->value.numeric.min_value = (double)row_group_id;
        rowGroupIndexItem->value.numeric.max_value = (double)row_group_id;
        rowGroupIndexItem->value.numeric.avg_value = (double)row_group_id;
        rowGroupIndexItem->value.numeric.mode_value = (double)row_group_id;
        rowGroupIndexItem->value.numeric.mode_count = 1;
        rowGroupIndexItem->value.numeric.total_count = 1;
        rowGroupIndexItem->value.numeric.null_count = 0;
    }
    
    // If a progress callback was provided, report progress
    if (progress_callback) {
        progress_callback(0.5f, NULL); // 50% progress
    }
    
    // Return the metadata
    return metadata;
}
