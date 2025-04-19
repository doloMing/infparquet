/**
 * arrow_adapter.cpp
 * 
 * This file implements adapters for the Arrow library to work with the InfParquet framework.
 */

#include "core/arrow_adapter.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <cstring>
#include <cstdarg>  // For va_start and va_end
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/buffer.h"
#include "arrow/csv/api.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"

// Static error message buffer
static char s_last_error[1024] = {0};

// Set the last error message
static void set_error(const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(s_last_error, sizeof(s_last_error), format, args);
    va_end(args);
}

// Convert Arrow type to ParquetValueType
static ParquetValueType convert_arrow_type(const std::shared_ptr<arrow::DataType>& type) {
    if (!type) {
        return PARQUET_BYTE_ARRAY; // Default for safety
    }
    
    switch (type->id()) {
        case arrow::Type::BOOL:
            return PARQUET_BOOLEAN;
        case arrow::Type::INT32:
            return PARQUET_INT32;
        case arrow::Type::INT64:
            return PARQUET_INT64;
        case arrow::Type::FLOAT:
            return PARQUET_FLOAT;
        case arrow::Type::DOUBLE:
            return PARQUET_DOUBLE;
        case arrow::Type::STRING:
            return PARQUET_STRING;
        case arrow::Type::BINARY:
            return PARQUET_BINARY;
        case arrow::Type::TIMESTAMP:
            return PARQUET_TIMESTAMP;
        case arrow::Type::FIXED_SIZE_BINARY:
            return PARQUET_FIXED_LEN_BYTE_ARRAY;
        default:
            return PARQUET_BYTE_ARRAY;
    }
}

/**
 * Read the structure of a Parquet file using Arrow
 */
int arrow_read_parquet_structure(const char* file_path, ParquetFile* parquet_file) {
    if (!file_path || !parquet_file) {
        set_error("Invalid parameters");
        return -1;
    }
    
    try {
        // Open the file
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(
            infile, 
            arrow::io::ReadableFile::Open(file_path, arrow::default_memory_pool())
        );
        
        // Create a ParquetFileReader
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader = 
            parquet::ParquetFileReader::Open(infile);
        
        // Get the file metadata
        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
        
        // Initialize the ParquetFile structure
        parquet_file->file_path = strdup(file_path);
        if (!parquet_file->file_path) {
            set_error("Memory allocation error for file path");
            return -1;
        }
        
        parquet_file->total_rows = 0;
        parquet_file->row_group_count = file_metadata->num_row_groups();
        
        // Allocate memory for row groups
        parquet_file->row_groups = (ParquetRowGroup*)malloc(
            parquet_file->row_group_count * sizeof(ParquetRowGroup)
        );
        
        if (!parquet_file->row_groups) {
            free(parquet_file->file_path);
            parquet_file->file_path = NULL;
            set_error("Failed to allocate memory for row groups");
            return -1;
        }
        
        // Initialize row groups to zero
        memset(parquet_file->row_groups, 0, 
               parquet_file->row_group_count * sizeof(ParquetRowGroup));
        
        // Process each row group
        for (int rg = 0; rg < parquet_file->row_group_count; rg++) {
            ParquetRowGroup* row_group = &parquet_file->row_groups[rg];
            row_group->row_group_index = rg;
            
            // Get row group reader
            std::shared_ptr<parquet::RowGroupReader> row_group_reader = 
                parquet_reader->RowGroup(rg);
            
            // Get row group metadata
            std::shared_ptr<parquet::RowGroupMetaData> row_group_metadata = 
                file_metadata->RowGroup(rg);
            
            // Set row group properties
            row_group->num_rows = row_group_metadata->num_rows();
            row_group->column_count = row_group_metadata->num_columns();
            row_group->metadata_path = NULL;
            
            // Update total rows
            parquet_file->total_rows += row_group->num_rows;
            
            // Allocate memory for columns
            row_group->columns = (ParquetColumn*)malloc(
                row_group->column_count * sizeof(ParquetColumn)
            );
            
            if (!row_group->columns) {
                set_error("Failed to allocate memory for columns in row group %d", rg);
                
                // Clean up previously allocated row groups
                for (int i = 0; i < rg; i++) {
                    if (parquet_file->row_groups[i].columns) {
                        free(parquet_file->row_groups[i].columns);
                    }
                }
                
                free(parquet_file->row_groups);
                free(parquet_file->file_path);
                parquet_file->file_path = NULL;
                parquet_file->row_groups = NULL;
                return -1;
            }
            
            // Initialize columns to zero
            memset(row_group->columns, 0, 
                   row_group->column_count * sizeof(ParquetColumn));
            
            // Process each column
            for (int col = 0; col < row_group->column_count; col++) {
                ParquetColumn* column = &row_group->columns[col];
                column->column_index = col;
                
                // Get column metadata
                std::shared_ptr<parquet::ColumnChunkMetaData> column_metadata = 
                    row_group_metadata->ColumnChunk(col);
                
                // Get column schema
                auto schema = file_metadata->schema();
                const parquet::ColumnDescriptor* column_desc = schema->Column(col);
                
                // Set column properties
                const char* col_name = schema->Column(col)->name().c_str();
                strncpy(column->name, col_name, MAX_COLUMN_NAME_LENGTH - 1);
                column->name[MAX_COLUMN_NAME_LENGTH - 1] = '\0';
                
                // Convert Arrow type to ParquetValueType
                parquet::Type::type physical_type = column_desc->physical_type();
                
                // Handle the type conversion
                switch (physical_type) {
                    case parquet::Type::BOOLEAN:
                        column->type = PARQUET_BOOLEAN;
                        break;
                    case parquet::Type::INT32:
                        column->type = PARQUET_INT32;
                        break;
                    case parquet::Type::INT64:
                        column->type = PARQUET_INT64;
                        break;
                    case parquet::Type::FLOAT:
                        column->type = PARQUET_FLOAT;
                        break;
                    case parquet::Type::DOUBLE:
                        column->type = PARQUET_DOUBLE;
                        break;
                    case parquet::Type::BYTE_ARRAY:
                        // Determine if this is a string or binary based on the logical type
                        if (column_desc->logical_type() && column_desc->logical_type()->is_string()) {
                            column->type = PARQUET_STRING;
                        } else {
                            column->type = PARQUET_BYTE_ARRAY;
                        }
                        break;
                    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
                        column->type = PARQUET_FIXED_LEN_BYTE_ARRAY;
                        // Store the fixed length in the type-specific metadata
                        column->fixed_len_byte_array_size = column_desc->type_length();
                        break;
                    default:
                        column->type = PARQUET_BYTE_ARRAY; // Default for unknown types
                        break;
                }
                
                column->total_compressed_size = column_metadata->total_compressed_size();
                column->total_uncompressed_size = column_metadata->total_uncompressed_size();
                column->total_values = column_metadata->num_values();
                column->compression_path = NULL;
                column->column_data = NULL;
                
                // Get page information if possible
                column->page_count = 0;
                column->pages = NULL;
                
                // If possible, read page information
                // Note: This requires an extension to the Arrow API not available in all versions
                // For now, we'll leave page information empty, as it's typically not needed for high-level operations
            }
        }
        
        return 0;
    } catch (const std::exception& e) {
        set_error("Arrow exception: %s", e.what());
        
        // Clean up any partially allocated resources
        if (parquet_file->file_path) {
            free(parquet_file->file_path);
            parquet_file->file_path = NULL;
        }
        
        if (parquet_file->row_groups) {
            for (uint32_t i = 0; i < parquet_file->row_group_count; i++) {
                if (parquet_file->row_groups[i].columns) {
                    free(parquet_file->row_groups[i].columns);
                }
            }
            free(parquet_file->row_groups);
            parquet_file->row_groups = NULL;
        }
        
        parquet_file->row_group_count = 0;
        parquet_file->total_rows = 0;
        
        return -1;
    }
}

/**
 * Read column data from a Parquet file using Arrow
 */
int arrow_read_column_data(const char* file_path, int row_group_id, int column_id, 
                           void** buffer, size_t* buffer_size) {
    if (!file_path || !buffer || !buffer_size) {
        set_error("Invalid parameters");
        return -1;
    }
    
    // Initialize output parameters
    *buffer = NULL;
    *buffer_size = 0;
    
    try {
        // Open the file
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(
            infile, 
            arrow::io::ReadableFile::Open(file_path, arrow::default_memory_pool())
        );
        
        // Create a ParquetFileReader
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader = 
            parquet::ParquetFileReader::Open(infile);
        
        // Get the file metadata
        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
        
        // Check row group and column IDs
        if (row_group_id < 0 || row_group_id >= file_metadata->num_row_groups()) {
            set_error("Invalid row group ID: %d", row_group_id);
            return -1;
        }
        
        if (column_id < 0 || column_id >= file_metadata->schema()->num_columns()) {
            set_error("Invalid column ID: %d", column_id);
            return -1;
        }
        
        // Get information about the column
        auto schema = file_metadata->schema();
        auto column_schema = schema->Column(column_id);
        auto physical_type = column_schema->physical_type();
        auto logical_type = column_schema->logical_type();
        
        // Create Arrow file reader
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        PARQUET_THROW_NOT_OK(
            parquet::arrow::FileReader::Make(arrow::default_memory_pool(), 
                                           parquet::ParquetFileReader::Open(infile),
                                           &arrow_reader)
        );
        
        // Read the specified column as an Arrow table
        std::shared_ptr<arrow::Table> table;
        std::vector<int> column_indices = {column_id};
        PARQUET_THROW_NOT_OK(
            arrow_reader->ReadRowGroup(row_group_id, column_indices, &table)
        );
        
        if (!table || table->num_columns() == 0) {
            set_error("Failed to read column data");
            return -1;
        }
        
        // Get the column chunk
        std::shared_ptr<arrow::ChunkedArray> column_chunk = table->column(0);
        if (!column_chunk || column_chunk->num_chunks() == 0) {
            set_error("Column data is empty");
            return -1;
        }
        
        // Different data types require different handling
        switch (physical_type) {
            case parquet::Type::BOOLEAN: {
                // Boolean values are bit-packed, we'll convert to a byte array for simplicity
                size_t num_values = column_chunk->length();
                *buffer_size = num_values * sizeof(bool);
                *buffer = malloc(*buffer_size);
                if (!*buffer) {
                    set_error("Failed to allocate memory for boolean data");
                    return -1;
                }
                
                bool* bool_buffer = static_cast<bool*>(*buffer);
                size_t offset = 0;
                
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(chunk);
                    for (int64_t i = 0; i < bool_array->length(); i++) {
                        if (offset < num_values) {
                            bool_buffer[offset++] = bool_array->IsNull(i) ? false : bool_array->Value(i);
                        }
                    }
                }
                break;
            }
            case parquet::Type::INT32: {
                size_t num_values = column_chunk->length();
                *buffer_size = num_values * sizeof(int32_t);
                *buffer = malloc(*buffer_size);
                if (!*buffer) {
                    set_error("Failed to allocate memory for int32 data");
                    return -1;
                }
                
                int32_t* int_buffer = static_cast<int32_t*>(*buffer);
                size_t offset = 0;
                
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto int_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
                    for (int64_t i = 0; i < int_array->length(); i++) {
                        if (offset < num_values) {
                            int_buffer[offset++] = int_array->IsNull(i) ? 0 : int_array->Value(i);
                        }
                    }
                }
                break;
            }
            case parquet::Type::INT64: {
                size_t num_values = column_chunk->length();
                *buffer_size = num_values * sizeof(int64_t);
                *buffer = malloc(*buffer_size);
                if (!*buffer) {
                    set_error("Failed to allocate memory for int64 data");
                    return -1;
                }
                
                int64_t* int_buffer = static_cast<int64_t*>(*buffer);
                size_t offset = 0;
                
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto int_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
                    for (int64_t i = 0; i < int_array->length(); i++) {
                        if (offset < num_values) {
                            int_buffer[offset++] = int_array->IsNull(i) ? 0 : int_array->Value(i);
                        }
                    }
                }
                break;
            }
            case parquet::Type::FLOAT: {
                size_t num_values = column_chunk->length();
                *buffer_size = num_values * sizeof(float);
                *buffer = malloc(*buffer_size);
                if (!*buffer) {
                    set_error("Failed to allocate memory for float data");
                    return -1;
                }
                
                float* float_buffer = static_cast<float*>(*buffer);
                size_t offset = 0;
                
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto float_array = std::static_pointer_cast<arrow::FloatArray>(chunk);
                    for (int64_t i = 0; i < float_array->length(); i++) {
                        if (offset < num_values) {
                            float_buffer[offset++] = float_array->IsNull(i) ? 0.0f : float_array->Value(i);
                        }
                    }
                }
                break;
            }
            case parquet::Type::DOUBLE: {
                size_t num_values = column_chunk->length();
                *buffer_size = num_values * sizeof(double);
                *buffer = malloc(*buffer_size);
                if (!*buffer) {
                    set_error("Failed to allocate memory for double data");
                    return -1;
                }
                
                double* double_buffer = static_cast<double*>(*buffer);
                size_t offset = 0;
                
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto double_array = std::static_pointer_cast<arrow::DoubleArray>(chunk);
                    for (int64_t i = 0; i < double_array->length(); i++) {
                        if (offset < num_values) {
                            double_buffer[offset++] = double_array->IsNull(i) ? 0.0 : double_array->Value(i);
                        }
                    }
                }
                break;
            }
            case parquet::Type::BYTE_ARRAY: {
                // Handle string or binary data - we'll format as length-prefixed data
                // Format: uint32_t length followed by the bytes
                size_t total_size = 0;
                size_t num_values = column_chunk->length();
                
                // First pass to calculate total size
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(chunk);
                    for (int64_t i = 0; i < binary_array->length(); i++) {
                        if (binary_array->IsNull(i)) {
                            total_size += sizeof(uint32_t); // For length field (0)
                        } else {
                            int32_t length = binary_array->value_length(i);
                            total_size += sizeof(uint32_t) + length; // Length field + data
                        }
                    }
                }
                
                *buffer_size = total_size;
                *buffer = malloc(*buffer_size);
                if (!*buffer) {
                    set_error("Failed to allocate memory for binary data");
                    return -1;
                }
                
                // Second pass to copy data
                uint8_t* data_ptr = static_cast<uint8_t*>(*buffer);
                size_t offset = 0;
                
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(chunk);
                    for (int64_t i = 0; i < binary_array->length(); i++) {
                        if (binary_array->IsNull(i)) {
                            // Write length 0 for NULL values
                            uint32_t length = 0;
                            memcpy(data_ptr + offset, &length, sizeof(uint32_t));
                            offset += sizeof(uint32_t);
                        } else {
                            // Get the binary data - use View method to get string view
                            int32_t length = binary_array->value_length(i);
                            const uint8_t* value = nullptr;
                            if (!binary_array->IsNull(i)) {
                                auto buffer = binary_array->value_data();
                                if (buffer) {
                                    value = buffer->data() + binary_array->value_offset(i);
                                }
                            }
                            
                            // Write length
                            memcpy(data_ptr + offset, &length, sizeof(uint32_t));
                            offset += sizeof(uint32_t);
                            
                            // Write data
                            if (value && length > 0) {
                                memcpy(data_ptr + offset, value, length);
                            }
                            offset += length;
                        }
                    }
                }
                break;
            }
            case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                // Get the fixed length from schema
                int fixed_len = column_schema->type_length();
                size_t num_values = column_chunk->length();
                *buffer_size = num_values * fixed_len;
                *buffer = malloc(*buffer_size);
                if (!*buffer) {
                    set_error("Failed to allocate memory for fixed-length binary data");
                    return -1;
                }
                
                uint8_t* data_ptr = static_cast<uint8_t*>(*buffer);
                size_t offset = 0;
                
                for (int chunk_idx = 0; chunk_idx < column_chunk->num_chunks(); chunk_idx++) {
                    std::shared_ptr<arrow::Array> chunk = column_chunk->chunk(chunk_idx);
                    auto fixed_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(chunk);
                    for (int64_t i = 0; i < fixed_array->length(); i++) {
                        if (fixed_array->IsNull(i)) {
                            // Zero-fill for NULL values
                            memset(data_ptr + offset, 0, fixed_len);
                        } else {
                            // Copy fixed-length data
                            memcpy(data_ptr + offset, fixed_array->Value(i), fixed_len);
                        }
                        offset += fixed_len;
                    }
                }
                break;
            }
            default: {
                set_error("Unsupported column data type");
                return -1;
            }
        }
        
        return 0;
    } catch (const std::exception& e) {
        set_error("Arrow exception: %s", e.what());
        if (*buffer) {
            free(*buffer);
            *buffer = NULL;
            *buffer_size = 0;
        }
        return -1;
    }
}

/**
 * Create a Parquet file from column data using Arrow
 * 
 * file_path: Path where the Parquet file will be written
 * column_data: Array of column data buffers
 * column_sizes: Array of column data sizes
 * schema: Array of column types
 * fixed_len_sizes: Array of fixed length sizes for FIXED_LEN_BYTE_ARRAY columns (can be NULL if no such columns)
 * column_count: Number of columns
 * row_count: Number of rows
 * 
 * Returns: 0 on success, non-zero error code on failure
 */
int arrow_create_parquet_file(const char* file_path, void** column_data, size_t* column_sizes,
                              ParquetValueType* schema, int* fixed_len_sizes, int column_count, int64_t row_count) {
    if (!file_path || !column_data || !column_sizes || !schema || column_count <= 0 || row_count <= 0) {
        set_error("Invalid parameters");
        return -1;
    }
    
    try {
        // Create a schema based on the provided types
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (int i = 0; i < column_count; i++) {
            std::shared_ptr<arrow::DataType> data_type;
            
            // Convert ParquetValueType to Arrow DataType
            switch (schema[i]) {
                case PARQUET_BOOLEAN:
                    data_type = arrow::boolean();
                    break;
                case PARQUET_INT32:
                    data_type = arrow::int32();
                    break;
                case PARQUET_INT64:
                    data_type = arrow::int64();
                    break;
                case PARQUET_FLOAT:
                    data_type = arrow::float32();
                    break;
                case PARQUET_DOUBLE:
                    data_type = arrow::float64();
                    break;
                case PARQUET_STRING:
                    data_type = arrow::utf8();
                    break;
                case PARQUET_BINARY:
                    data_type = arrow::binary();
                    break;
                case PARQUET_TIMESTAMP:
                    data_type = arrow::timestamp(arrow::TimeUnit::MICRO);
                    break;
                case PARQUET_FIXED_LEN_BYTE_ARRAY:
                    if (!fixed_len_sizes) {
                        set_error("Fixed length sizes array is required for FIXED_LEN_BYTE_ARRAY columns");
                        return -1;
                    }
                    data_type = arrow::fixed_size_binary(fixed_len_sizes[i]);
                    break;
                case PARQUET_INT96:
                    // INT96 is always 12 bytes (96 bits)
                    data_type = arrow::fixed_size_binary(12);
                    break;
                default:
                    data_type = arrow::binary();
                    break;
            }
            
            fields.push_back(arrow::field("col_" + std::to_string(i), data_type));
        }
        
        auto schema_arrow = arrow::schema(fields);
        
        // Create arrays from the provided data
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (int i = 0; i < column_count; i++) {
            std::unique_ptr<arrow::ArrayBuilder> builder;
            
            switch (schema[i]) {
                case PARQUET_BOOLEAN:
                    builder.reset(new arrow::BooleanBuilder());
                    break;
                case PARQUET_INT32:
                    builder.reset(new arrow::Int32Builder());
                    break;
                case PARQUET_INT64:
                    builder.reset(new arrow::Int64Builder());
                    break;
                case PARQUET_FLOAT:
                    builder.reset(new arrow::FloatBuilder());
                    break;
                case PARQUET_DOUBLE:
                    builder.reset(new arrow::DoubleBuilder());
                    break;
                case PARQUET_STRING:
                    builder.reset(new arrow::StringBuilder());
                    break;
                case PARQUET_BINARY:
                    builder.reset(new arrow::BinaryBuilder());
                    break;
                case PARQUET_TIMESTAMP:
                    builder.reset(new arrow::TimestampBuilder(arrow::timestamp(arrow::TimeUnit::MICRO),
                                                             arrow::default_memory_pool()));
                    break;
                case PARQUET_FIXED_LEN_BYTE_ARRAY:
                    if (!fixed_len_sizes) {
                        set_error("Fixed length sizes array is required for FIXED_LEN_BYTE_ARRAY columns");
                        return -1;
                    }
                    builder.reset(new arrow::FixedSizeBinaryBuilder(
                        arrow::fixed_size_binary(fixed_len_sizes[i]), 
                        arrow::default_memory_pool()));
                    break;
                case PARQUET_INT96:
                    // INT96 is always 12 bytes (96 bits)
                    builder.reset(new arrow::FixedSizeBinaryBuilder(
                        arrow::fixed_size_binary(12), 
                        arrow::default_memory_pool()));
                    break;
                default:
                    builder.reset(new arrow::BinaryBuilder());
                    break;
            }
            
            if (!builder) {
                set_error("Failed to create array builder");
                return -1;
            }
            
            // Append the data to the builder
            // Provide complete conversion implementation for each data type
            if (column_data[i] && column_sizes[i] > 0) {
                switch (schema[i]) {
                    case PARQUET_BOOLEAN: {
                        auto bool_builder = static_cast<arrow::BooleanBuilder*>(builder.get());
                        const bool* bool_data = static_cast<const bool*>(column_data[i]);
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(bool_builder->Append(bool_data[j]));
                        }
                        break;
                    }
                    case PARQUET_INT32: {
                        auto int_builder = static_cast<arrow::Int32Builder*>(builder.get());
                        const int32_t* int_data = static_cast<const int32_t*>(column_data[i]);
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(int_builder->Append(int_data[j]));
                        }
                        break;
                    }
                    case PARQUET_INT64: {
                        auto int_builder = static_cast<arrow::Int64Builder*>(builder.get());
                        const int64_t* int_data = static_cast<const int64_t*>(column_data[i]);
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(int_builder->Append(int_data[j]));
                        }
                        break;
                    }
                    case PARQUET_FLOAT: {
                        auto float_builder = static_cast<arrow::FloatBuilder*>(builder.get());
                        const float* float_data = static_cast<const float*>(column_data[i]);
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(float_builder->Append(float_data[j]));
                        }
                        break;
                    }
                    case PARQUET_DOUBLE: {
                        auto double_builder = static_cast<arrow::DoubleBuilder*>(builder.get());
                        const double* double_data = static_cast<const double*>(column_data[i]);
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(double_builder->Append(double_data[j]));
                        }
                        break;
                    }
                    case PARQUET_STRING: {
                        auto string_builder = static_cast<arrow::StringBuilder*>(builder.get());
                        // String data needs special handling, as they have variable length
                        // Assuming the format is: length (uint32_t) + string data + length + string data...
                        const uint8_t* data = static_cast<const uint8_t*>(column_data[i]);
                        size_t offset = 0;
                        
                        for (int64_t j = 0; j < row_count && offset < column_sizes[i]; j++) {
                            uint32_t length = 0;
                            // Get the string length
                            if (offset + sizeof(uint32_t) <= column_sizes[i]) {
                                memcpy(&length, data + offset, sizeof(uint32_t));
                                offset += sizeof(uint32_t);
                            } else {
                                // Data format error
                                PARQUET_THROW_NOT_OK(string_builder->AppendNull());
                                continue;
                            }
                            
                            // Check if there is enough data
                            if (offset + length <= column_sizes[i]) {
                                // Add the string value
                                PARQUET_THROW_NOT_OK(string_builder->Append(
                                    reinterpret_cast<const char*>(data + offset), length));
                                offset += length;
                            } else {
                                // String length exceeds buffer
                                PARQUET_THROW_NOT_OK(string_builder->AppendNull());
                            }
                        }
                        break;
                    }
                    case PARQUET_BINARY: {
                        auto binary_builder = static_cast<arrow::BinaryBuilder*>(builder.get());
                        // Binary data is similar to strings
                        const uint8_t* data = static_cast<const uint8_t*>(column_data[i]);
                        size_t offset = 0;
                        
                        for (int64_t j = 0; j < row_count && offset < column_sizes[i]; j++) {
                            uint32_t length = 0;
                            if (offset + sizeof(uint32_t) <= column_sizes[i]) {
                                memcpy(&length, data + offset, sizeof(uint32_t));
                                offset += sizeof(uint32_t);
                            } else {
                                PARQUET_THROW_NOT_OK(binary_builder->AppendNull());
                                continue;
                            }
                            
                            if (offset + length <= column_sizes[i]) {
                                PARQUET_THROW_NOT_OK(binary_builder->Append(data + offset, length));
                                offset += length;
                            } else {
                                PARQUET_THROW_NOT_OK(binary_builder->AppendNull());
                            }
                        }
                        break;
                    }
                    case PARQUET_TIMESTAMP: {
                        auto ts_builder = static_cast<arrow::TimestampBuilder*>(builder.get());
                        const int64_t* ts_data = static_cast<const int64_t*>(column_data[i]);
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(ts_builder->Append(ts_data[j]));
                        }
                        break;
                    }
                    case PARQUET_INT96: {
                        // INT96 is typically used for old-style timestamp formats in Parquet
                        // Since there is no direct INT96 Builder, we use FixedSizeBinaryBuilder
                        auto fixed_builder = static_cast<arrow::FixedSizeBinaryBuilder*>(builder.get());
                        const uint8_t* data = static_cast<const uint8_t*>(column_data[i]);
                        
                        // INT96 is always 12 bytes (96 bits)
                        const int fixed_len = 12;
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(fixed_builder->Append(data + j * fixed_len));
                        }
                        break;
                    }
                    case PARQUET_FIXED_LEN_BYTE_ARRAY: {
                        // Get the actual fixed length from the provided array
                        const int fixed_len = fixed_len_sizes[i];
                        if (fixed_len <= 0) {
                            set_error("Invalid fixed length size for column %d: %d", i, fixed_len);
                            return -1;
                        }
                        
                        auto fixed_builder = static_cast<arrow::FixedSizeBinaryBuilder*>(builder.get());
                        const uint8_t* data = static_cast<const uint8_t*>(column_data[i]);
                        
                        for (int64_t j = 0; j < row_count; j++) {
                            PARQUET_THROW_NOT_OK(fixed_builder->Append(data + j * fixed_len));
                        }
                        break;
                    }
                    default:
                        set_error("Unsupported data type for column %d", i);
                        return -1;
                }
            }
            
            // Build the array
            std::shared_ptr<arrow::Array> array;
            PARQUET_THROW_NOT_OK(builder->Finish(&array));
            arrays.push_back(array);
        }
        
        // Create a table from the arrays
        auto table = arrow::Table::Make(schema_arrow, arrays);
        
        // Open the output file
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_ASSIGN_OR_THROW(
            outfile, 
            arrow::io::FileOutputStream::Open(file_path)
        );
        
        // Create Parquet writer properties
        auto builder = parquet::WriterProperties::Builder();
        builder.compression(parquet::Compression::UNCOMPRESSED);
        std::shared_ptr<parquet::WriterProperties> props = builder.build();
        
        // Write the table to the Parquet file
        PARQUET_THROW_NOT_OK(
            parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, row_count, props)
        );
        
        return 0;
    } catch (const std::exception& e) {
        set_error("Arrow exception: %s", e.what());
        return -1;
    }
}

/**
 * Get the last error message from Arrow operations
 */
const char* arrow_get_last_error() {
    return s_last_error[0] != '\0' ? s_last_error : NULL;
} 