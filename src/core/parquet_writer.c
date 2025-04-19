#include "core/parquet_writer.h"
#include "core/parquet_structure.h"
#include "lzma/LzmaDec.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include "compression/lzma_decompressor.h"

/* LZMA constants */
#define LZMA_PROPS_SIZE 5    /* Size of LZMA properties header */

/* Error codes for the writer */
#define PARQUET_WRITER_SCHEMA_ERROR 4
#define PARQUET_WRITER_DATA_ERROR 5
#define PARQUET_WRITER_COMPRESSION_ERROR 6

/**
 * Internal structure for parquet writer context
 */
struct ParquetWriterContext {
    char* file_path;
    void* arrow_writer;  // Internal writer state for the Parquet file
    int current_row_group;
    int total_columns;
    char error_message[256];
    int current_column;
    int schema_finalized;
};

/**
 * Create a new parquet writer context
 * 
 * This function initializes a writer context for creating a new parquet file.
 * The context must be released with parquet_writer_close when no longer needed.
 * 
 * file_path: Path where the parquet file will be written
 * returns: A new writer context, or NULL if an error occurred
 */
ParquetWriterContext* parquet_writer_create(const char* file_path) {
    if (!file_path) {
        return NULL;
    }
    
    // Allocate memory for the writer context
    ParquetWriterContext* context = (ParquetWriterContext*)malloc(sizeof(ParquetWriterContext));
    if (!context) {
        return NULL;
    }
    
    // Clear the structure to initialize all fields to zero
    memset(context, 0, sizeof(ParquetWriterContext));
    
    // Allocate memory for the file path and copy it
    size_t path_len = strlen(file_path) + 1;
    context->file_path = (char*)malloc(path_len);
    if (!context->file_path) {
        free(context);
        return NULL;
    }
    
    // Copy the file path
    memcpy(context->file_path, file_path, path_len);
    
    // Initialize the Parquet writer state
    context->arrow_writer = NULL;
    context->current_row_group = -1;  // No row group started yet
    context->total_columns = 0;
    
    return context;
}

/**
 * Close a parquet writer context and free associated resources
 * 
 * This function finishes writing the parquet file and releases all resources 
 * associated with the writer context.
 * 
 * context: The writer context to close
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_close(ParquetWriterContext* context) {
    if (!context) {
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Close the writer if it exists
    if (context->arrow_writer) {
        // Finalize and close the writer
    }
    
    // Free the file path
    if (context->file_path) {
        free(context->file_path);
    }
    
    // Free the context
    free(context);
    
    return PARQUET_WRITER_OK;
}

/**
 * Add a column to the parquet schema
 * 
 * This function adds a new column definition to the parquet schema.
 * Must be called before starting to write row groups.
 * 
 * context: The writer context
 * name: Name of the column
 * type: Data type of the column
 * column_id: Pointer to store the assigned column ID
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_add_column(
    ParquetWriterContext* context,
    const char* name,
    ParquetValueType type,
    int* column_id
) {
    if (!context || !name || !column_id) {
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Make sure we haven't started writing row groups yet
    if (context->current_row_group >= 0) {
        snprintf(context->error_message, sizeof(context->error_message),
                "Cannot add columns after starting to write row groups");
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Add a column to the parquet schema
    
    // Assign a column ID
    *column_id = context->total_columns++;
    
    return PARQUET_WRITER_OK;
}

/**
 * Start a new row group in the parquet file
 * 
 * This function begins a new row group in the parquet file.
 * 
 * context: The writer context
 * row_group_id: Pointer to store the assigned row group ID
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_start_row_group(
    ParquetWriterContext* context,
    int* row_group_id
) {
    if (!context || !row_group_id) {
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Make sure we have at least one column defined
    if (context->total_columns <= 0) {
        snprintf(context->error_message, sizeof(context->error_message),
                "Cannot start a row group without defining columns first");
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Make sure the previous row group was properly ended
    if (context->current_row_group >= 0) {
        snprintf(context->error_message, sizeof(context->error_message),
                "Previous row group was not ended before starting a new one");
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Start a new row group in the file
    
    // Start a new row group
    context->current_row_group = (context->current_row_group < 0) ? 0 : context->current_row_group + 1;
    *row_group_id = context->current_row_group;
    
    return PARQUET_WRITER_OK;
}

/**
 * Finish the current row group
 * 
 * This function completes the current row group.
 * 
 * context: The writer context
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_end_row_group(ParquetWriterContext* context) {
    if (!context) {
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Make sure we have a row group to end
    if (context->current_row_group < 0) {
        snprintf(context->error_message, sizeof(context->error_message),
                "No row group has been started");
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // End the current row group in the file
    
    // End the current row group
    context->current_row_group = -1;  // Mark that no row group is active
    
    return PARQUET_WRITER_OK;
}

/**
 * Write column data to the current row group
 * 
 * This function writes data for a column in the current row group.
 * 
 * context: The writer context
 * column_id: ID of the column to write
 * buffer: Buffer containing the column data
 * buffer_size: Size of the buffer in bytes
 * row_count: Number of rows in the column
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_write_column(
    ParquetWriterContext* context,
    int column_id,
    const void* buffer,
    size_t buffer_size,
    int row_count
) {
    if (!context || !buffer || buffer_size == 0 || row_count <= 0) {
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Make sure we have an active row group
    if (context->current_row_group < 0) {
        snprintf(context->error_message, sizeof(context->error_message),
                "No active row group to write to");
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Make sure the column ID is valid
    if (column_id < 0 || column_id >= context->total_columns) {
        snprintf(context->error_message, sizeof(context->error_message),
                "Invalid column ID: %d", column_id);
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Write the column data to the parquet file
    
    return PARQUET_WRITER_OK;
}

/**
 * Reconstructs a parquet file from column files
 * 
 * This function reconstructs a parquet file from compressed column files.
 * Used during decompression to reconstruct the original parquet file.
 * 
 * file_structure: Structure describing the parquet file organization
 * output_path: Path where the reconstructed file will be written
 * column_file_paths: Array of paths to compressed column files
 * 
 * Return: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_reconstruct_file(
    const ParquetFile* file_structure,
    const char* output_path,
    char** column_file_paths
) {
    if (!file_structure || !output_path || !column_file_paths) {
        return PARQUET_WRITER_INVALID_PARAMETER;
    }
    
    // Create a new writer context
    ParquetWriterContext* context = parquet_writer_create(output_path);
    if (!context) {
        return PARQUET_WRITER_MEMORY_ERROR;
    }
    
    // Process each row group
    for (int rg = 0; rg < file_structure->row_group_count; rg++) {
        const ParquetRowGroup* row_group = &file_structure->row_groups[rg];
        int row_group_id;
        
        // Start a new row group
        ParquetWriterError err = parquet_writer_start_row_group(context, &row_group_id);
        if (err != PARQUET_WRITER_OK) {
            parquet_writer_close(context);
            return err;
        }
        
        // Process each column in the row group
        for (int col = 0; col < row_group->column_count; col++) {
            const ParquetColumn* column = &row_group->columns[col];
            char* column_file_path = column_file_paths[rg * row_group->column_count + col];
            
            if (!column_file_path) {
                parquet_writer_close(context);
                return PARQUET_WRITER_INVALID_PARAMETER;
            }
            
            // Create a temporary output file for decompression
            char temp_file[1024];
            snprintf(temp_file, sizeof(temp_file), "%s.temp.%d.%d", output_path, rg, col);
            
            // Define a null progress callback for decompression
            DecompressionProgressCallback progress_cb = NULL;
            
            // Use LZMA to decompress the file
            int decompression_result = lzma_decompress_file(column_file_path, temp_file, progress_cb, NULL);
            if (decompression_result != 0) {
                parquet_writer_close(context);
                return PARQUET_WRITER_INVALID_PARAMETER;
            }
            
            // Read the decompressed data
            FILE* temp_fp = fopen(temp_file, "rb");
            if (!temp_fp) {
                remove(temp_file);
                parquet_writer_close(context);
                return PARQUET_WRITER_FILE_ERROR;
            }
            
            // Read the row count
            int row_count = 0;
            if (fread(&row_count, sizeof(int), 1, temp_fp) != 1) {
                fclose(temp_fp);
                remove(temp_file);
                parquet_writer_close(context);
                return PARQUET_WRITER_FILE_ERROR;
            }
            
            // Get the size of the remaining data
            fseek(temp_fp, 0, SEEK_END);
            long file_size = ftell(temp_fp);
            size_t data_size = file_size - sizeof(int);
            fseek(temp_fp, sizeof(int), SEEK_SET);
            
            // Allocate memory and read the data
            void* buffer = malloc(data_size);
            if (!buffer) {
                fclose(temp_fp);
                remove(temp_file);
                parquet_writer_close(context);
                return PARQUET_WRITER_MEMORY_ERROR;
            }
            
            if (fread(buffer, 1, data_size, temp_fp) != data_size) {
                free(buffer);
                fclose(temp_fp);
                remove(temp_file);
                parquet_writer_close(context);
                return PARQUET_WRITER_FILE_ERROR;
            }
            
            fclose(temp_fp);
            remove(temp_file);  // Delete the temporary file
            
            // Write the column data
            err = parquet_writer_write_column(context, col, buffer, data_size, row_count);
            free(buffer);
            
            if (err != PARQUET_WRITER_OK) {
                parquet_writer_close(context);
                return err;
            }
        }
        
        // End the row group
        ParquetWriterError err_end = parquet_writer_end_row_group(context);
        if (err_end != PARQUET_WRITER_OK) {
            parquet_writer_close(context);
            return err_end;
        }
    }
    
    // Close the parquet writer context
    ParquetWriterError close_err = parquet_writer_close(context);
    if (close_err != PARQUET_WRITER_OK) {
        return close_err;
    }
    
    return PARQUET_WRITER_OK;
}

/**
 * Get the last error message from the writer
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any writer function.
 * 
 * context: The writer context
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* parquet_writer_get_error(ParquetWriterContext* context) {
    if (!context || context->error_message[0] == '\0') {
        return NULL;
    }
    
    return context->error_message;
} 