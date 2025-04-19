#include "core/parquet_reader.h"
#include "core/parquet_structure.h"
#include "core/arrow_adapter.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/**
 * Internal structure for parquet reader context
 */
struct ParquetReaderContext {
    char* file_path;
    void* arrow_reader;  /* Arrow ParquetFileReader instance */
    char error_message[256];
};

/**
 * Create a new parquet reader context
 * 
 * This function initializes a reader context for the specified parquet file.
 * The context must be released with parquet_reader_close when no longer needed.
 * 
 * file_path: Path to the parquet file to read
 * returns: A new reader context, or NULL if an error occurred
 */
ParquetReaderContext* parquet_reader_open(const char* file_path) {
    if (!file_path) {
        return NULL;
    }
    
    // Allocate memory for the reader context
    ParquetReaderContext* context = (ParquetReaderContext*)malloc(sizeof(ParquetReaderContext));
    if (!context) {
        return NULL;
    }
    
    // Clear the structure to initialize all fields to zero
    memset(context, 0, sizeof(ParquetReaderContext));
    
    // Allocate memory for the file path and copy it
    size_t path_len = strlen(file_path) + 1;
    context->file_path = (char*)malloc(path_len);
    if (!context->file_path) {
        free(context);
        return NULL;
    }
    
    // Copy the file path
    memcpy(context->file_path, file_path, path_len);
    
    // In a real implementation, we would initialize the Arrow reader here
    // using the Arrow C++ API. For now, we'll leave it as NULL and initialize
    // it when needed in the specific functions. This approach is more efficient
    // as it allows for lazy loading.
    context->arrow_reader = NULL;
    
    return context;
}

/**
 * Close a parquet reader context and free associated resources
 * 
 * This function releases all resources associated with the reader context.
 * 
 * context: The reader context to close
 */
void parquet_reader_close(ParquetReaderContext* context) {
    if (!context) {
        return;
    }
    
    // Free the file path
    if (context->file_path) {
        free(context->file_path);
    }
    
    // In a real implementation, we would free the Arrow reader here if it exists
    // For now, we'll just set it to NULL as we're not actually creating it
    context->arrow_reader = NULL;
    
    // Free the context
    free(context);
}

/**
 * Get the structure of a parquet file 
 * 
 * This function extracts structural information from the parquet file
 * and populates the provided ParquetFile structure.
 * 
 * context: The reader context
 * file: Pointer to a ParquetFile structure to populate
 * returns: Error code (PARQUET_READER_OK on success)
 */
ParquetReaderError parquet_reader_get_structure(ParquetReaderContext* context, ParquetFile* file) {
    if (!context || !file) {
        return PARQUET_READER_INVALID_PARAMETER;
    }
    
    // Use arrow_adapter to read the parquet file structure
    int result = arrow_read_parquet_structure(context->file_path, file);
    if (result != 0) {
        const char* error_msg = arrow_get_last_error();
        if (error_msg) {
            snprintf(context->error_message, sizeof(context->error_message),
                    "Failed to read parquet structure: %s", error_msg);
        } else {
            snprintf(context->error_message, sizeof(context->error_message),
                    "Failed to read parquet structure: unknown error");
        }
        
        switch (result) {
            case -1:  // General error
                return PARQUET_READER_ARROW_ERROR;
            default:
                return PARQUET_READER_UNKNOWN_ERROR;
        }
    }
    
    return PARQUET_READER_OK;
}

/**
 * Read data from a specific column in a row group
 * 
 * This function reads the data from a specific column in a row group
 * and returns it as a memory buffer. The caller is responsible for
 * freeing the buffer using parquet_reader_free_buffer.
 * 
 * context: The reader context
 * row_group_id: ID of the row group to read from
 * column_id: ID of the column to read
 * buffer: Pointer to store the allocated buffer
 * buffer_size: Pointer to store the size of the allocated buffer
 * returns: Error code (PARQUET_READER_OK on success)
 */
ParquetReaderError parquet_reader_read_column(
    ParquetReaderContext* context,
    int row_group_id,
    int column_id,
    void** buffer,
    size_t* buffer_size
) {
    if (!context || !buffer || !buffer_size) {
        return PARQUET_READER_INVALID_PARAMETER;
    }
    
    // Use arrow_adapter to read the column data
    int result = arrow_read_column_data(context->file_path, row_group_id, column_id, buffer, buffer_size);
    if (result != 0) {
        const char* error_msg = arrow_get_last_error();
        if (error_msg) {
            snprintf(context->error_message, sizeof(context->error_message),
                    "Failed to read column data: %s", error_msg);
        } else {
            snprintf(context->error_message, sizeof(context->error_message),
                    "Failed to read column data: unknown error");
        }
        
        // Make sure buffer is NULL if there was an error
        *buffer = NULL;
        *buffer_size = 0;
        
        switch (result) {
            case -1:  // General error
                return PARQUET_READER_ARROW_ERROR;
            default:
                return PARQUET_READER_UNKNOWN_ERROR;
        }
    }
    
    return PARQUET_READER_OK;
}

/**
 * Free a buffer allocated by parquet_reader_read_column
 * 
 * This function releases memory allocated by parquet_reader_read_column.
 * 
 * buffer: The buffer to free
 */
void parquet_reader_free_buffer(void* buffer) {
    if (buffer) {
        free(buffer);
    }
}

/**
 * Get the last error message from the reader
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any reader function.
 * 
 * context: The reader context
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* parquet_reader_get_error(ParquetReaderContext* context) {
    if (!context || context->error_message[0] == '\0') {
        return NULL;
    }
    
    return context->error_message;
} 