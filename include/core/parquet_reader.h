#ifndef INFPARQUET_PARQUET_READER_H
#define INFPARQUET_PARQUET_READER_H

#include "parquet_structure.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Error codes for parquet reader operations
 */
typedef enum {
    PARQUET_READER_OK = 0,
    PARQUET_READER_FILE_NOT_FOUND,
    PARQUET_READER_INVALID_FILE,
    PARQUET_READER_MEMORY_ERROR,
    PARQUET_READER_ARROW_ERROR,
    PARQUET_READER_INVALID_PARAMETER,
    PARQUET_READER_UNKNOWN_ERROR
} ParquetReaderError;

/**
 * Context structure for parquet reader
 * Maintains the internal state of the reader
 */
typedef struct ParquetReaderContext ParquetReaderContext;

/**
 * Create a new parquet reader context
 * 
 * This function initializes a reader context for the specified parquet file.
 * The context must be released with parquet_reader_close when no longer needed.
 * 
 * file_path: Path to the parquet file to read
 * returns: A new reader context, or NULL if an error occurred
 */
ParquetReaderContext* parquet_reader_open(const char* file_path);

/**
 * Close a parquet reader context and free associated resources
 * 
 * This function releases all resources associated with the reader context.
 * 
 * context: The reader context to close
 */
void parquet_reader_close(ParquetReaderContext* context);

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
ParquetReaderError parquet_reader_get_structure(ParquetReaderContext* context, ParquetFile* file);

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
);

/**
 * Free a buffer allocated by parquet_reader_read_column
 * 
 * This function releases memory allocated by parquet_reader_read_column.
 * 
 * buffer: The buffer to free
 */
void parquet_reader_free_buffer(void* buffer);

/**
 * Get the last error message from the reader
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any reader function.
 * 
 * context: The reader context
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* parquet_reader_get_error(ParquetReaderContext* context);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_PARQUET_READER_H */ 