#ifndef INFPARQUET_PARQUET_WRITER_H
#define INFPARQUET_PARQUET_WRITER_H

#include "parquet_structure.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Error codes for parquet writer operations
 */
typedef enum {
    PARQUET_WRITER_OK = 0,
    PARQUET_WRITER_FILE_ERROR,
    PARQUET_WRITER_MEMORY_ERROR,
    PARQUET_WRITER_ARROW_ERROR,
    PARQUET_WRITER_INVALID_PARAMETER,
    PARQUET_WRITER_UNKNOWN_ERROR
} ParquetWriterError;

/**
 * Context structure for parquet writer
 * Maintains the internal state of the writer
 */
typedef struct ParquetWriterContext ParquetWriterContext;

/**
 * Create a new parquet writer context
 * 
 * This function initializes a writer context for creating a new parquet file.
 * The context must be released with parquet_writer_close when no longer needed.
 * 
 * file_path: Path where the parquet file will be written
 * returns: A new writer context, or NULL if an error occurred
 */
ParquetWriterContext* parquet_writer_create(const char* file_path);

/**
 * Close a parquet writer context and free associated resources
 * 
 * This function finishes writing the parquet file and releases all resources 
 * associated with the writer context.
 * 
 * context: The writer context to close
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_close(ParquetWriterContext* context);

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
);

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
);

/**
 * Finish the current row group
 * 
 * This function completes the current row group.
 * 
 * context: The writer context
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_end_row_group(ParquetWriterContext* context);

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
);

/**
 * Reconstruct a parquet file from multiple column files
 * 
 * This function assembles a parquet file from separate column files.
 * Used during decompression to reconstruct the original parquet file.
 * 
 * file_structure: Structure of the original parquet file
 * output_path: Path where the reconstructed file will be written
 * column_file_paths: Array of paths to the column files
 * returns: Error code (PARQUET_WRITER_OK on success)
 */
ParquetWriterError parquet_writer_reconstruct_file(
    const ParquetFile* file_structure,
    const char* output_path,
    char** column_file_paths
);

/**
 * Get the last error message from the writer
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any writer function.
 * 
 * context: The writer context
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* parquet_writer_get_error(ParquetWriterContext* context);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_PARQUET_WRITER_H */ 