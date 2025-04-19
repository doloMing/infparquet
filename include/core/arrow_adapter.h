/**
 * arrow_adapter.h
 * 
 * This header provides functions to adapt the Apache Arrow library for use with InfParquet.
 * It abstracts Arrow's Parquet reading and writing functionality to simplify integration.
 */

#ifndef INFPARQUET_ARROW_ADAPTER_H
#define INFPARQUET_ARROW_ADAPTER_H

/**
 * Arrow Adapter Interface
 * 
 * This file provides interfaces for working with Arrow and Parquet files.
 */

#include <stdint.h>
#include <stddef.h>
#include "core/parquet_structure.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Reads the structure of a Parquet file using Arrow
 * 
 * This function parses a Parquet file using Arrow and fills a ParquetFile structure
 * with information about the file structure, including row groups and columns.
 * 
 * file_path: Path to the Parquet file
 * parquet_file: Pointer to a ParquetFile structure to be filled
 * 
 * Return: 0 on success, non-zero on error
 */
int arrow_read_parquet_structure(const char* file_path, ParquetFile* parquet_file);

/**
 * Reads column data from a Parquet file using Arrow
 * 
 * This function reads data from a specific column in a Parquet file using Arrow.
 * The data is returned as a newly allocated buffer, which the caller must free.
 * 
 * file_path: Path to the Parquet file
 * row_group_id: Index of the row group
 * column_id: Index of the column
 * buffer: Pointer to a void pointer that will receive the allocated buffer
 * buffer_size: Pointer to a size_t that will receive the buffer size
 * 
 * Return: 0 on success, non-zero on error
 */
int arrow_read_column_data(const char* file_path, int row_group_id, int column_id, 
                          void** buffer, size_t* buffer_size);

/**
 * Creates a new Parquet file with the given data and schema
 * 
 * This function creates a new Parquet file using Arrow, with the provided
 * column data and schema.
 * 
 * file_path: Path where the Parquet file will be written
 * column_data: Array of pointers to column data
 * column_sizes: Array of sizes for each column
 * schema: Array of ParquetValueType for each column
 * fixed_len_sizes: Array of fixed lengths for FIXED_LEN_BYTE_ARRAY columns, or NULL for others
 * column_count: Number of columns
 * row_count: Number of rows
 * 
 * Return: 0 on success, non-zero on error
 */
int arrow_create_parquet_file(const char* file_path, void** column_data, size_t* column_sizes,
                             ParquetValueType* schema, int* fixed_len_sizes, 
                             int column_count, int64_t row_count);

/**
 * Gets the last error message from the Arrow adapter
 * 
 * Return: The last error message, or an empty string if no error has occurred
 */
const char* arrow_get_last_error();

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_ARROW_ADAPTER_H */ 