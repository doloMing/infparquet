/**
 * parquet_structure.h
 * 
 * This header file defines the core structures and constants for working with Parquet files.
 * It includes the definitions of page, column, row group and file structures.
 */

#ifndef INFPARQUET_PARQUET_STRUCTURE_H
#define INFPARQUET_PARQUET_STRUCTURE_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Constants */
#define MAX_COLUMN_NAME_LENGTH 128
#define MAX_COLUMNS_PER_ROW_GROUP 1024
#define MAX_ROWGROUPS_PER_FILE 1024
#define MAX_PAGES_PER_COLUMN 4096

/**
 * Enumeration of Parquet value types supported by InfParquet
 */
typedef enum {
    PARQUET_BOOLEAN = 0,
    PARQUET_INT32 = 1,
    PARQUET_INT64 = 2,
    PARQUET_FLOAT = 3,
    PARQUET_DOUBLE = 4,
    PARQUET_BYTE_ARRAY = 5,
    PARQUET_FIXED_LEN_BYTE_ARRAY = 6,
    PARQUET_INT96 = 7,  /* Commonly used for timestamp in Parquet */
    PARQUET_STRING = 8,
    PARQUET_BINARY = 9,
    PARQUET_TIMESTAMP = 10
} ParquetValueType;

/**
 * Enumeration for compression types
 */
typedef enum {
    COMPRESSION_NONE = 0,
    COMPRESSION_LZMA2 = 1,
    COMPRESSION_SNAPPY = 2,
    COMPRESSION_GZIP = 3,
    COMPRESSION_LZ4 = 4,
    COMPRESSION_ZSTD = 5
} CompressionType;

/**
 * Structure representing a Parquet page
 */
typedef struct {
    uint32_t page_index;
    uint64_t offset;
    uint64_t compressed_size;
    uint64_t uncompressed_size;
    uint32_t value_count;
    uint32_t null_count;
    void* page_data;         /* Optional, can be NULL if data is not loaded */
} ParquetPage;

/**
 * Structure representing a Parquet column
 */
typedef struct {
    char name[MAX_COLUMN_NAME_LENGTH];
    uint32_t column_index;
    ParquetValueType type;
    uint64_t total_uncompressed_size;
    uint64_t total_compressed_size;
    uint64_t total_values;
    uint32_t page_count;
    ParquetPage* pages;      /* Array of pages */
    void* column_data;       /* Optional, can be NULL if data is not loaded */
    char* compression_path;  /* Path to compressed file if applicable */
    uint32_t fixed_len_byte_array_size; /* Size in bytes for FIXED_LEN_BYTE_ARRAY type */
} ParquetColumn;

/**
 * Structure representing a Parquet row group
 */
typedef struct {
    uint32_t row_group_index;
    uint64_t num_rows;
    uint32_t column_count;
    ParquetColumn* columns;  /* Array of columns */
    char* metadata_path;     /* Path to row group metadata file if applicable */
} ParquetRowGroup;

/**
 * Structure representing a Parquet file
 */
typedef struct {
    char* file_path;
    uint64_t total_rows;
    uint32_t row_group_count;
    ParquetRowGroup* row_groups;  /* Array of row groups */
    char* metadata_path;          /* Path to file metadata if applicable */
} ParquetFile;

/**
 * Creates a new empty Parquet file structure
 * 
 * This function allocates memory for a new ParquetFile structure and initializes
 * its fields with default values. The caller is responsible for freeing the memory
 * using releaseParquetFile function.
 * 
 * Return: Pointer to a new ParquetFile structure, or NULL if allocation fails
 */
ParquetFile* createParquetFile();

/**
 * Releases memory allocated for a Parquet file structure
 * 
 * This function frees all memory allocated for the ParquetFile structure, including
 * its row groups, columns, and pages. After calling this function, the pointer
 * should not be used anymore.
 * 
 * file: Pointer to a ParquetFile structure to be freed
 */
void releaseParquetFile(ParquetFile* file);

/**
 * Creates a new row group and adds it to the file
 * 
 * This function allocates memory for a new ParquetRowGroup structure, initializes it
 * with the provided parameters, and adds it to the specified ParquetFile.
 * 
 * file: Pointer to a ParquetFile structure where the row group will be added
 * num_rows: Number of rows in the row group
 * 
 * Return: Pointer to the newly created row group, or NULL if allocation fails
 */
ParquetRowGroup* addRowGroupToFile(ParquetFile* file, uint64_t num_rows);

/**
 * Creates a new column and adds it to the row group
 * 
 * This function allocates memory for a new ParquetColumn structure, initializes it
 * with the provided parameters, and adds it to the specified ParquetRowGroup.
 * 
 * row_group: Pointer to a ParquetRowGroup where the column will be added
 * name: Name of the column
 * type: Type of values stored in the column
 * 
 * Return: Pointer to the newly created column, or NULL if allocation fails
 */
ParquetColumn* addColumnToRowGroup(ParquetRowGroup* row_group, const char* name, ParquetValueType type);

/**
 * Creates a new page and adds it to the column
 * 
 * This function allocates memory for a new ParquetPage structure, initializes it
 * with the provided parameters, and adds it to the specified ParquetColumn.
 * 
 * column: Pointer to a ParquetColumn where the page will be added
 * offset: Offset of the page in the column
 * compressed_size: Size of the page after compression
 * uncompressed_size: Size of the page before compression
 * value_count: Number of values in the page
 * null_count: Number of null values in the page
 * 
 * Return: Pointer to the newly created page, or NULL if allocation fails
 */
ParquetPage* addPageToColumn(ParquetColumn* column, uint64_t offset,
                             uint64_t compressed_size, uint64_t uncompressed_size,
                             uint32_t value_count, uint32_t null_count);

/**
 * Get the number of row groups in a parquet file
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: Number of row groups, or 0 if file is NULL
 */
uint32_t parquet_file_get_row_group_count(const ParquetFile* file);

/**
 * Get the number of columns in a parquet file
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: Number of columns in the first row group, or 0 if file is NULL or has no row groups
 */
uint32_t parquet_file_get_column_count(const ParquetFile* file);

/**
 * Get the file path of a parquet file
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: File path, or NULL if file is NULL
 */
const char* parquet_file_get_path(const ParquetFile* file);

/**
 * Get the type of a column in a parquet file
 * 
 * file: Pointer to a ParquetFile structure
 * column_index: Index of the column
 * 
 * Return: Column type, or 0 if file is NULL, has no row groups, or column index is out of bounds
 */
ParquetValueType parquet_file_get_column_type(const ParquetFile* file, uint32_t column_index);

/**
 * Get the size of a row group in a parquet file
 * 
 * file: Pointer to a ParquetFile structure
 * row_group_index: Index of the row group
 * 
 * Return: Row group size in bytes, or 0 if file is NULL or row group index is out of bounds
 */
uint64_t parquet_file_get_row_group_size(const ParquetFile* file, uint32_t row_group_index);

/**
 * Get the name of a column in a parquet file
 * 
 * file: Pointer to a ParquetFile structure
 * column_index: Index of the column
 * 
 * Return: Column name, or NULL if file is NULL, has no row groups, or column index is out of bounds
 */
const char* parquet_file_get_column_name(const ParquetFile* file, uint32_t column_index);

/**
 * Get the total size of a parquet file
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: File size in bytes, or 0 if file is NULL
 */
uint64_t parquet_file_get_size(const ParquetFile* file);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_PARQUET_STRUCTURE_H */ 