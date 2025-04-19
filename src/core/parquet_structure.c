/**
 * parquet_structure.c
 * 
 * This file implements the functions declared in parquet_structure.h for managing
 * Parquet file structures including memory allocation and deallocation.
 */

#include "core/parquet_structure.h"
#include "core/arrow_adapter.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/**
 * Creates a new empty Parquet file structure
 * 
 * This function allocates memory for a new ParquetFile structure and initializes
 * its fields with default values. The caller is responsible for freeing the memory
 * using releaseParquetFile function.
 * 
 * Return: Pointer to a new ParquetFile structure, or NULL if allocation fails
 */
ParquetFile* createParquetFile() {
    ParquetFile* file = (ParquetFile*)malloc(sizeof(ParquetFile));
    if (!file) {
        return NULL;
    }
    
    // Initialize fields with default values
    file->file_path = NULL;
    file->total_rows = 0;
    file->row_group_count = 0;
    file->row_groups = NULL;
    file->metadata_path = NULL;
    
    return file;
}

/**
 * Releases memory allocated for a Parquet file structure
 * 
 * This function frees all memory allocated for the ParquetFile structure, including
 * its row groups, columns, and pages. After calling this function, the pointer
 * should not be used anymore.
 * 
 * file: Pointer to a ParquetFile structure to be freed
 */
void releaseParquetFile(ParquetFile* file) {
    if (!file) {
        return;
    }
    
    // Free file path
    if (file->file_path) {
        free(file->file_path);
    }
    
    // Free metadata path
    if (file->metadata_path) {
        free(file->metadata_path);
    }
    
    // Free row groups
    for (uint32_t i = 0; i < file->row_group_count; i++) {
        ParquetRowGroup* row_group = &file->row_groups[i];
        
        // Free metadata path
        if (row_group->metadata_path) {
            free(row_group->metadata_path);
        }
        
        // Free columns
        for (uint32_t j = 0; j < row_group->column_count; j++) {
            ParquetColumn* column = &row_group->columns[j];
            
            // Free column data
            if (column->column_data) {
                free(column->column_data);
            }
            
            // Free compression path
            if (column->compression_path) {
                free(column->compression_path);
            }
            
            // Free pages
            if (column->pages) {
                for (uint32_t k = 0; k < column->page_count; k++) {
                    ParquetPage* page = &column->pages[k];
                    
                    // Free page data
                    if (page->page_data) {
                        free(page->page_data);
                    }
                }
                
                free(column->pages);
            }
        }
        
        if (row_group->columns) {
            free(row_group->columns);
        }
    }
    
    if (file->row_groups) {
        free(file->row_groups);
    }
    
    free(file);
}

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
ParquetRowGroup* addRowGroupToFile(ParquetFile* file, uint64_t num_rows) {
    if (!file) {
        return NULL;
    }
    
    // Allocate memory for the new row group
    uint32_t new_count = file->row_group_count + 1;
    ParquetRowGroup* new_row_groups = (ParquetRowGroup*)realloc(
        file->row_groups, new_count * sizeof(ParquetRowGroup));
    
    if (!new_row_groups) {
        return NULL;
    }
    
    file->row_groups = new_row_groups;
    
    // Initialize the new row group
    ParquetRowGroup* row_group = &file->row_groups[file->row_group_count];
    row_group->row_group_index = file->row_group_count;
    row_group->num_rows = num_rows;
    row_group->column_count = 0;
    row_group->columns = NULL;
    row_group->metadata_path = NULL;
    
    // Update file
    file->row_group_count = new_count;
    file->total_rows += num_rows;
    
    return row_group;
}

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
ParquetColumn* addColumnToRowGroup(ParquetRowGroup* row_group, const char* name, ParquetValueType type) {
    if (!row_group || !name) {
        return NULL;
    }
    
    // Check name length to prevent buffer overflow
    size_t name_length = strlen(name);
    if (name_length >= MAX_COLUMN_NAME_LENGTH) {
        fprintf(stderr, "Column name too long: %s\n", name);
        return NULL;
    }
    
    // Allocate memory for the new column
    uint32_t new_count = row_group->column_count + 1;
    ParquetColumn* new_columns = (ParquetColumn*)realloc(
        row_group->columns, new_count * sizeof(ParquetColumn));
    
    if (!new_columns) {
        return NULL;
    }
    
    row_group->columns = new_columns;
    
    // Initialize the new column
    ParquetColumn* column = &row_group->columns[row_group->column_count];
    strncpy(column->name, name, MAX_COLUMN_NAME_LENGTH - 1);
    column->name[MAX_COLUMN_NAME_LENGTH - 1] = '\0';  // Ensure null-termination
    column->column_index = row_group->column_count;
    column->type = type;
    column->total_uncompressed_size = 0;
    column->total_compressed_size = 0;
    column->total_values = 0;
    column->page_count = 0;
    column->pages = NULL;
    column->column_data = NULL;
    column->compression_path = NULL;
    
    // Update row group
    row_group->column_count = new_count;
    
    return column;
}

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
                             uint32_t value_count, uint32_t null_count) {
    if (!column) {
        return NULL;
    }
    
    // Allocate memory for the new page
    uint32_t new_count = column->page_count + 1;
    ParquetPage* new_pages = (ParquetPage*)realloc(
        column->pages, new_count * sizeof(ParquetPage));
    
    if (!new_pages) {
        return NULL;
    }
    
    column->pages = new_pages;
    
    // Initialize the new page
    ParquetPage* page = &column->pages[column->page_count];
    page->page_index = column->page_count;
    page->offset = offset;
    page->compressed_size = compressed_size;
    page->uncompressed_size = uncompressed_size;
    page->value_count = value_count;
    page->null_count = null_count;
    page->page_data = NULL;
    
    // Update column
    column->page_count = new_count;
    column->total_compressed_size += compressed_size;
    column->total_uncompressed_size += uncompressed_size;
    column->total_values += value_count;
    
    return page;
}

/**
 * Initialize a new parquet file structure
 * 
 * This function allocates and initializes a new ParquetFile structure.
 * The caller is responsible for freeing the allocated memory using
 * the parquet_file_free function.
 * 
 * file_path: Path to the parquet file
 * returns: Pointer to the newly allocated ParquetFile structure or NULL on failure
 */
ParquetFile* parquet_file_init(const char* file_path) {
    if (!file_path) {
        return NULL;
    }
    
    // Allocate memory for the ParquetFile structure
    ParquetFile* file = (ParquetFile*)malloc(sizeof(ParquetFile));
    if (!file) {
        return NULL;
    }
    
    // Clear the structure to initialize all fields to zero
    memset(file, 0, sizeof(ParquetFile));
    
    // Allocate memory for the file path and copy it
    size_t path_len = strlen(file_path) + 1;
    file->file_path = (char*)malloc(path_len);
    if (!file->file_path) {
        free(file);
        return NULL;
    }
    
    // Copy the file path
    memcpy(file->file_path, file_path, path_len);
    
    return file;
}

/**
 * Free resources associated with a parquet file structure
 * 
 * This function releases all memory allocated for the ParquetFile structure.
 * 
 * file: Pointer to the ParquetFile structure to free
 */
void parquet_file_free(ParquetFile* file) {
    if (!file) {
        return;
    }
    
    // Free the file path
    if (file->file_path) {
        free(file->file_path);
    }
    
    // Free the file structure
    free(file);
}

/**
 * Load the structure of a parquet file
 * 
 * This function reads the parquet file metadata to populate the ParquetFile structure.
 * It does not load the actual column data, only the file structure.
 * 
 * file: Pointer to an initialized ParquetFile structure
 * returns: 0 on success, non-zero error code on failure
 */
int parquet_load_structure(ParquetFile* file) {
    if (!file) {
        return -1;
    }
    
    if (!file->file_path) {
        return -2;
    }
    
    // Use the arrow_adapter to read the parquet file structure
    int result = arrow_read_parquet_structure(file->file_path, file);
    if (result != 0) {
        // Get the error message from arrow_adapter
        const char* error_msg = arrow_get_last_error();
        if (error_msg) {
            fprintf(stderr, "Error reading parquet structure: %s\n", error_msg);
        } else {
            fprintf(stderr, "Unknown error reading parquet structure\n");
        }
        return -3;
    }
    
    return 0;
}

/**
 * Get the number of row groups in a parquet file
 * 
 * This function returns the number of row groups in the specified parquet file.
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: Number of row groups, or 0 if file is NULL
 */
uint32_t parquet_file_get_row_group_count(const ParquetFile* file) {
    if (!file) {
        return 0;
    }
    
    return file->row_group_count;
}

/**
 * Get the number of columns in a parquet file
 * 
 * This function returns the number of columns in the first row group of the specified parquet file.
 * If there are no row groups, it returns 0.
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: Number of columns, or 0 if file is NULL or has no row groups
 */
uint32_t parquet_file_get_column_count(const ParquetFile* file) {
    if (!file || file->row_group_count == 0) {
        return 0;
    }
    
    return file->row_groups[0].column_count;
}

/**
 * Get the file path of a parquet file
 * 
 * This function returns the file path of the specified parquet file.
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: File path, or NULL if file is NULL or has no file path
 */
const char* parquet_file_get_path(const ParquetFile* file) {
    if (!file) {
        return NULL;
    }
    
    return file->file_path;
}

/**
 * Get the type of a column in a parquet file
 * 
 * This function returns the type of the specified column in the first row group of the parquet file.
 * If there are no row groups or the column index is out of bounds, it returns 0.
 * 
 * file: Pointer to a ParquetFile structure
 * column_index: Index of the column
 * 
 * Return: Column type, or 0 if file is NULL, has no row groups, or column index is out of bounds
 */
ParquetValueType parquet_file_get_column_type(const ParquetFile* file, uint32_t column_index) {
    if (!file || file->row_group_count == 0 || column_index >= file->row_groups[0].column_count) {
        return 0;
    }
    
    return file->row_groups[0].columns[column_index].type;
}

/**
 * Get the size of a row group in a parquet file
 * 
 * This function returns the size (in bytes) of the specified row group.
 * The size is calculated as the sum of the total uncompressed sizes of all columns in the row group.
 * 
 * file: Pointer to a ParquetFile structure
 * row_group_index: Index of the row group
 * 
 * Return: Row group size in bytes, or 0 if file is NULL or row group index is out of bounds
 */
uint64_t parquet_file_get_row_group_size(const ParquetFile* file, uint32_t row_group_index) {
    if (!file || row_group_index >= file->row_group_count) {
        return 0;
    }
    
    ParquetRowGroup* row_group = &file->row_groups[row_group_index];
    
    uint64_t total_size = 0;
    for (uint32_t i = 0; i < row_group->column_count; i++) {
        total_size += row_group->columns[i].total_uncompressed_size;
    }
    
    return total_size;
}

/**
 * Get the name of a column in a parquet file
 * 
 * This function returns the name of the specified column in the first row group of the parquet file.
 * If there are no row groups or the column index is out of bounds, it returns NULL.
 * 
 * file: Pointer to a ParquetFile structure
 * column_index: Index of the column
 * 
 * Return: Column name, or NULL if file is NULL, has no row groups, or column index is out of bounds
 */
const char* parquet_file_get_column_name(const ParquetFile* file, uint32_t column_index) {
    if (!file || file->row_group_count == 0 || column_index >= file->row_groups[0].column_count) {
        return NULL;
    }
    
    return file->row_groups[0].columns[column_index].name;
}

/**
 * Get the total size of a parquet file
 * 
 * This function returns the total size (in bytes) of the parquet file.
 * The size is calculated as the sum of the total uncompressed sizes of all columns in all row groups.
 * 
 * file: Pointer to a ParquetFile structure
 * 
 * Return: File size in bytes, or 0 if file is NULL
 */
uint64_t parquet_file_get_size(const ParquetFile* file) {
    if (!file) {
        return 0;
    }
    
    uint64_t total_size = 0;
    for (uint32_t i = 0; i < file->row_group_count; i++) {
        ParquetRowGroup* row_group = &file->row_groups[i];
        
        for (uint32_t j = 0; j < row_group->column_count; j++) {
            total_size += row_group->columns[j].total_uncompressed_size;
        }
    }
    
    return total_size;
} 