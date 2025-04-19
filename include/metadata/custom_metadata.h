#ifndef INFPARQUET_CUSTOM_METADATA_H
#define INFPARQUET_CUSTOM_METADATA_H

#include "metadata_types.h"
#include "../core/parquet_structure.h"
#include "../core/parquet_reader.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Constants for custom metadata */
#define MAX_SQL_QUERY_LENGTH 512

/**
 * Target types for custom metadata
 */
typedef enum {
    CUSTOM_METADATA_TARGET_FILE = 0,
    CUSTOM_METADATA_TARGET_ROW_GROUP,
    CUSTOM_METADATA_TARGET_COLUMN
} CustomMetadataTarget;

/**
 * Update frequency for custom metadata
 */
typedef enum {
    CUSTOM_METADATA_UPDATE_ON_READ = 0,
    CUSTOM_METADATA_UPDATE_ON_WRITE,
    CUSTOM_METADATA_UPDATE_MANUAL
} CustomMetadataUpdateFrequency;

/**
 * Error codes for custom metadata operations
 */
typedef enum {
    CUSTOM_METADATA_OK = 0,
    CUSTOM_METADATA_MEMORY_ERROR,
    CUSTOM_METADATA_INVALID_PARAMETER,
    CUSTOM_METADATA_PARSE_ERROR,
    CUSTOM_METADATA_TOO_MANY_ITEMS,
    CUSTOM_METADATA_FILE_ERROR,
    CUSTOM_METADATA_INVALID_FORMAT,
    CUSTOM_METADATA_UNKNOWN_ERROR
} CustomMetadataError;

/**
 * Extended CustomMetadataItem structure
 * This extends the CustomMetadataItem defined in metadata_types.h
 * with additional fields specific to the custom metadata implementation.
 */
typedef struct {
    CustomMetadataItem base;  /* Base CustomMetadataItem structure */
    char* description;        /* Additional description */
    CustomMetadataTarget target;  /* Target type for the metadata */
    bool cache_results;       /* Whether to cache the results */
    CustomMetadataUpdateFrequency update_frequency;  /* Update frequency */
} CustomMetadataItemExt;

/**
 * Parse custom metadata configuration from a JSON file
 * 
 * This function reads a user-provided JSON file containing custom metadata
 * specifications (SQL queries) and initializes custom metadata items.
 * 
 * config_path: Path to the JSON configuration file
 * custom_metadata: Pointer to store the array of custom metadata items
 * item_count: Pointer to store the number of items parsed
 * returns: Error code (CUSTOM_METADATA_OK on success)
 */
CustomMetadataError custom_metadata_parse_config(
    const char* config_path,
    CustomMetadataItem** custom_metadata,
    uint32_t* item_count
);

/**
 * Evaluate custom metadata for a parquet file
 * 
 * This function evaluates the custom metadata items against the parquet file
 * and populates the boolean results for each item. The results are stored in
 * a nested format as described in the requirements.
 * 
 * file: Parquet file structure containing the file organization
 * reader_context: Context for reading the parquet file data
 * items: Array of custom metadata items to evaluate
 * count: Number of items in the array
 * returns: Error code (CUSTOM_METADATA_OK on success)
 */
CustomMetadataError custom_metadata_evaluate(
    const ParquetFile* file,
    ParquetReaderContext* reader_context,
    CustomMetadataItem items[MAX_CUSTOM_METADATA_ITEMS],
    int count
);

/**
 * Free memory allocated for custom metadata items
 * 
 * This function releases all memory associated with an array of custom metadata items,
 * including their result matrices and description strings.
 * 
 * items: Array of custom metadata items to free
 * count: Number of items in the array
 */
void custom_metadata_free_items(
    CustomMetadataItem items[MAX_CUSTOM_METADATA_ITEMS],
    int count
);

/**
 * Get the last error message from custom metadata operations
 * 
 * Returns a pointer to a static string containing the last error message.
 * This string should not be freed by the caller.
 * 
 * returns: Pointer to the error message string, or NULL if no error has occurred
 */
const char* custom_metadata_get_error();

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_CUSTOM_METADATA_H */ 