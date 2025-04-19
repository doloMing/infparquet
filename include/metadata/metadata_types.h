/**
 * metadata_types.h
 * 
 * This header file defines the metadata structures used in the InfParquet framework.
 * It includes definitions for the four basic metadata types (timestamp, string, numeric, 
 * categorical) and custom user-defined metadata.
 */

#ifndef INFPARQUET_METADATA_TYPES_H
#define INFPARQUET_METADATA_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Constants */
#define MAX_METADATA_ITEM_NAME_LENGTH 128
#define MAX_STRING_LENGTH 256
#define MAX_HIGH_FREQ_STRINGS 10
#define MAX_SPECIAL_STRINGS 20
#define MAX_HIGH_FREQ_CATEGORIES 20
#define MAX_CUSTOM_METADATA_ITEMS 20
#define MAX_METADATA_JSON_SIZE 8192
#define MAX_METADATA_STRING_LENGTH 256

/**
 * Enumeration of metadata types
 */
typedef enum {
    METADATA_TYPE_TIMESTAMP = 0,
    METADATA_TYPE_STRING = 1,
    METADATA_TYPE_NUMERIC = 2,
    METADATA_TYPE_CATEGORICAL = 3,
    METADATA_TYPE_CUSTOM = 4,
    METADATA_TYPE_FILE = 5,
    METADATA_TYPE_ROW_GROUP = 6,
    METADATA_TYPE_COLUMN = 7
} MetadataType;

/**
 * Enumeration of metadata generator errors
 */
typedef enum {
    METADATA_GEN_OK = 0,
    METADATA_GEN_MEMORY_ERROR,
    METADATA_GEN_INVALID_PARAMETER,
    METADATA_GEN_PARQUET_ERROR,
    METADATA_GEN_FILE_ERROR,
    METADATA_GEN_CUSTOM_METADATA_ERROR
} MetadataGeneratorError;

/**
 * Structure for high frequency string with count
 */
typedef struct {
    char string[MAX_STRING_LENGTH];
    uint32_t count;
} HighFreqString;

/**
 * Structure for custom metadata items
 * These are user-defined metadata based on SQL queries
 * Results are stored as binary matrices 
 * Format: {{11}{00}{10}} means:
 * - First row group: all columns have value 1
 * - Second row group: all columns have value 0
 * - Third row group: first column has value 1, second column has value 0
 */
typedef struct {
    char name[MAX_METADATA_ITEM_NAME_LENGTH];  /* Name of the custom metadata item */
    char sql_query[MAX_STRING_LENGTH];         /* SQL query used to generate this metadata */
    char* result_matrix;                       /* Binary matrix result as described above */
    uint32_t row_group_count;                  /* Number of row groups in the matrix */
    uint32_t column_count;                     /* Number of columns per row group */
} CustomMetadataItem;

/**
 * Structure for timestamp metadata
 * Contains time range information for time-based columns
 */
typedef struct {
    bool has_timestamps;                /* Whether this metadata has timestamp data */
    time_t min_timestamp;               /* Minimum timestamp value */
    time_t max_timestamp;               /* Maximum timestamp value */
    uint64_t count;                     /* Number of timestamp values */
    uint32_t null_count;                /* Number of null timestamp values */
    bool has_timestamp_data;            /* Flag indicating presence of timestamp data */
} TimestampMetadata;

/**
 * Structure for string metadata high-frequency string
 */
typedef struct {
    char string[MAX_STRING_LENGTH];     /* String value */
    uint32_t count;                     /* Frequency count */
} StringHighFreqItem;

/**
 * Structure for string metadata
 * Contains high frequency strings and special strings
 */
typedef struct {
    bool has_string_data;                                          /* Whether this metadata has string data */
    char high_frequency_strings[MAX_HIGH_FREQ_STRINGS][MAX_STRING_LENGTH];  /* Most frequent strings */
    int frequencies[MAX_HIGH_FREQ_STRINGS];                        /* Count of each high frequency string */
    uint32_t count;                                                /* Number of high frequency strings stored */
    uint32_t min_length;                                           /* Minimum string length */
    uint32_t max_length;                                           /* Maximum string length */
    uint64_t total_length;                                         /* Total length of all strings */
    uint64_t total_count;                                          /* Total number of strings */
    float avg_length;                                              /* Average string length */
    uint32_t null_count;                                           /* Number of null values */
    
    // Special strings fields
    char special_strings[MAX_SPECIAL_STRINGS][MAX_STRING_LENGTH];  /* Special strings like "error", "bug", etc. */
    uint32_t special_string_counts[MAX_SPECIAL_STRINGS];           /* Count of each special string */
    uint32_t special_string_count;                                 /* Number of special strings stored */
    uint64_t total_string_count;                                   /* Total number of strings */
    uint32_t avg_string_length;                                    /* Average string length */
    uint32_t high_freq_counts[MAX_HIGH_FREQ_STRINGS];              /* Count of each high frequency string */
    
    struct {
        char string[MAX_STRING_LENGTH];
        uint32_t count;
    } high_freq_strings[MAX_HIGH_FREQ_STRINGS];                    /* High frequency strings with counts */
    uint32_t high_freq_count;                                      /* Number of high frequency strings */
} StringMetadata;

/**
 * Structure for numeric metadata
 * Contains statistical information about numeric columns
 */
typedef struct {
    bool has_numeric_data;              /* Whether this metadata has numeric data */
    double min_value;                   /* Minimum value */
    double max_value;                   /* Maximum value */
    double mean_value;                  /* Mean (average) value */
    double avg_value;                   /* Average value (alias for mean_value) */
    double mode_value;                  /* Mode (most frequent value) */
    uint64_t mode_count;                /* Count of the mode value */
    uint64_t total_count;               /* Total number of values */
    uint32_t null_count;                /* Number of null values */
} NumericMetadata;

/**
 * Structure for categorical metadata
 * Contains information about categorical columns
 */
typedef struct {
    bool has_categorical_data;                                     /* Whether this metadata has categorical data */
    char categories[MAX_HIGH_FREQ_CATEGORIES][MAX_STRING_LENGTH];   /* Most frequent categories */
    uint32_t category_counts[MAX_HIGH_FREQ_CATEGORIES];             /* Count of each category */
    uint32_t high_freq_category_count;                              /* Number of high frequency categories stored */
    uint32_t total_category_count;                                  /* Total number of distinct categories */
    uint64_t total_value_count;                                     /* Total number of values */
} CategoricalMetadata;

/**
 * Structure for metadata item
 */
typedef struct MetadataItem {
    char name[MAX_METADATA_ITEM_NAME_LENGTH];  /* Name of the metadata item */
    MetadataType type;                         /* Type of metadata */
    union {
        TimestampMetadata timestamp;
        StringMetadata string;
        NumericMetadata numeric;
        CategoricalMetadata categorical;
    } value;
    double numeric_value;                      /* Numeric value for METADATA_TYPE_NUMERIC */
    uint64_t timestamp_value;                  /* Timestamp value for METADATA_TYPE_TIMESTAMP */
} MetadataItem;

/**
 * Base metadata structure
 * Contains the common metadata fields for all metadata types
 */
typedef struct {
    TimestampMetadata timestamp_metadata;
    StringMetadata string_metadata;
    NumericMetadata numeric_metadata;
    CategoricalMetadata categorical_metadata;
    uint32_t metadata_count;
    MetadataItem* metadata;
    uint32_t item_count;
    MetadataItem** items;
} BaseMetadata;

/**
 * Structure for file level metadata
 */
typedef struct {
    uint32_t basic_metadata_count;                    /* Number of basic metadata items */
    MetadataItem* basic_metadata;                     /* Array of basic metadata items */
    
    uint32_t custom_metadata_count;                   /* Number of custom metadata items */
    CustomMetadataItem* custom_metadata;              /* Array of custom metadata items */
    
    bool use_basic_metadata;                          /* Whether to use basic metadata */
} FileMetadata;

/**
 * Structure for row group metadata
 */
typedef struct {
    uint32_t row_group_index;                         /* Index of the row group */
    uint32_t metadata_count;                          /* Number of metadata items */
    MetadataItem* metadata;                           /* Array of metadata items */
    BaseMetadata* base_metadata;                      /* Base metadata for this row group */
    uint32_t column_count;                            /* Number of columns in this row group */
    struct ColumnMetadata** columns;                  /* Array of column metadata */
} RowGroupMetadata;

/**
 * Structure for column metadata
 */
typedef struct ColumnMetadata {
    uint32_t column_index;                            /* Index of the column */
    char column_name[MAX_METADATA_ITEM_NAME_LENGTH];  /* Name of the column */
    uint32_t metadata_count;                          /* Number of metadata items */
    MetadataItem* metadata;                           /* Array of metadata items */
    BaseMetadata* base_metadata;                      /* Base metadata for this column */
} ColumnMetadata;

/**
 * Extended metadata structure
 * Used internally for handling metadata hierarchies
 */
struct ExtendedMetadata {
    int type;
    int id;
    char name[MAX_METADATA_STRING_LENGTH];
    BaseMetadata* base_metadata;
    int custom_metadata_count;
    CustomMetadataItem* custom_metadata;
    int child_count;
    struct ExtendedMetadata** child_metadata;
    // Fields for compatibility with original metadata structures
    struct {
        int type;
        int id;
        char name[MAX_METADATA_STRING_LENGTH];
    } metadata;
};

/**
 * Main metadata structure
 * Contains all metadata associated with a file, row groups, and columns
 */
typedef struct {
    char* file_path;                           /* Path to the original Parquet file */
    FileMetadata file_metadata;                /* File-level metadata */
    
    uint32_t row_group_metadata_count;         /* Number of row group metadata items */
    RowGroupMetadata* row_group_metadata;      /* Array of row group metadata */
    
    uint32_t column_metadata_count;            /* Number of column metadata items */
    ColumnMetadata* column_metadata;           /* Array of column metadata */
} Metadata;

/**
 * Callback function type for progress reporting
 */
typedef void (*ProgressCallback)(float progress, void* user_data);

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
Metadata* createMetadata(const char* file_path);

/**
 * Releases memory allocated for a metadata structure
 * 
 * This function frees all memory allocated for the Metadata structure, including
 * its file, row group, and column metadata. After calling this function, the pointer
 * should not be used anymore.
 * 
 * metadata: Pointer to a Metadata structure to be freed
 */
void metadata_release(Metadata* metadata);

/**
 * Alias for metadata_release for backward compatibility
 * 
 * Input: metadata Pointer to the metadata structure to free
 */
void releaseMetadata(Metadata* metadata);

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
MetadataItem* addBasicFileMetadataItem(Metadata* metadata, const char* name, MetadataType type);

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
CustomMetadataItem* addCustomFileMetadataItem(Metadata* metadata, const char* name, const char* sql_query);

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
                                     const char* name, MetadataType type);

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
                                   const char* name, MetadataType type);

/**
 * Serializes metadata to a JSON string
 * 
 * This function serializes the metadata structure to a JSON string.
 * The caller is responsible for freeing the returned string.
 * 
 * metadata: Pointer to the Metadata structure
 * 
 * Return: Pointer to a newly allocated string containing the JSON representation,
 *         or NULL if serialization fails
 */
char* metadataToJson(const Metadata* metadata);

/**
 * Deserializes metadata from a JSON string
 * 
 * This function creates a new Metadata structure from a JSON string.
 * The caller is responsible for freeing the returned structure.
 * 
 * json_str: JSON string containing metadata
 * 
 * Return: Pointer to a new Metadata structure, or NULL if deserialization fails
 */
Metadata* metadataFromJson(const char* json_str);

/**
 * Helper functions for accessing metadata fields
 * These functions are provided for language compatibility and safety
 */

/**
 * Gets the type of a metadata object
 * 
 * metadata: Pointer to a Metadata structure
 * 
 * Return: Type of the metadata, or 0 if metadata is NULL
 */
int metadata_get_type(const Metadata* metadata);

/**
 * Gets the name of a metadata object
 * 
 * metadata: Pointer to a Metadata structure
 * 
 * Return: Name of the metadata, or "" if metadata is NULL
 */
const char* metadata_get_name(const Metadata* metadata);

/**
 * Gets the number of child metadata items
 * 
 * metadata: Pointer to a Metadata structure
 * 
 * Return: Number of child metadata items, or 0 if metadata is NULL
 */
int metadata_get_child_count(const Metadata* metadata);

/**
 * Gets a child metadata item by index
 * 
 * metadata: Pointer to a Metadata structure
 * index: Index of the child metadata item
 * 
 * Return: Pointer to the child metadata item, or NULL if index is out of range
 */
Metadata* metadata_get_child(const Metadata* metadata, int index);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_METADATA_TYPES_H */ 