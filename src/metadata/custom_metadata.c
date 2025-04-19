#include "metadata/custom_metadata.h"
#include "metadata/metadata_types.h"
#include "core/parquet_structure.h"
#include "core/parquet_reader.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <stdbool.h>

/* Static global for error messages */
static char s_error_message[256];

/**
 * Helper function to find a JSON field in a JSON string.
 * Returns a pointer to the value part of the field, or NULL if not found.
 * 
 * json_str: The JSON string to search in
 * field_name: The name of the field to find
 * returns: Pointer to the value, or NULL if not found
 */
static const char* find_json_field(const char* json_str, const char* field_name) {
    if (!json_str || !field_name) {
        return NULL;
    }
    
    // Create the search string with quotes: "field_name":
    char search[256];
    snprintf(search, sizeof(search), "\"%s\"", field_name);
    size_t search_len = strlen(search);
    
    // Find the field name
    const char* p = json_str;
    while ((p = strstr(p, search)) != NULL) {
        // Make sure this is a complete field name match (not a prefix)
        if (p > json_str && !isspace(*(p-1)) && *(p-1) != ',') {
            p += search_len;
            continue;
        }
        
        // Skip to the colon after the field name
        p += search_len;
        while (isspace(*p)) p++;
        
        // Make sure there is a colon
        if (*p != ':') {
            continue;
        }
        
        // Skip the colon and any whitespace
        p++;
        while (isspace(*p)) p++;
        
        // We found the field, return a pointer to the value
        return p;
    }
    
    return NULL;
}

/**
 * Helper function to extract a string value from a JSON field.
 * The string should be quoted in the JSON.
 * 
 * json_value: Pointer to the JSON value (after the field name and colon)
 * out_str: Buffer to store the extracted string
 * max_len: Maximum length of the output buffer
 * returns: true if successful, false otherwise
 */
static bool extract_json_string(const char* json_value, char* out_str, size_t max_len) {
    if (!json_value || !out_str || max_len == 0) {
        return false;
    }
    
    // Clear the output buffer
    out_str[0] = '\0';
    
    // Skip whitespace
    while (isspace(*json_value)) json_value++;
    
    // Check for quotes
    if (*json_value != '"') {
        return false;
    }
    
    // Skip the opening quote
    json_value++;
    
    // Copy characters up to the closing quote
    size_t i = 0;
    while (*json_value && *json_value != '"' && i < max_len - 1) {
        // Handle escape sequences
        if (*json_value == '\\' && *(json_value + 1)) {
            json_value++;  // Skip the backslash
            
            // Map escaped characters
            switch (*json_value) {
                case 'n': out_str[i++] = '\n'; break;
                case 'r': out_str[i++] = '\r'; break;
                case 't': out_str[i++] = '\t'; break;
                case 'b': out_str[i++] = '\b'; break;
                case 'f': out_str[i++] = '\f'; break;
                case '\\': out_str[i++] = '\\'; break;
                case '/': out_str[i++] = '/'; break;
                case '"': out_str[i++] = '"'; break;
                case 'u': 
                    // Skip unicode escapes
                    json_value += 4;
                    break;
                default:
                    out_str[i++] = *json_value;
                    break;
            }
        } else {
            out_str[i++] = *json_value;
        }
        
        json_value++;
    }
    
    // Null-terminate the string
    out_str[i] = '\0';
    
    // Make sure we found the closing quote
    return *json_value == '"';
}

/**
 * Helper function to extract a boolean value from a JSON field.
 * 
 * json_value: Pointer to the JSON value (after the field name and colon)
 * out_bool: Pointer to store the extracted boolean value
 * returns: true if successful, false otherwise
 */
static bool extract_json_bool(const char* json_value, bool* out_bool) {
    if (!json_value || !out_bool) {
        return false;
    }
    
    // Skip whitespace
    while (isspace(*json_value)) json_value++;
    
    // Check for "true" or "false"
    if (strncmp(json_value, "true", 4) == 0) {
        *out_bool = true;
        return true;
    } else if (strncmp(json_value, "false", 5) == 0) {
        *out_bool = false;
        return true;
    }
    
    return false;
}

/**
 * Helper function to extract an integer value from a JSON field.
 * 
 * json_value: Pointer to the JSON value (after the field name and colon)
 * out_int: Pointer to store the extracted integer value
 * returns: true if successful, false otherwise
 */
static bool extract_json_int(const char* json_value, int* out_int) {
    if (!json_value || !out_int) {
        return false;
    }
    
    // Skip whitespace
    while (isspace(*json_value)) json_value++;
    
    // Check if it's a number
    if (!isdigit(*json_value) && *json_value != '-') {
        return false;
    }
    
    // Parse the number
    char* end;
    long value = strtol(json_value, &end, 10);
    
    // Make sure we read a complete number
    if (end == json_value) {
        return false;
    }
    
    *out_int = (int)value;
    return true;
}

/**
 * Helper function to find the matching closing bracket for an opening '{'.
 * 
 * start: Pointer to the opening '{' character
 * returns: Pointer to the matching '}', or NULL if not found
 */
static const char* find_matching_bracket(const char* start) {
    if (!start || *start != '{') {
        return NULL;
    }
    
    int depth = 1;
    const char* p = start + 1;
    
    while (*p && depth > 0) {
        if (*p == '{') {
            depth++;
        } else if (*p == '}') {
            depth--;
        } else if (*p == '"') {
            // Skip quoted strings
            p++;
            while (*p && *p != '"') {
                // Handle escaped characters
                if (*p == '\\' && *(p + 1)) {
                    p += 2;
                } else {
                    p++;
                }
            }
            
            if (!*p) {
                // Unterminated string
                return NULL;
            }
        }
        
        if (depth > 0) {
            p++;
        }
    }
    
    return depth == 0 ? p : NULL;
}

/**
 * Parse custom metadata configuration from a JSON file
 * 
 * This function reads and processes a JSON configuration file to extract
 * custom metadata definitions.
 * 
 * config_file: Path to the configuration file
 * custom_metadata: Pointer to store the array of custom metadata items
 * item_count: Pointer to store the number of items read
 * returns: Error code (CUSTOM_METADATA_OK on success)
 */
CustomMetadataError custom_metadata_parse_config(
    const char* config_file,
    CustomMetadataItem** custom_metadata,
    uint32_t* item_count
) {
    if (!config_file || !custom_metadata || !item_count) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid parameters: config_file or output pointers are NULL");
        return CUSTOM_METADATA_INVALID_PARAMETER;
    }
    
    // Initialize output parameters
    *custom_metadata = NULL;
    *item_count = 0;
    
    // Open and read the configuration file
    FILE* file = fopen(config_file, "r");
    if (!file) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to open configuration file: %s", config_file);
        return CUSTOM_METADATA_FILE_ERROR;
    }
    
    // Get the file size
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    
    // Allocate memory for the file contents
    char* file_contents = (char*)malloc(file_size + 1);
    if (!file_contents) {
        fclose(file);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for file contents");
        return CUSTOM_METADATA_MEMORY_ERROR;
    }
    
    // Read the file contents
    size_t bytes_read = fread(file_contents, 1, file_size, file);
    fclose(file);
    
    if (bytes_read != file_size) {
        free(file_contents);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to read configuration file: %s", config_file);
        return CUSTOM_METADATA_FILE_ERROR;
    }
    
    // Null-terminate the file contents
    file_contents[file_size] = '\0';
    
    // Find the custom_metadata array in the JSON
    const char* custom_metadata_field = find_json_field(file_contents, "custom_metadata");
    if (!custom_metadata_field) {
        free(file_contents);
        snprintf(s_error_message, sizeof(s_error_message),
                "Configuration file does not contain custom_metadata array");
        return CUSTOM_METADATA_INVALID_FORMAT;
    }
    
    // Count the number of items in the array
    uint32_t count = 0;
    const char* p = custom_metadata_field;
    
    // First, count the items by counting the opening braces at the start of objects
    while ((p = strchr(p, '{')) != NULL) {
        count++;
        p++;
    }
    
    if (count == 0) {
        free(file_contents);
        *item_count = 0;
        return CUSTOM_METADATA_OK; // Empty array is valid
    }
    
    // Allocate memory for the custom metadata items
    CustomMetadataItemExt* ext_items = (CustomMetadataItemExt*)malloc(count * sizeof(CustomMetadataItemExt));
    if (!ext_items) {
        free(file_contents);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for custom metadata items");
        return CUSTOM_METADATA_MEMORY_ERROR;
    }
    
    // Initialize the memory
    memset(ext_items, 0, count * sizeof(CustomMetadataItemExt));
    
    // Parse each item in the array
    p = custom_metadata_field;
    uint32_t parsed_count = 0;
    
    while (parsed_count < count) {
        // Find the start of the item
        p = strchr(p, '{');
        if (!p) break;
        
        // Find the end of this item
        const char* item_end = find_matching_bracket(p);
        if (!item_end) break;
        
        // Extract the item JSON
        size_t item_len = item_end - p + 1;
        char* item_json = (char*)malloc(item_len + 1);
        if (!item_json) {
            free(file_contents);
            free(ext_items);
            *custom_metadata = NULL;
            snprintf(s_error_message, sizeof(s_error_message),
                    "Failed to allocate memory for item JSON");
            return CUSTOM_METADATA_MEMORY_ERROR;
        }
        
        strncpy(item_json, p, item_len);
        item_json[item_len] = '\0';
        
        CustomMetadataItemExt* item = &ext_items[parsed_count];
        
        // Parse name
        const char* name_field = find_json_field(item_json, "name");
        if (name_field) {
            extract_json_string(name_field, item->base.name, MAX_METADATA_ITEM_NAME_LENGTH);
        } else {
            // Name is required
            snprintf(item->base.name, MAX_METADATA_ITEM_NAME_LENGTH, "Custom_%u", parsed_count);
        }
        
        // Parse SQL query
        const char* sql_field = find_json_field(item_json, "sql_query");
        if (sql_field) {
            extract_json_string(sql_field, item->base.sql_query, MAX_SQL_QUERY_LENGTH);
        } else {
            // SQL query is required
            free(item_json);
            free(file_contents);
            free(ext_items);
            *custom_metadata = NULL;
            snprintf(s_error_message, sizeof(s_error_message),
                    "Missing SQL query for custom metadata item: %s", item->base.name);
            return CUSTOM_METADATA_INVALID_FORMAT;
        }
        
        // Parse description (optional)
        const char* desc_field = find_json_field(item_json, "description");
        if (desc_field) {
            char description[MAX_STRING_LENGTH];
            if (extract_json_string(desc_field, description, MAX_STRING_LENGTH)) {
                item->description = strdup(description);
                // Note: memory will be freed by custom_metadata_free_items
            }
        }
        
        // Parse target (file, row_group, or column)
        const char* target_field = find_json_field(item_json, "target");
        if (target_field) {
            char target_str[32];
            if (extract_json_string(target_field, target_str, sizeof(target_str))) {
                if (strcmp(target_str, "file") == 0) {
                    item->target = CUSTOM_METADATA_TARGET_FILE;
                } else if (strcmp(target_str, "row_group") == 0) {
                    item->target = CUSTOM_METADATA_TARGET_ROW_GROUP;
                } else if (strcmp(target_str, "column") == 0) {
                    item->target = CUSTOM_METADATA_TARGET_COLUMN;
                } else {
                    // Default to file
                    item->target = CUSTOM_METADATA_TARGET_FILE;
                }
            }
        } else {
            // Default to file
            item->target = CUSTOM_METADATA_TARGET_FILE;
        }
        
        // Parse options (optional)
        const char* options_field = find_json_field(item_json, "options");
        if (options_field) {
            // Find cache_results
            const char* cache_field = find_json_field(options_field, "cache_results");
            if (cache_field) {
                bool cache_results;
                if (extract_json_bool(cache_field, &cache_results)) {
                    item->cache_results = cache_results;
                }
            }
            
            // Find update_frequency
            const char* update_field = find_json_field(options_field, "update_frequency");
            if (update_field) {
                char update_str[32];
                if (extract_json_string(update_field, update_str, sizeof(update_str))) {
                    if (strcmp(update_str, "read") == 0) {
                        item->update_frequency = CUSTOM_METADATA_UPDATE_ON_READ;
                    } else if (strcmp(update_str, "write") == 0) {
                        item->update_frequency = CUSTOM_METADATA_UPDATE_ON_WRITE;
                    } else if (strcmp(update_str, "manual") == 0) {
                        item->update_frequency = CUSTOM_METADATA_UPDATE_MANUAL;
                    } else {
                        // Default
                        item->update_frequency = CUSTOM_METADATA_UPDATE_ON_READ;
                    }
                }
            }
        }
        
        free(item_json);
        parsed_count++;
        p = item_end + 1;
    }
    
    free(file_contents);
    
    // Extract only the base CustomMetadataItem into the output array
    *custom_metadata = (CustomMetadataItem*)malloc(parsed_count * sizeof(CustomMetadataItem));
    if (!*custom_metadata) {
        free(ext_items);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for output custom metadata items");
        return CUSTOM_METADATA_MEMORY_ERROR;
    }
    
    // Copy only the base part of each item
    for (uint32_t i = 0; i < parsed_count; i++) {
        memcpy(&(*custom_metadata)[i], &ext_items[i].base, sizeof(CustomMetadataItem));
        
        // Handle dynamically allocated memory
        if (ext_items[i].description) {
            free(ext_items[i].description);
        }
    }
    
    free(ext_items);
    *item_count = parsed_count;
    
    return CUSTOM_METADATA_OK;
}

/**
 * Check if a column contains NULL values
 * 
 * This function checks if a column contains any NULL values.
 * Used for the "has_null" custom metadata item.
 * 
 * reader_context: Context for reading the parquet file
 * file: Parquet file structure
 * row_group_id: ID of the row group to check
 * column_id: ID of the column to check
 * result: Pointer to store the result (1 if has nulls, 0 if not)
 * returns: Error code (0 on success, non-zero on failure)
 */
static int check_column_has_null(
    ParquetReaderContext* reader_context,
    const ParquetFile* file,
    int row_group_id,
    int column_id,
    int* result
) {
    if (!reader_context || !file || !result) {
        return -1;
    }
    
    // Default to no nulls
    *result = 0;
    
    // Read the column data
    void* buffer;
    size_t buffer_size;
    
    ParquetReaderError reader_error = parquet_reader_read_column(
        reader_context,
        row_group_id,
        column_id,
        &buffer,
        &buffer_size
    );
    
    if (reader_error != PARQUET_READER_OK) {
        return -2;
    }
    
    // If buffer is NULL or empty, consider it a NULL column
    if (!buffer || buffer_size == 0) {
        *result = 1;
        if (buffer) {
            parquet_reader_free_buffer(buffer);
        }
        return 0;
    }
    
    // Get the column information
    if (row_group_id >= 0 && row_group_id < file->row_group_count &&
        column_id >= 0 && column_id < file->row_groups[row_group_id].column_count) {
        
        const ParquetColumn* column = &file->row_groups[row_group_id].columns[column_id];
        
        // Check for nulls based on the column type
        switch (column->type) {
            case PARQUET_BOOLEAN: {
                // For boolean, check for null representation based on the specific format
                // This may vary depending on your implementation
                // For simplicity, we'll scan for a specific bit pattern
                const uint8_t* data = (const uint8_t*)buffer;
                for (size_t i = 0; i < buffer_size; i++) {
                    // If any null indicator bit is set
                    if ((data[i] & 0x80) != 0) {  // Check for null indicator bit
                        *result = 1;
                        break;
                    }
                }
                break;
            }
            
            case PARQUET_INT32: {
                // For int32, check for the minimum value as null marker
                const int32_t* data = (const int32_t*)buffer;
                size_t count = buffer_size / sizeof(int32_t);
                for (size_t i = 0; i < count; i++) {
                    if (data[i] == INT32_MIN) {
                        *result = 1;
                        break;
                    }
                }
                break;
            }
            
            case PARQUET_INT64: {
                // For int64, check for the minimum value as null marker
                const int64_t* data = (const int64_t*)buffer;
                size_t count = buffer_size / sizeof(int64_t);
                for (size_t i = 0; i < count; i++) {
                    if (data[i] == INT64_MIN) {
                        *result = 1;
                        break;
                    }
                }
                break;
            }
            
            case PARQUET_FLOAT: {
                // For float, check for NaN as null marker
                const float* data = (const float*)buffer;
                size_t count = buffer_size / sizeof(float);
                for (size_t i = 0; i < count; i++) {
                    if (isnan(data[i])) {
                        *result = 1;
                        break;
                    }
                }
                break;
            }
            
            case PARQUET_DOUBLE: {
                // For double, check for NaN as null marker
                const double* data = (const double*)buffer;
                size_t count = buffer_size / sizeof(double);
                for (size_t i = 0; i < count; i++) {
                    if (isnan(data[i])) {
                        *result = 1;
                        break;
                    }
                }
                break;
            }
            
            case PARQUET_STRING:
            case PARQUET_BYTE_ARRAY: {
                // For string data, check for zero-length strings as null marker
                // assuming format: uint32_t length + data
                const uint8_t* data = (const uint8_t*)buffer;
                size_t offset = 0;
                
                while (offset + sizeof(uint32_t) <= buffer_size) {
                    uint32_t length;
                    memcpy(&length, data + offset, sizeof(uint32_t));
                    offset += sizeof(uint32_t);
                    
                    if (length == 0) {
                        *result = 1;
                        break;
                    }
                    
                    offset += length;
                }
                break;
            }
            
            case PARQUET_FIXED_LEN_BYTE_ARRAY: {
                // For fixed length byte arrays, check for all zeros as null marker
                const uint8_t* data = (const uint8_t*)buffer;
                
                // Get the fixed length
                int fixed_len = column->fixed_len_byte_array_size;
                if (fixed_len <= 0) {
                    fixed_len = 16;  // Fallback to default if not specified
                }
                
                size_t count = buffer_size / fixed_len;
                for (size_t i = 0; i < count; i++) {
                    // Check if all bytes are zero
                    bool all_zeros = true;
                    for (int j = 0; j < fixed_len; j++) {
                        if (data[i * fixed_len + j] != 0) {
                            all_zeros = false;
                            break;
                        }
                    }
                    
                    if (all_zeros) {
                        *result = 1;
                        break;
                    }
                }
                break;
            }
            
            default:
                // For other types, we can't easily detect nulls
                // A more complete implementation would need to handle all types
                break;
        }
    }
    
    // Free the column data buffer
    parquet_reader_free_buffer(buffer);
    
    return 0;
}

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
) {
    if (!file || !reader_context || !items || count <= 0 || count > MAX_CUSTOM_METADATA_ITEMS) {
        return CUSTOM_METADATA_INVALID_PARAMETER;
    }
    
    // Process each custom metadata item
    for (int i = 0; i < count; i++) {
        // Determine the total size needed for the results string
        // Format: {{r1c1,r1c2,...}{r2c1,r2c2,...}...}
        int max_size = 2;  // Initial and final braces
        
        for (uint32_t j = 0; j < file->row_group_count; j++) {
            max_size += 2;  // Braces around each row group
            
            const ParquetRowGroup* row_group = &file->row_groups[j];
            max_size += row_group->column_count;  // For the 0/1 values
            max_size += row_group->column_count - 1;  // For the commas
        }
        
        // Allocate memory for the results string
        items[i].result_matrix = (char*)malloc(max_size + 1);  // +1 for null terminator
        if (!items[i].result_matrix) {
            // Free any previously allocated results
            for (int j = 0; j < i; j++) {
                if (items[j].result_matrix) {
                    free(items[j].result_matrix);
                    items[j].result_matrix = NULL;
                }
            }
            
            snprintf(s_error_message, sizeof(s_error_message),
                    "Failed to allocate memory for custom metadata results");
            return CUSTOM_METADATA_MEMORY_ERROR;
        }
        
        // Update counts
        items[i].row_group_count = file->row_group_count;
        
        // Build the results string
        char* p = items[i].result_matrix;
        *p++ = '{';
        
        for (uint32_t j = 0; j < file->row_group_count; j++) {
            const ParquetRowGroup* row_group = &file->row_groups[j];
            *p++ = '{';
            
            // Update column count for this item
            items[i].column_count = row_group->column_count > items[i].column_count ? 
                                    row_group->column_count : items[i].column_count;
            
            for (uint32_t k = 0; k < row_group->column_count; k++) {
                int result = 0;
                
                // Process based on SQL query - this is a simplified approach
                // In a real implementation, we would parse and execute the SQL query
                
                // Check for "has_null" query as example
                if (strstr(items[i].sql_query, "has_null") != NULL) {
                    check_column_has_null(reader_context, file, j, k, &result);
                }
                // Add more query types here...
                
                // Add the result to the matrix
                *p++ = result ? '1' : '0';
                
                // Add separator comma (except for the last column)
                if (k < row_group->column_count - 1) {
                    *p++ = ',';
                }
            }
            
            *p++ = '}';
            
            // Add separator comma (except for the last row group)
            if (j < file->row_group_count - 1) {
                *p++ = ',';
            }
        }
        
        *p++ = '}';
        *p = '\0';
    }
    
    return CUSTOM_METADATA_OK;
}

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
) {
    if (!items || count <= 0 || count > MAX_CUSTOM_METADATA_ITEMS) {
        return;
    }
    
    for (int i = 0; i < count; i++) {
        if (items[i].result_matrix) {
            free(items[i].result_matrix);
            items[i].result_matrix = NULL;
        }
        
        // We no longer handle description here since it belongs to CustomMetadataItemExt
    }
}

/**
 * Get the last error message from custom metadata operations
 * 
 * Returns a pointer to a static string containing the last error message.
 * This string should not be freed by the caller.
 * 
 * returns: Pointer to the error message string, or NULL if no error has occurred
 */
const char* custom_metadata_get_error() {
    return s_error_message[0] != '\0' ? s_error_message : NULL;
} 