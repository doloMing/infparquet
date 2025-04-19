#include "metadata/json_serialization.h"
#include "metadata/metadata_types.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <ctype.h>
#include <stdbool.h>

/* Constants */
#define MAX_METADATA_JSON_SIZE (64 * 1024)  // 64 KB max size for JSON
#define MAX_FILE_PATH_LENGTH 256

/* Static global for error messages */
static char s_error_message[256];

/**
 * Structure for JSON serialization context
 */
typedef struct {
    char* buffer;         // Buffer for JSON data
    size_t buffer_size;   // Size of the buffer
    size_t position;      // Current position in the buffer
} JSONContext;

/**
 * Internal helper function to serialize a single metadata item to JSON
 */
static char* serialize_metadata_item(const MetadataItem* item, int indent_level) {
    if (!item) {
        return strdup("null");
    }
    
    char* buffer = (char*)malloc(MAX_METADATA_JSON_SIZE);
    if (!buffer) {
        return NULL;
    }
    
    // Create indentation
    char indent[32] = "";
    for (int i = 0; i < indent_level && i < 15; i++) {
        strcat(indent, "  ");
    }
    
    // Format depends on type
    switch (item->type) {
        case METADATA_TYPE_TIMESTAMP: {
            // Format the timestamps as ISO 8601 strings
            char min_time_str[32], max_time_str[32];
            struct tm* tm_info;
            
            tm_info = localtime(&item->value.timestamp.min_timestamp);
            strftime(min_time_str, sizeof(min_time_str), "%Y-%m-%dT%H:%M:%S", tm_info);
            
            tm_info = localtime(&item->value.timestamp.max_timestamp);
            strftime(max_time_str, sizeof(max_time_str), "%Y-%m-%dT%H:%M:%S", tm_info);
            
            snprintf(buffer, MAX_METADATA_JSON_SIZE,
                    "%s{\n"
                    "%s  \"name\": \"%s\",\n"
                    "%s  \"type\": \"timestamp\",\n"
                    "%s  \"min_timestamp\": \"%s\",\n"
                    "%s  \"max_timestamp\": \"%s\",\n"
                    "%s  \"count\": %llu\n"
                    "%s}",
                    indent,
                    indent, item->name,
                    indent,
                    indent, min_time_str,
                    indent, max_time_str,
                    indent, (unsigned long long)item->value.timestamp.count,
                    indent);
            break;
        }
        
        case METADATA_TYPE_STRING: {
            // Start building the JSON object
            int pos = snprintf(buffer, MAX_METADATA_JSON_SIZE,
                    "%s{\n"
                    "%s  \"name\": \"%s\",\n"
                    "%s  \"type\": \"string\",\n"
                    "%s  \"total_count\": %llu,\n"
                    "%s  \"avg_length\": %u,\n"
                    "%s  \"high_freq_strings\": [",
                    indent,
                    indent, item->name,
                    indent,
                    indent, (unsigned long long)item->value.string.total_string_count,
                    indent, item->value.string.avg_string_length,
                    indent);
            
            // Add high frequency strings
            for (uint32_t i = 0; i < item->value.string.high_freq_count; i++) {
                pos += snprintf(buffer + pos, MAX_METADATA_JSON_SIZE - pos,
                        "%s\n%s    {\n"
                        "%s      \"string\": \"%s\",\n"
                        "%s      \"count\": %u\n"
                        "%s    }%s",
                        i == 0 ? "" : ",",
                        indent,
                        indent, item->value.string.high_freq_strings[i],
                        indent, item->value.string.high_freq_counts[i],
                        indent,
                        i == item->value.string.high_freq_count - 1 ? "" : ",");
            }
            
            // Add special strings
            pos += snprintf(buffer + pos, MAX_METADATA_JSON_SIZE - pos,
                    "\n%s  ],\n"
                    "%s  \"special_strings\": [",
                    indent,
                    indent);
            
            for (uint32_t i = 0; i < item->value.string.special_string_count; i++) {
                pos += snprintf(buffer + pos, MAX_METADATA_JSON_SIZE - pos,
                        "%s\n%s    {\n"
                        "%s      \"string\": \"%s\",\n"
                        "%s      \"count\": %u\n"
                        "%s    }%s",
                        i == 0 ? "" : ",",
                        indent,
                        indent, item->value.string.special_strings[i],
                        indent, item->value.string.special_string_counts[i],
                        indent,
                        i == item->value.string.special_string_count - 1 ? "" : ",");
            }
            
            // Close the JSON object
            snprintf(buffer + pos, MAX_METADATA_JSON_SIZE - pos,
                    "\n%s  ]\n"
                    "%s}",
                    indent,
                    indent);
            break;
        }
        
        case METADATA_TYPE_NUMERIC: {
            snprintf(buffer, MAX_METADATA_JSON_SIZE,
                    "%s{\n"
                    "%s  \"name\": \"%s\",\n"
                    "%s  \"type\": \"numeric\",\n"
                    "%s  \"min\": %.6f,\n"
                    "%s  \"max\": %.6f,\n"
                    "%s  \"avg\": %.6f,\n"
                    "%s  \"mode\": %.6f,\n"
                    "%s  \"mode_count\": %llu,\n"
                    "%s  \"total_count\": %llu,\n"
                    "%s  \"null_count\": %u\n"
                    "%s}",
                    indent,
                    indent, item->name,
                    indent,
                    indent, item->value.numeric.min_value,
                    indent, item->value.numeric.max_value,
                    indent, item->value.numeric.avg_value,
                    indent, item->value.numeric.mode_value,
                    indent, (unsigned long long)item->value.numeric.mode_count,
                    indent, (unsigned long long)item->value.numeric.total_count,
                    indent, item->value.numeric.null_count,
                    indent);
            break;
        }
        
        case METADATA_TYPE_CATEGORICAL: {
            // Start building the JSON object
            int pos = snprintf(buffer, MAX_METADATA_JSON_SIZE,
                    "%s{\n"
                    "%s  \"name\": \"%s\",\n"
                    "%s  \"type\": \"categorical\",\n"
                    "%s  \"total_count\": %llu,\n"
                    "%s  \"total_categories\": %u,\n"
                    "%s  \"categories\": [",
                    indent,
                    indent, item->name,
                    indent,
                    indent, (unsigned long long)item->value.categorical.total_value_count,
                    indent, item->value.categorical.total_category_count,
                    indent);
            
            // Add categories
            for (uint32_t i = 0; i < item->value.categorical.high_freq_category_count; i++) {
                pos += snprintf(buffer + pos, MAX_METADATA_JSON_SIZE - pos,
                        "%s\n%s    {\n"
                        "%s      \"category\": \"%s\",\n"
                        "%s      \"count\": %u\n"
                        "%s    }%s",
                        i == 0 ? "" : ",",
                        indent,
                        indent, item->value.categorical.categories[i],
                        indent, item->value.categorical.category_counts[i],
                        indent,
                        i == item->value.categorical.high_freq_category_count - 1 ? "" : ",");
            }
            
            // Close the JSON object
            snprintf(buffer + pos, MAX_METADATA_JSON_SIZE - pos,
                    "\n%s  ]\n"
                    "%s}",
                    indent,
                    indent);
            break;
        }
        
        case METADATA_TYPE_CUSTOM:
        default:
            // For unsupported or custom types, just output a simple JSON object
            snprintf(buffer, MAX_METADATA_JSON_SIZE,
                    "%s{\n"
                    "%s  \"name\": \"%s\",\n"
                    "%s  \"type\": \"custom\"\n"
                    "%s}",
                    indent,
                    indent, item->name,
                    indent,
                    indent);
            break;
    }
    
    return buffer;
}

/**
 * Serializes metadata to a JSON string
 * 
 * This function converts the metadata structure to a JSON string.
 * The caller is responsible for freeing the returned string.
 * 
 * metadata: Pointer to the metadata structure to serialize
 * 
 * Return: A newly allocated string containing the JSON representation,
 *         or NULL if serialization fails
 */
char* metadata_to_json(const Metadata* metadata) {
    if (!metadata) {
        return NULL;
    }
    
    // Allocate a buffer for the JSON string
    char* json = (char*)malloc(MAX_METADATA_JSON_SIZE);
    if (!json) {
        return NULL;
    }
    
    // Start building the JSON object
    int pos = snprintf(json, MAX_METADATA_JSON_SIZE,
            "{\n"
            "  \"file_path\": \"%s\",\n"
            "  \"file_metadata\": {\n"
            "    \"basic_metadata_count\": %u,\n"
            "    \"custom_metadata_count\": %u,\n"
            "    \"use_basic_metadata\": %s,\n"
            "    \"basic_metadata\": [",
            metadata->file_path ? metadata->file_path : "",
            metadata->file_metadata.basic_metadata_count,
            metadata->file_metadata.custom_metadata_count,
            metadata->file_metadata.use_basic_metadata ? "true" : "false");
    
    // Add basic metadata items
    for (uint32_t i = 0; i < metadata->file_metadata.basic_metadata_count; i++) {
        char* item_json = serialize_metadata_item(&metadata->file_metadata.basic_metadata[i], 3);
        if (item_json) {
            pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                    "%s\n%s",
                    i == 0 ? "" : ",",
                    item_json);
            free(item_json);
        }
    }
    
    // Add custom metadata items
    pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
            "\n    ],\n"
            "    \"custom_metadata\": [");
    
    for (uint32_t i = 0; i < metadata->file_metadata.custom_metadata_count; i++) {
        const CustomMetadataItem* item = &metadata->file_metadata.custom_metadata[i];
        pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                "%s\n      {\n"
                "        \"name\": \"%s\",\n"
                "        \"sql_query\": \"%s\",\n"
                "        \"row_group_count\": %u,\n"
                "        \"column_count\": %u\n"
                "      }%s",
                i == 0 ? "" : ",",
                item->name,
                item->sql_query,
                item->row_group_count,
                item->column_count,
                i == metadata->file_metadata.custom_metadata_count - 1 ? "" : ",");
    }
    
    // Add row group metadata
    pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
            "\n    ]\n"
            "  },\n"
            "  \"row_group_metadata_count\": %u,\n"
            "  \"row_group_metadata\": [",
            metadata->row_group_metadata_count);
    
    for (uint32_t i = 0; i < metadata->row_group_metadata_count; i++) {
        const RowGroupMetadata* row_group = &metadata->row_group_metadata[i];
        pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                "%s\n    {\n"
                "      \"row_group_index\": %u,\n"
                "      \"metadata_count\": %u,\n"
                "      \"metadata\": [",
                i == 0 ? "" : ",",
                row_group->row_group_index,
                row_group->metadata_count);
        
        // Add row group metadata items
        for (uint32_t j = 0; j < row_group->metadata_count; j++) {
            char* item_json = serialize_metadata_item(&row_group->metadata[j], 4);
            if (item_json) {
                pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                        "%s\n%s",
                        j == 0 ? "" : ",",
                        item_json);
                free(item_json);
            }
        }
        
        // Close the row group
        pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                "\n      ]\n"
                "    }");
    }
    
    // Add column metadata
    pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
            "\n  ],\n"
            "  \"column_metadata_count\": %u,\n"
            "  \"column_metadata\": [",
            metadata->column_metadata_count);
    
    for (uint32_t i = 0; i < metadata->column_metadata_count; i++) {
        const ColumnMetadata* column = &metadata->column_metadata[i];
        pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                "%s\n    {\n"
                "      \"column_index\": %u,\n"
                "      \"column_name\": \"%s\",\n"
                "      \"metadata_count\": %u,\n"
                "      \"metadata\": [",
                i == 0 ? "" : ",",
                column->column_index,
                column->column_name,
                column->metadata_count);
        
        // Add column metadata items
        for (uint32_t j = 0; j < column->metadata_count; j++) {
            char* item_json = serialize_metadata_item(&column->metadata[j], 4);
            if (item_json) {
                pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                        "%s\n%s",
                        j == 0 ? "" : ",",
                        item_json);
                free(item_json);
            }
        }
        
        // Close the column
        pos += snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
                "\n      ]\n"
                "    }");
    }
    
    // Close the main JSON object
    snprintf(json + pos, MAX_METADATA_JSON_SIZE - pos,
            "\n  ]\n"
            "}");
    
    return json;
}

/**
 * Deserialize metadata from a JSON string
 * 
 * This function parses a JSON string into a metadata structure.
 * The caller is responsible for freeing the returned metadata using
 * metadata_generator_free_metadata.
 * 
 * json_string: JSON string to parse
 * metadata: Pointer to store the parsed metadata structure
 * returns: Error code (JSON_SERIALIZATION_OK on success)
 */
JsonSerializationError json_serialization_json_to_metadata(
    const char* json_string,
    Metadata** metadata
) {
    if (!json_string || !metadata) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid parameters: json_string or metadata is NULL");
        return JSON_SERIALIZATION_INVALID_PARAMETER;
    }
    
    // Allocate memory for the metadata structure
    *metadata = (Metadata*)malloc(sizeof(Metadata));
    if (!*metadata) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for metadata structure");
        return JSON_SERIALIZATION_MEMORY_ERROR;
    }
    
    // Clear the structure to initialize all fields to zero
    memset(*metadata, 0, sizeof(Metadata));
    
    // Parse file path
    const char* file_path_field = find_json_field(json_string, "file_path");
    if (file_path_field) {
        char file_path[MAX_FILE_PATH_LENGTH];
        if (extract_json_string(file_path_field, file_path, sizeof(file_path))) {
            (*metadata)->file_path = strdup(file_path);
            if (!(*metadata)->file_path) {
                metadata_generator_free_metadata(*metadata);
                *metadata = NULL;
                snprintf(s_error_message, sizeof(s_error_message),
                        "Failed to allocate memory for file path");
                return JSON_SERIALIZATION_MEMORY_ERROR;
            }
        }
    } else {
        // File path is required
        (*metadata)->file_path = strdup("");
    }
    
    // Parse file metadata
    const char* file_metadata_field = find_json_field(json_string, "file_metadata");
    if (file_metadata_field) {
        // Find basic_metadata_count
        const char* count_field = find_json_field(file_metadata_field, "basic_metadata_count");
        if (count_field) {
            int count;
            if (extract_json_int(count_field, &count)) {
                (*metadata)->file_metadata.basic_metadata_count = count;
            }
        }
        
        // Find custom_metadata_count
        count_field = find_json_field(file_metadata_field, "custom_metadata_count");
        if (count_field) {
            int count;
            if (extract_json_int(count_field, &count)) {
                (*metadata)->file_metadata.custom_metadata_count = count;
            }
        }
        
        // Find use_basic_metadata
        const char* use_basic_field = find_json_field(file_metadata_field, "use_basic_metadata");
        if (use_basic_field) {
            bool use_basic;
            if (extract_json_bool(use_basic_field, &use_basic)) {
                (*metadata)->file_metadata.use_basic_metadata = use_basic;
            }
        }
        
        // Parse basic metadata
        const char* basic_metadata_field = find_json_field(file_metadata_field, "basic_metadata");
        if (basic_metadata_field && (*metadata)->file_metadata.basic_metadata_count > 0) {
            // Allocate memory for basic metadata items
            (*metadata)->file_metadata.basic_metadata = (MetadataItem*)malloc(
                (*metadata)->file_metadata.basic_metadata_count * sizeof(MetadataItem));
            
            if (!(*metadata)->file_metadata.basic_metadata) {
                metadata_generator_free_metadata(*metadata);
                *metadata = NULL;
                snprintf(s_error_message, sizeof(s_error_message),
                        "Failed to allocate memory for basic metadata items");
                return JSON_SERIALIZATION_MEMORY_ERROR;
            }
            
            // Clear the memory
            memset((*metadata)->file_metadata.basic_metadata, 0,
                   (*metadata)->file_metadata.basic_metadata_count * sizeof(MetadataItem));
            
            // Parse each basic metadata item
            const char* p = basic_metadata_field;
            int item_count = 0;
            
            while (item_count < (*metadata)->file_metadata.basic_metadata_count) {
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
                    metadata_generator_free_metadata(*metadata);
                    *metadata = NULL;
                    snprintf(s_error_message, sizeof(s_error_message),
                            "Failed to allocate memory for item JSON");
                    return JSON_SERIALIZATION_MEMORY_ERROR;
                }
                
                strncpy(item_json, p, item_len);
                item_json[item_len] = '\0';
                
                MetadataItem* item = &(*metadata)->file_metadata.basic_metadata[item_count];
                
                // Parse name
                const char* name_field = find_json_field(item_json, "name");
                if (name_field) {
                    extract_json_string(name_field, item->name, MAX_METADATA_ITEM_NAME_LENGTH);
                }
                
                // Parse type
                const char* type_field = find_json_field(item_json, "type");
                if (type_field) {
                    char type_str[32];
                    if (extract_json_string(type_field, type_str, sizeof(type_str))) {
                        if (strcmp(type_str, "timestamp") == 0) {
                            item->type = METADATA_TYPE_TIMESTAMP;
                            
                            // Parse timestamp values
                            const char* min_field = find_json_field(item_json, "min_timestamp");
                            const char* max_field = find_json_field(item_json, "max_timestamp");
                            const char* count_field = find_json_field(item_json, "count");
                            
                            if (min_field) {
                                char min_str[32];
                                if (extract_json_string(min_field, min_str, sizeof(min_str))) {
                                    // Convert string to time_t (simplified)
                                    struct tm tm = {0};
                                    if (strptime(min_str, "%Y-%m-%dT%H:%M:%S", &tm)) {
                                        item->value.timestamp.min_timestamp = mktime(&tm);
                                    }
                                }
                            }
                            
                            if (max_field) {
                                char max_str[32];
                                if (extract_json_string(max_field, max_str, sizeof(max_str))) {
                                    // Convert string to time_t (simplified)
                                    struct tm tm = {0};
                                    if (strptime(max_str, "%Y-%m-%dT%H:%M:%S", &tm)) {
                                        item->value.timestamp.max_timestamp = mktime(&tm);
                                    }
                                }
                            }
                            
                            if (count_field) {
                                uint64_t count;
                                if (extract_json_uint64(count_field, &count)) {
                                    item->value.timestamp.count = count;
                                }
                            }
                        } else if (strcmp(type_str, "string") == 0) {
                            item->type = METADATA_TYPE_STRING;
                            
                            // Parse string values
                            const char* total_count_field = find_json_field(item_json, "total_count");
                            const char* avg_length_field = find_json_field(item_json, "avg_length");
                            
                            if (total_count_field) {
                                uint64_t count;
                                if (extract_json_uint64(total_count_field, &count)) {
                                    item->value.string.total_string_count = count;
                                }
                            }
                            
                            if (avg_length_field) {
                                uint32_t avg_length;
                                if (extract_json_uint32(avg_length_field, &avg_length)) {
                                    item->value.string.avg_string_length = avg_length;
                                }
                            }
                            
                            // Parse high frequency strings
                            const char* high_freq_field = find_json_field(item_json, "high_freq_strings");
                            if (high_freq_field) {
                                parse_string_array(high_freq_field, item->value.string.high_freq_strings, 
                                                 item->value.string.high_freq_counts, 
                                                 MAX_HIGH_FREQ_STRINGS, MAX_STRING_LENGTH,
                                                 &item->value.string.high_freq_count);
                            }
                            
                            // Parse special strings
                            const char* special_field = find_json_field(item_json, "special_strings");
                            if (special_field) {
                                parse_string_array(special_field, item->value.string.special_strings,
                                                 item->value.string.special_string_counts,
                                                 MAX_SPECIAL_STRINGS, MAX_STRING_LENGTH,
                                                 &item->value.string.special_string_count);
                            }
                        } else if (strcmp(type_str, "numeric") == 0) {
                            item->type = METADATA_TYPE_NUMERIC;
                            
                            // Parse numeric values
                            const char* min_field = find_json_field(item_json, "min");
                            const char* max_field = find_json_field(item_json, "max");
                            const char* avg_field = find_json_field(item_json, "avg");
                            const char* mode_field = find_json_field(item_json, "mode");
                            const char* mode_count_field = find_json_field(item_json, "mode_count");
                            const char* total_count_field = find_json_field(item_json, "total_count");
                            const char* null_count_field = find_json_field(item_json, "null_count");
                            
                            if (min_field) extract_json_double(min_field, &item->value.numeric.min_value);
                            if (max_field) extract_json_double(max_field, &item->value.numeric.max_value);
                            if (avg_field) extract_json_double(avg_field, &item->value.numeric.avg_value);
                            if (mode_field) extract_json_double(mode_field, &item->value.numeric.mode_value);
                            
                            if (mode_count_field) {
                                uint64_t count;
                                if (extract_json_uint64(mode_count_field, &count)) {
                                    item->value.numeric.mode_count = count;
                                }
                            }
                            
                            if (total_count_field) {
                                uint64_t count;
                                if (extract_json_uint64(total_count_field, &count)) {
                                    item->value.numeric.total_count = count;
                                }
                            }
                            
                            if (null_count_field) {
                                uint32_t count;
                                if (extract_json_uint32(null_count_field, &count)) {
                                    item->value.numeric.null_count = count;
                                }
                            }
                        } else if (strcmp(type_str, "categorical") == 0) {
                            item->type = METADATA_TYPE_CATEGORICAL;
                            
                            // Parse categorical values
                            const char* total_count_field = find_json_field(item_json, "total_count");
                            const char* total_categories_field = find_json_field(item_json, "total_categories");
                            
                            if (total_count_field) {
                                uint64_t count;
                                if (extract_json_uint64(total_count_field, &count)) {
                                    item->value.categorical.total_value_count = count;
                                }
                            }
                            
                            if (total_categories_field) {
                                uint32_t count;
                                if (extract_json_uint32(total_categories_field, &count)) {
                                    item->value.categorical.total_category_count = count;
                                }
                            }
                            
                            // Parse categories
                            const char* categories_field = find_json_field(item_json, "categories");
                            if (categories_field) {
                                parse_string_array(categories_field, item->value.categorical.categories,
                                                 item->value.categorical.category_counts,
                                                 MAX_HIGH_FREQ_CATEGORIES, MAX_STRING_LENGTH,
                                                 &item->value.categorical.high_freq_category_count);
                            }
                        } else if (strcmp(type_str, "custom") == 0) {
                            item->type = METADATA_TYPE_CUSTOM;
                            // Custom metadata is handled separately
                        }
                    }
                }
                
                free(item_json);
                item_count++;
                p = item_end + 1;
            }
        }
        
        // Parse custom metadata
        const char* custom_metadata_field = find_json_field(file_metadata_field, "custom_metadata");
        if (custom_metadata_field && (*metadata)->file_metadata.custom_metadata_count > 0) {
            // Allocate memory for custom metadata items
            (*metadata)->file_metadata.custom_metadata = (CustomMetadataItem*)malloc(
                (*metadata)->file_metadata.custom_metadata_count * sizeof(CustomMetadataItem));
            
            if (!(*metadata)->file_metadata.custom_metadata) {
                metadata_generator_free_metadata(*metadata);
                *metadata = NULL;
                snprintf(s_error_message, sizeof(s_error_message),
                        "Failed to allocate memory for custom metadata items");
                return JSON_SERIALIZATION_MEMORY_ERROR;
            }
            
            // Clear the memory
            memset((*metadata)->file_metadata.custom_metadata, 0,
                   (*metadata)->file_metadata.custom_metadata_count * sizeof(CustomMetadataItem));
            
            // Parse each custom metadata item
            const char* p = custom_metadata_field;
            int item_count = 0;
            
            while (item_count < (*metadata)->file_metadata.custom_metadata_count) {
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
                    metadata_generator_free_metadata(*metadata);
                    *metadata = NULL;
                    snprintf(s_error_message, sizeof(s_error_message),
                            "Failed to allocate memory for item JSON");
                    return JSON_SERIALIZATION_MEMORY_ERROR;
                }
                
                strncpy(item_json, p, item_len);
                item_json[item_len] = '\0';
                
                CustomMetadataItem* item = &(*metadata)->file_metadata.custom_metadata[item_count];
                
                // Parse name
                const char* name_field = find_json_field(item_json, "name");
                if (name_field) {
                    extract_json_string(name_field, item->name, MAX_METADATA_ITEM_NAME_LENGTH);
                }
                
                // Parse SQL query
                const char* sql_field = find_json_field(item_json, "sql_query");
                if (sql_field) {
                    extract_json_string(sql_field, item->sql_query, MAX_STRING_LENGTH);
                }
                
                // Parse row group count
                const char* row_group_field = find_json_field(item_json, "row_group_count");
                if (row_group_field) {
                    uint32_t count;
                    if (extract_json_uint32(row_group_field, &count)) {
                        item->row_group_count = count;
                    }
                }
                
                // Parse column count
                const char* column_field = find_json_field(item_json, "column_count");
                if (column_field) {
                    uint32_t count;
                    if (extract_json_uint32(column_field, &count)) {
                        item->column_count = count;
                    }
                }
                
                // Parse result matrix if present
                const char* matrix_field = find_json_field(item_json, "result_matrix");
                if (matrix_field) {
                    char matrix_str[MAX_METADATA_JSON_SIZE];
                    if (extract_json_string(matrix_field, matrix_str, sizeof(matrix_str))) {
                        // Allocate memory for the result matrix
                        size_t matrix_size = item->row_group_count * item->column_count;
                        if (matrix_size > 0) {
                            item->result_matrix = (char*)malloc(matrix_size + 1);
                            if (item->result_matrix) {
                                strncpy(item->result_matrix, matrix_str, matrix_size);
                                item->result_matrix[matrix_size] = '\0';
                            }
                        }
                    }
                }
                
                free(item_json);
                item_count++;
                p = item_end + 1;
            }
        }
    }
    
    // Parse row group metadata
    const char* row_group_metadata_count_field = find_json_field(json_string, "row_group_metadata_count");
    if (row_group_metadata_count_field) {
        uint32_t count;
        if (extract_json_uint32(row_group_metadata_count_field, &count)) {
            (*metadata)->row_group_metadata_count = count;
        }
    }
    
    const char* row_group_metadata_field = find_json_field(json_string, "row_group_metadata");
    if (row_group_metadata_field && (*metadata)->row_group_metadata_count > 0) {
        // Allocate memory for row group metadata
        (*metadata)->row_group_metadata = (RowGroupMetadata*)malloc(
            (*metadata)->row_group_metadata_count * sizeof(RowGroupMetadata));
        
        if (!(*metadata)->row_group_metadata) {
            metadata_generator_free_metadata(*metadata);
            *metadata = NULL;
            snprintf(s_error_message, sizeof(s_error_message),
                    "Failed to allocate memory for row group metadata");
            return JSON_SERIALIZATION_MEMORY_ERROR;
        }
        
        // Clear the memory
        memset((*metadata)->row_group_metadata, 0,
               (*metadata)->row_group_metadata_count * sizeof(RowGroupMetadata));
        
        // Parse each row group
        const char* p = row_group_metadata_field;
        int group_count = 0;
        
        while (group_count < (*metadata)->row_group_metadata_count) {
            // Find the start of the row group
            p = strchr(p, '{');
            if (!p) break;
            
            // Find the end of this row group
            const char* group_end = find_matching_bracket(p);
            if (!group_end) break;
            
            // Extract the row group JSON
            size_t group_len = group_end - p + 1;
            char* group_json = (char*)malloc(group_len + 1);
            if (!group_json) {
                metadata_generator_free_metadata(*metadata);
                *metadata = NULL;
                snprintf(s_error_message, sizeof(s_error_message),
                        "Failed to allocate memory for row group JSON");
                return JSON_SERIALIZATION_MEMORY_ERROR;
            }
            
            strncpy(group_json, p, group_len);
            group_json[group_len] = '\0';
            
            RowGroupMetadata* row_group = &(*metadata)->row_group_metadata[group_count];
            
            // Parse row group index
            const char* index_field = find_json_field(group_json, "row_group_index");
            if (index_field) {
                uint32_t index;
                if (extract_json_uint32(index_field, &index)) {
                    row_group->row_group_index = index;
                }
            }
            
            // Parse metadata count
            const char* count_field = find_json_field(group_json, "metadata_count");
            if (count_field) {
                uint32_t metadata_count;
                if (extract_json_uint32(count_field, &metadata_count)) {
                    row_group->metadata_count = metadata_count;
                }
            }
            
            // Parse metadata items
            if (row_group->metadata_count > 0) {
                // Allocate memory for metadata items
                row_group->metadata = (MetadataItem*)malloc(
                    row_group->metadata_count * sizeof(MetadataItem));
                
                if (!row_group->metadata) {
                    free(group_json);
                    metadata_generator_free_metadata(*metadata);
                    *metadata = NULL;
                    snprintf(s_error_message, sizeof(s_error_message),
                            "Failed to allocate memory for row group metadata items");
                    return JSON_SERIALIZATION_MEMORY_ERROR;
                }
                
                // Clear the memory
                memset(row_group->metadata, 0,
                       row_group->metadata_count * sizeof(MetadataItem));
                
                // Find metadata array
                const char* metadata_field = find_json_field(group_json, "metadata");
                if (metadata_field) {
                    // Parse metadata items
                    const char* q = metadata_field;
                    int item_count = 0;
                    
                    while (item_count < row_group->metadata_count) {
                        // Find the start of the item
                        q = strchr(q, '{');
                        if (!q) break;
                        
                        // Find the end of this item
                        const char* item_end = find_matching_bracket(q);
                        if (!item_end) break;
                        
                        // Extract the item JSON
                        size_t item_len = item_end - q + 1;
                        char* item_json = (char*)malloc(item_len + 1);
                        if (!item_json) {
                            free(group_json);
                            metadata_generator_free_metadata(*metadata);
                            *metadata = NULL;
                            snprintf(s_error_message, sizeof(s_error_message),
                                    "Failed to allocate memory for metadata item JSON");
                            return JSON_SERIALIZATION_MEMORY_ERROR;
                        }
                        
                        strncpy(item_json, q, item_len);
                        item_json[item_len] = '\0';
                        
                        MetadataItem* item = &row_group->metadata[item_count];
                        
                        // Parse item (name and type)
                        parse_metadata_item(item_json, item);
                        
                        free(item_json);
                        item_count++;
                        q = item_end + 1;
                    }
                }
            }
            
            free(group_json);
            group_count++;
            p = group_end + 1;
        }
    }
    
    // Parse column metadata
    const char* column_metadata_count_field = find_json_field(json_string, "column_metadata_count");
    if (column_metadata_count_field) {
        uint32_t count;
        if (extract_json_uint32(column_metadata_count_field, &count)) {
            (*metadata)->column_metadata_count = count;
        }
    }
    
    const char* column_metadata_field = find_json_field(json_string, "column_metadata");
    if (column_metadata_field && (*metadata)->column_metadata_count > 0) {
        // Allocate memory for column metadata
        (*metadata)->column_metadata = (ColumnMetadata*)malloc(
            (*metadata)->column_metadata_count * sizeof(ColumnMetadata));
        
        if (!(*metadata)->column_metadata) {
            metadata_generator_free_metadata(*metadata);
            *metadata = NULL;
            snprintf(s_error_message, sizeof(s_error_message),
                    "Failed to allocate memory for column metadata");
            return JSON_SERIALIZATION_MEMORY_ERROR;
        }
        
        // Clear the memory
        memset((*metadata)->column_metadata, 0,
               (*metadata)->column_metadata_count * sizeof(ColumnMetadata));
        
        // Parse each column
        const char* p = column_metadata_field;
        int column_count = 0;
        
        while (column_count < (*metadata)->column_metadata_count) {
            // Find the start of the column
            p = strchr(p, '{');
            if (!p) break;
            
            // Find the end of this column
            const char* column_end = find_matching_bracket(p);
            if (!column_end) break;
            
            // Extract the column JSON
            size_t column_len = column_end - p + 1;
            char* column_json = (char*)malloc(column_len + 1);
            if (!column_json) {
                metadata_generator_free_metadata(*metadata);
                *metadata = NULL;
                snprintf(s_error_message, sizeof(s_error_message),
                        "Failed to allocate memory for column JSON");
                return JSON_SERIALIZATION_MEMORY_ERROR;
            }
            
            strncpy(column_json, p, column_len);
            column_json[column_len] = '\0';
            
            ColumnMetadata* column = &(*metadata)->column_metadata[column_count];
            
            // Parse column index
            const char* index_field = find_json_field(column_json, "column_index");
            if (index_field) {
                uint32_t index;
                if (extract_json_uint32(index_field, &index)) {
                    column->column_index = index;
                }
            }
            
            // Parse column name
            const char* name_field = find_json_field(column_json, "column_name");
            if (name_field) {
                extract_json_string(name_field, column->column_name, MAX_METADATA_ITEM_NAME_LENGTH);
            }
            
            // Parse metadata count
            const char* count_field = find_json_field(column_json, "metadata_count");
            if (count_field) {
                uint32_t metadata_count;
                if (extract_json_uint32(count_field, &metadata_count)) {
                    column->metadata_count = metadata_count;
                }
            }
            
            // Parse metadata items
            if (column->metadata_count > 0) {
                // Allocate memory for metadata items
                column->metadata = (MetadataItem*)malloc(
                    column->metadata_count * sizeof(MetadataItem));
                
                if (!column->metadata) {
                    free(column_json);
                    metadata_generator_free_metadata(*metadata);
                    *metadata = NULL;
                    snprintf(s_error_message, sizeof(s_error_message),
                            "Failed to allocate memory for column metadata items");
                    return JSON_SERIALIZATION_MEMORY_ERROR;
                }
                
                // Clear the memory
                memset(column->metadata, 0,
                       column->metadata_count * sizeof(MetadataItem));
                
                // Find metadata array
                const char* metadata_field = find_json_field(column_json, "metadata");
                if (metadata_field) {
                    // Parse metadata items
                    const char* q = metadata_field;
                    int item_count = 0;
                    
                    while (item_count < column->metadata_count) {
                        // Find the start of the item
                        q = strchr(q, '{');
                        if (!q) break;
                        
                        // Find the end of this item
                        const char* item_end = find_matching_bracket(q);
                        if (!item_end) break;
                        
                        // Extract the item JSON
                        size_t item_len = item_end - q + 1;
                        char* item_json = (char*)malloc(item_len + 1);
                        if (!item_json) {
                            free(column_json);
                            metadata_generator_free_metadata(*metadata);
                            *metadata = NULL;
                            snprintf(s_error_message, sizeof(s_error_message),
                                    "Failed to allocate memory for metadata item JSON");
                            return JSON_SERIALIZATION_MEMORY_ERROR;
                        }
                        
                        strncpy(item_json, q, item_len);
                        item_json[item_len] = '\0';
                        
                        MetadataItem* item = &column->metadata[item_count];
                        
                        // Parse item (name and type)
                        parse_metadata_item(item_json, item);
                        
                        free(item_json);
                        item_count++;
                        q = item_end + 1;
                    }
                }
            }
            
            free(column_json);
            column_count++;
            p = column_end + 1;
        }
    }
    
    return JSON_SERIALIZATION_OK;
}

/**
 * Serialize metadata to a binary file
 * 
 * This function converts the metadata to JSON and then writes it to a binary file.
 * The file is not stored as plain JSON text, but as a binary representation
 * that can be efficiently loaded using json_serialization_load_from_binary.
 * 
 * metadata: Pointer to the metadata structure to serialize
 * file_path: Path where the binary file will be written
 * returns: Error code (JSON_SERIALIZATION_OK on success)
 */
JsonSerializationError json_serialization_save_to_binary(
    const Metadata* metadata,
    const char* file_path
) {
    if (!metadata || !file_path) {
        return JSON_SERIALIZATION_INVALID_PARAMETER;
    }
    
    // Convert the metadata to a JSON string
    char* json_string;
    JsonSerializationError error = json_serialization_metadata_to_json(
        metadata, &json_string);
    
    if (error != JSON_SERIALIZATION_OK) {
        return error;
    }
    
    // Open the file for writing
    FILE* file = fopen(file_path, "wb");
    if (!file) {
        free(json_string);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to open file for writing: %s", file_path);
        return JSON_SERIALIZATION_FILE_ERROR;
    }
    
    // Write the JSON string to the file
    size_t length = strlen(json_string);
    size_t written = fwrite(json_string, 1, length, file);
    
    free(json_string);
    fclose(file);
    
    if (written != length) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to write JSON to file: %s", file_path);
        return JSON_SERIALIZATION_FILE_ERROR;
    }
    
    return JSON_SERIALIZATION_OK;
}

/**
 * Load metadata from a binary file
 * 
 * This function reads a binary file created with json_serialization_save_to_binary
 * and parses it into a metadata structure. The caller is responsible for
 * freeing the returned metadata using metadata_generator_free_metadata.
 * 
 * file_path: Path to the binary file
 * metadata: Pointer to store the parsed metadata structure
 * returns: Error code (JSON_SERIALIZATION_OK on success)
 */
JsonSerializationError json_serialization_load_from_binary(
    const char* file_path,
    Metadata** metadata
) {
    if (!file_path || !metadata) {
        return JSON_SERIALIZATION_INVALID_PARAMETER;
    }
    
    // Open the file for reading
    FILE* file = fopen(file_path, "rb");
    if (!file) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to open file for reading: %s", file_path);
        return JSON_SERIALIZATION_FILE_ERROR;
    }
    
    // Determine the file size
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    rewind(file);
    
    // Allocate memory for the file contents
    char* json_string = (char*)malloc(file_size + 1);
    if (!json_string) {
        fclose(file);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for file contents");
        return JSON_SERIALIZATION_MEMORY_ERROR;
    }
    
    // Read the file contents
    size_t read = fread(json_string, 1, file_size, file);
    fclose(file);
    
    if (read != (size_t)file_size) {
        free(json_string);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to read file contents: %s", file_path);
        return JSON_SERIALIZATION_FILE_ERROR;
    }
    
    // Null-terminate the string
    json_string[file_size] = '\0';
    
    // Parse the JSON string into a metadata structure
    JsonSerializationError error = json_serialization_json_to_metadata(
        json_string, metadata);
    
    free(json_string);
    
    return error;
}

/**
 * Free a string allocated by json_serialization_metadata_to_json
 * 
 * This function releases memory allocated for a JSON string.
 * 
 * json_string: The string to free
 */
void json_serialization_free_string(char* json_string) {
    if (json_string) {
        free(json_string);
    }
}

/**
 * Get the last error message from JSON serialization operations
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any JSON serialization function.
 * 
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* json_serialization_get_error(void) {
    if (s_error_message[0] == '\0') {
        return NULL;
    }
    
    return s_error_message;
}

/**
 * Metadata serializer with basic serialization logic
 */
char* json_serialize_metadata(const Metadata* metadata) {
    if (!metadata) {
        set_error("Invalid metadata pointer");
        return NULL;
    }
    
    return metadata_to_json(metadata);
} 