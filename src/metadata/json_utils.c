/**
 * json_utils.c
 * 
 * Implementation of JSON utility functions for parsing and manipulating JSON data.
 */

#include "metadata/json_helper.h"
#include "metadata/metadata_types.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <stdbool.h>
#include <time.h>
#include <stdarg.h>  // add this header file to support va_list

/* Constants */
#define MAX_JSON_ERROR_LENGTH 256
#define MAX_HIGH_FREQ_STRINGS 10
#define MAX_SPECIAL_STRINGS 10
#define MAX_CATEGORIES 20

/* Static global for error messages */
static char s_error_message[MAX_JSON_ERROR_LENGTH];

/**
 * Find a field in a JSON string
 * 
 * This function finds a field in a JSON string and returns a pointer to the value.
 * 
 * json: The JSON string to search
 * field: The field to find
 * 
 * Return: Pointer to the value, or NULL if the field is not found
 */
const char* find_json_field(const char* json, const char* field) {
    if (!json || !field) {
        return NULL;
    }
    
    // Format the field name for searching
    char search_key[256];
    snprintf(search_key, sizeof(search_key), "\"%s\":", field);
    
    // Find the field
    const char* field_pos = strstr(json, search_key);
    if (!field_pos) {
        return NULL;
    }
    
    // Move to the start of the value
    const char* value_start = field_pos + strlen(search_key);
    
    // Skip whitespace
    while (*value_start && isspace((unsigned char)*value_start)) {
        value_start++;
    }
    
    return value_start;
}

/**
 * Find the matching closing bracket for an opening bracket
 * 
 * This function finds the matching closing bracket for an opening bracket.
 * It handles nested brackets correctly.
 * 
 * json: The JSON string containing the opening bracket
 * open_char: The opening bracket character ('[' or '{')
 * close_char: The closing bracket character (']' or '}')
 * 
 * Return: Pointer to the matching closing bracket, or NULL if not found
 */
const char* find_matching_bracket(const char* json, char open_char, char close_char) {
    if (!json) {
        return NULL;
    }
    
    // Skip to the opening bracket
    while (*json && *json != open_char) {
        json++;
    }
    
    if (!*json) {
        return NULL;  // Opening bracket not found
    }
    
    // Count the number of nested brackets
    int bracket_count = 1;
    json++;  // Skip the opening bracket
    
    while (*json && bracket_count > 0) {
        if (*json == open_char) {
            bracket_count++;
        } else if (*json == close_char) {
            bracket_count--;
        } else if (*json == '"') {
            // Skip string literals
            json++;
            while (*json && *json != '"') {
                if (*json == '\\' && *(json + 1)) {
                    json++;  // Skip escaped character
                }
                json++;
            }
            if (!*json) {
                return NULL;  // Unterminated string
            }
        }
        
        if (bracket_count > 0) {
            json++;
        }
    }
    
    return bracket_count == 0 ? json : NULL;
}

/**
 * Extract a string value from a JSON string
 * 
 * This function extracts a string value from a JSON string.
 * 
 * json: The JSON string containing the value
 * result: Buffer to store the extracted string
 * max_length: Maximum length of the result buffer
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_string(const char* json, char* result, size_t max_length) {
    if (!json || !result || max_length == 0) {
        return false;
    }
    
    // Skip whitespace
    while (*json && isspace((unsigned char)*json)) {
        json++;
    }
    
    // Check for null value
    if (strncmp(json, "null", 4) == 0) {
        result[0] = '\0';
        return true;
    }
    
    // Ensure this is a string (starts with ")
    if (*json != '"') {
        return false;
    }
    
    // Skip the opening quote
    json++;
    
    // Copy the string value
    size_t i = 0;
    while (*json && *json != '"' && i < max_length - 1) {
        // Handle escaped characters
        if (*json == '\\' && *(json + 1)) {
            json++;
            
            // Handle escaped quotes, backslashes, etc.
            switch (*json) {
                case '"':
                case '\\':
                case '/':
                    result[i++] = *json;
                    break;
                case 'b': result[i++] = '\b'; break;
                case 'f': result[i++] = '\f'; break;
                case 'n': result[i++] = '\n'; break;
                case 'r': result[i++] = '\r'; break;
                case 't': result[i++] = '\t'; break;
                default:
                    // Unknown escape sequence, just copy it
                    result[i++] = *json;
                    break;
            }
        } else {
            result[i++] = *json;
        }
        
        json++;
    }
    
    result[i] = '\0';
    
    // Ensure the string was terminated properly
    return *json == '"';
}

/**
 * Extract an integer value from a JSON string
 * 
 * This function extracts an integer value from a JSON string.
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_int(const char* json, int* result) {
    if (!json || !result) {
        return false;
    }
    
    // Skip whitespace
    while (*json && isspace((unsigned char)*json)) {
        json++;
    }
    
    // Check for null value
    if (strncmp(json, "null", 4) == 0) {
        *result = 0;
        return true;
    }
    
    char* end;
    long value = strtol(json, &end, 10);
    
    // Ensure we read a valid number
    if (end == json) {
        return false;
    }
    
    *result = (int)value;
    return true;
}

/**
 * Extract a uint32 value from a JSON string
 * 
 * This function extracts a uint32 value from a JSON string.
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_uint32(const char* json, uint32_t* result) {
    if (!json || !result) {
        return false;
    }
    
    // Skip whitespace
    while (*json && isspace((unsigned char)*json)) {
        json++;
    }
    
    // Check for null value
    if (strncmp(json, "null", 4) == 0) {
        *result = 0;
        return true;
    }
    
    char* end;
    unsigned long value = strtoul(json, &end, 10);
    
    // Ensure we read a valid number
    if (end == json) {
        return false;
    }
    
    *result = (uint32_t)value;
    return true;
}

/**
 * Extract a uint64 value from a JSON string
 * 
 * This function extracts a uint64 value from a JSON string.
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_uint64(const char* json, uint64_t* result) {
    if (!json || !result) {
        return false;
    }
    
    // Skip whitespace
    while (*json && isspace((unsigned char)*json)) {
        json++;
    }
    
    // Check for null value
    if (strncmp(json, "null", 4) == 0) {
        *result = 0;
        return true;
    }
    
    char* end;
    unsigned long long value = strtoull(json, &end, 10);
    
    // Ensure we read a valid number
    if (end == json) {
        return false;
    }
    
    *result = (uint64_t)value;
    return true;
}

/**
 * Extract a double value from a JSON string
 * 
 * This function extracts a double value from a JSON string.
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_double(const char* json, double* result) {
    if (!json || !result) {
        return false;
    }
    
    // Skip whitespace
    while (*json && isspace((unsigned char)*json)) {
        json++;
    }
    
    // Check for null value
    if (strncmp(json, "null", 4) == 0) {
        *result = 0.0;
        return true;
    }
    
    char* end;
    double value = strtod(json, &end);
    
    // Ensure we read a valid number
    if (end == json) {
        return false;
    }
    
    *result = value;
    return true;
}

/**
 * Extract a boolean value from a JSON string
 * 
 * This function extracts a boolean value from a JSON string.
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_bool(const char* json, bool* result) {
    if (!json || !result) {
        return false;
    }
    
    // Skip whitespace
    while (*json && isspace((unsigned char)*json)) {
        json++;
    }
    
    // Check for null value
    if (strncmp(json, "null", 4) == 0) {
        *result = false;
        return true;
    }
    
    // Check for true or false
    if (strncmp(json, "true", 4) == 0) {
        *result = true;
        return true;
    } else if (strncmp(json, "false", 5) == 0) {
        *result = false;
        return true;
    }
    
    return false;
}

/**
 * Parse a string array from a JSON string
 * 
 * This function parses a string array from a JSON string.
 * 
 * json: The JSON string containing the array
 * strings: Array to store the strings
 * counts: Array to store the counts (optional, can be NULL)
 * max_count: Maximum number of strings to parse
 * 
 * Return: Number of strings parsed, or -1 on error
 */
int parse_string_array(const char* json, char** strings, uint32_t* counts, int max_count) {
    if (!json || !strings || max_count <= 0) {
        return -1;
    }
    
    // Skip to the array start
    while (*json && *json != '[') {
        json++;
    }
    
    if (!*json) {
        return -1;  // Array start not found
    }
    
    // Skip the opening bracket
    json++;
    
    // Parse array elements
    int count = 0;
    bool in_object = false;
    bool in_string = false;
    bool in_count = false;
    char temp_string[256];
    uint32_t temp_count = 0;
    
    while (*json && *json != ']' && count < max_count) {
        // Skip whitespace
        while (*json && isspace((unsigned char)*json)) {
            json++;
        }
        
        // Check for end of array
        if (*json == ']') {
            break;
        }
        
        // Check for start of object
        if (*json == '{') {
            in_object = true;
            json++;
            
            // Parse object fields
            while (*json && in_object) {
                // Skip whitespace
                while (*json && isspace((unsigned char)*json)) {
                    json++;
                }
                
                // Check for end of object
                if (*json == '}') {
                    in_object = false;
                    json++;
                    
                    // Add the string to the array
                    strings[count] = strdup(temp_string);
                    if (counts) {
                        counts[count] = temp_count;
                    }
                    count++;
                    
                    // Check for comma
                    while (*json && isspace((unsigned char)*json)) {
                        json++;
                    }
                    if (*json == ',') {
                        json++;
                    }
                    
                    break;
                }
                
                // Check for field name
                if (*json == '"') {
                    json++;
                    
                    // Read field name
                    char field_name[32];
                    int i = 0;
                    while (*json && *json != '"' && i < sizeof(field_name) - 1) {
                        field_name[i++] = *json++;
                    }
                    field_name[i] = '\0';
                    
                    // Skip closing quote and colon
                    if (*json == '"') {
                        json++;
                    }
                    
                    // Skip whitespace
                    while (*json && isspace((unsigned char)*json)) {
                        json++;
                    }
                    
                    // Skip colon
                    if (*json == ':') {
                        json++;
                    }
                    
                    // Skip whitespace
                    while (*json && isspace((unsigned char)*json)) {
                        json++;
                    }
                    
                    // Check field type
                    if (strcmp(field_name, "string") == 0) {
                        in_string = true;
                        if (*json == '"') {
                            extract_json_string(json, temp_string, sizeof(temp_string));
                            // Skip to end of string
                            json++;
                            while (*json && *json != '"') {
                                if (*json == '\\' && *(json + 1)) {
                                    json++;
                                }
                                json++;
                            }
                            if (*json == '"') {
                                json++;
                            }
                        }
                    } else if (strcmp(field_name, "count") == 0) {
                        in_count = true;
                        extract_json_uint32(json, &temp_count);
                        // Skip to end of number
                        while (*json && (isdigit((unsigned char)*json) || *json == '.')) {
                            json++;
                        }
                    }
                    
                    // Skip commas between fields
                    if (*json == ',') {
                        json++;
                    }
                } else {
                    // Skip unexpected characters
                    json++;
                }
            }
        } else if (*json == '"') {
            // Simple string array
            extract_json_string(json, temp_string, sizeof(temp_string));
            strings[count] = strdup(temp_string);
            if (counts) {
                counts[count] = 1;  // Default count
            }
            count++;
            
            // Skip to end of string
            json++;
            while (*json && *json != '"') {
                if (*json == '\\' && *(json + 1)) {
                    json++;
                }
                json++;
            }
            if (*json == '"') {
                json++;
            }
            
            // Skip commas between elements
            while (*json && isspace((unsigned char)*json)) {
                json++;
            }
            if (*json == ',') {
                json++;
            }
        } else {
            // Skip unexpected characters
            json++;
        }
    }
    
    return count;
}

/**
 * Set an error message
 * 
 * This function sets the error message for the json_helper module.
 * 
 * format: The format string
 * ...: The format arguments
 */
void set_error(const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(s_error_message, MAX_JSON_ERROR_LENGTH, format, args);
    va_end(args);
}

/**
 * Get the error message
 * 
 * This function returns the error message for the json_helper module.
 * 
 * Return: The error message, or NULL if no error has occurred
 */
const char* json_helper_get_error() {
    return s_error_message[0] ? s_error_message : NULL;
}

/**
 * Serialize metadata to JSON
 * 
 * This function is a wrapper for the actual implementation in json_serialization.c
 */
char* json_serialization_metadata_to_json(const Metadata* metadata) {
    // This is implemented in json_serialization.c, we're just providing a linking stub
    return NULL;
}

/**
 * Parse a metadata item from a JSON string
 */
bool parse_metadata_item(const char* json, MetadataItem* item) {
    if (!json || !item) {
        return false;
    }
    
    // Initialize the item
    memset(item, 0, sizeof(MetadataItem));
    
    // Parse the name
    const char* name_field = find_json_field(json, "name");
    if (name_field) {
        extract_json_string(name_field, item->name, sizeof(item->name));
    }
    
    // Parse the type
    const char* type_field = find_json_field(json, "type");
    if (type_field) {
        char type_str[32];
        extract_json_string(type_field, type_str, sizeof(type_str));
        
        if (strcmp(type_str, "timestamp") == 0) {
            item->type = METADATA_TYPE_TIMESTAMP;
            
            // Parse timestamp-specific fields
            const char* min_timestamp = find_json_field(json, "min_timestamp");
            const char* max_timestamp = find_json_field(json, "max_timestamp");
            const char* count = find_json_field(json, "count");
            
            if (min_timestamp) {
                char min_ts_str[32];
                extract_json_string(min_timestamp, min_ts_str, sizeof(min_ts_str));
                // Convert string to time_t
                struct tm tm = {0};
                strptime(min_ts_str, "%Y-%m-%dT%H:%M:%S", &tm);
                item->value.timestamp.min_timestamp = mktime(&tm);
            }
            
            if (max_timestamp) {
                char max_ts_str[32];
                extract_json_string(max_timestamp, max_ts_str, sizeof(max_ts_str));
                // Convert string to time_t
                struct tm tm = {0};
                strptime(max_ts_str, "%Y-%m-%dT%H:%M:%S", &tm);
                item->value.timestamp.max_timestamp = mktime(&tm);
            }
            
            if (count) {
                extract_json_uint64(count, &item->value.timestamp.count);
            }
        } else if (strcmp(type_str, "string") == 0) {
            item->type = METADATA_TYPE_STRING;
            
            // Parse string-specific fields
            const char* total_count = find_json_field(json, "total_count");
            const char* avg_length = find_json_field(json, "avg_length");
            
            if (total_count) {
                extract_json_uint64(total_count, &item->value.string.total_string_count);
            }
            
            if (avg_length) {
                extract_json_uint32(avg_length, &item->value.string.avg_string_length);
            }
            
            // Parse high frequency strings
            const char* high_freq = find_json_field(json, "high_freq_strings");
            if (high_freq) {
                const char* array_end = find_matching_bracket(high_freq, '[', ']');
                if (array_end) {
                    // dynamically allocate memory
                    char** high_freq_strings = (char**)malloc(sizeof(char*) * MAX_HIGH_FREQ_STRINGS);
                    uint32_t* high_freq_counts = (uint32_t*)malloc(sizeof(uint32_t) * MAX_HIGH_FREQ_STRINGS);
                    
                    if (high_freq_strings && high_freq_counts) {
                        int count = parse_string_array(
                            high_freq, 
                            high_freq_strings, 
                            high_freq_counts, 
                            MAX_HIGH_FREQ_STRINGS);
                            
                        // Copy the parsed results to item - the correct way
                        item->value.string.high_freq_count = (count > MAX_HIGH_FREQ_STRINGS) ? 
                                                          MAX_HIGH_FREQ_STRINGS : count;
                        
                        for (int i = 0; i < item->value.string.high_freq_count; i++) {
                            if (high_freq_strings[i]) {
                                strncpy(item->value.string.high_freq_strings[i].string, 
                                       high_freq_strings[i], MAX_STRING_LENGTH - 1);
                                item->value.string.high_freq_strings[i].string[MAX_STRING_LENGTH - 1] = '\0';
                                item->value.string.high_freq_counts[i] = high_freq_counts[i];
                                item->value.string.high_freq_strings[i].count = high_freq_counts[i];
                                free(high_freq_strings[i]); // Free the dynamically allocated string
                            }
                        }
                        
                        // Free the array pointers
                        free(high_freq_strings);
                        free(high_freq_counts);
                    } else {
                        // memory allocation failed, free the allocated memory
                        if (high_freq_strings) {
                            free(high_freq_strings);
                        }
                        if (high_freq_counts) {
                            free(high_freq_counts);
                        }
                    }
                }
            }
            
            // Parse special strings
            const char* special = find_json_field(json, "special_strings");
            if (special) {
                const char* array_end = find_matching_bracket(special, '[', ']');
                if (array_end) {
                    // dynamically allocate memory
                    char** special_strings = (char**)malloc(sizeof(char*) * MAX_SPECIAL_STRINGS);
                    uint32_t* special_counts = (uint32_t*)malloc(sizeof(uint32_t) * MAX_SPECIAL_STRINGS);
                    
                    if (special_strings && special_counts) {
                        int count = parse_string_array(
                            special, 
                            special_strings, 
                            special_counts, 
                            MAX_SPECIAL_STRINGS);
                            
                        // Copy the parsed results to item - the correct way
                        item->value.string.special_string_count = (count > MAX_SPECIAL_STRINGS) ? 
                                                              MAX_SPECIAL_STRINGS : count;
                        
                        for (int i = 0; i < item->value.string.special_string_count; i++) {
                            if (special_strings[i]) {
                                strncpy(item->value.string.special_strings[i], 
                                       special_strings[i], MAX_STRING_LENGTH - 1);
                                item->value.string.special_strings[i][MAX_STRING_LENGTH - 1] = '\0';
                                item->value.string.special_string_counts[i] = special_counts[i];
                                free(special_strings[i]); // Free the dynamically allocated string
                            }
                        }
                        
                        // Free the array pointers
                        free(special_strings);
                        free(special_counts);
                    } else {
                        // memory allocation failed, free the allocated memory
                        if (special_strings) {
                            free(special_strings);
                        }
                        if (special_counts) {
                            free(special_counts);
                        }
                    }
                }
            }
        } else if (strcmp(type_str, "numeric") == 0) {
            item->type = METADATA_TYPE_NUMERIC;
            
            // Parse numeric-specific fields
            const char* min_value = find_json_field(json, "min");
            const char* max_value = find_json_field(json, "max");
            const char* avg_value = find_json_field(json, "avg");
            const char* mode_value = find_json_field(json, "mode");
            const char* mode_count = find_json_field(json, "mode_count");
            const char* total_count = find_json_field(json, "total_count");
            const char* null_count = find_json_field(json, "null_count");
            
            if (min_value) extract_json_double(min_value, &item->value.numeric.min_value);
            if (max_value) extract_json_double(max_value, &item->value.numeric.max_value);
            if (avg_value) extract_json_double(avg_value, &item->value.numeric.avg_value);
            if (mode_value) extract_json_double(mode_value, &item->value.numeric.mode_value);
            if (mode_count) extract_json_uint64(mode_count, &item->value.numeric.mode_count);
            if (total_count) extract_json_uint64(total_count, &item->value.numeric.total_count);
            if (null_count) extract_json_uint32(null_count, &item->value.numeric.null_count);
        } else if (strcmp(type_str, "categorical") == 0) {
            item->type = METADATA_TYPE_CATEGORICAL;
            
            // Parse categorical-specific fields
            const char* total_value_count = find_json_field(json, "total_count");
            const char* total_category_count = find_json_field(json, "total_categories");
            
            if (total_value_count) {
                extract_json_uint64(total_value_count, &item->value.categorical.total_value_count);
            }
            
            if (total_category_count) {
                extract_json_uint32(total_category_count, &item->value.categorical.total_category_count);
            }
            
            // Parse categories
            const char* categories = find_json_field(json, "categories");
            if (categories) {
                const char* array_end = find_matching_bracket(categories, '[', ']');
                if (array_end) {
                    // dynamically allocate memory
                    char** category_strings = (char**)malloc(sizeof(char*) * MAX_CATEGORIES);
                    uint32_t* category_counts = (uint32_t*)malloc(sizeof(uint32_t) * MAX_CATEGORIES);
                    
                    if (category_strings && category_counts) {
                        int count = parse_string_array(
                            categories, 
                            category_strings, 
                            category_counts, 
                            MAX_CATEGORIES);
                            
                        // Copy the parsed results to item - the correct way
                        item->value.categorical.high_freq_category_count = (count > MAX_HIGH_FREQ_CATEGORIES) ? 
                                                                      MAX_HIGH_FREQ_CATEGORIES : count;
                        
                        for (int i = 0; i < item->value.categorical.high_freq_category_count; i++) {
                            if (category_strings[i]) {
                                strncpy(item->value.categorical.categories[i], 
                                       category_strings[i], MAX_STRING_LENGTH - 1);
                                item->value.categorical.categories[i][MAX_STRING_LENGTH - 1] = '\0';
                                item->value.categorical.category_counts[i] = category_counts[i];
                                free(category_strings[i]); // Free the dynamically allocated string
                            }
                        }
                        
                        // Free the array pointers
                        free(category_strings);
                        free(category_counts);
                    } else {
                        // memory allocation failed, free the allocated memory
                        if (category_strings) {
                            free(category_strings);
                        }
                        if (category_counts) {
                            free(category_counts);
                        }
                    }
                }
            }
        }
    }
    
    return true;
}

#ifdef _WIN32
/**
 * strptime function implementation for Windows
 */
char* strptime(const char* s, const char* format, struct tm* tm) {
    // Simple implementation of strptime for Windows
    // Only supports ISO 8601 format (%Y-%m-%dT%H:%M:%S)
    
    int year, month, day, hour, minute, second;
    
    if (sscanf(s, "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute, &second) == 6) {
        tm->tm_year = year - 1900;
        tm->tm_mon = month - 1;
        tm->tm_mday = day;
        tm->tm_hour = hour;
        tm->tm_min = minute;
        tm->tm_sec = second;
        tm->tm_isdst = -1;
        
        // Return a pointer to the character after the parsed date
        const char* p = s;
        while (*p && *p != ' ') p++;
        return (char*)p;
    }
    
    return NULL;
}
#endif 