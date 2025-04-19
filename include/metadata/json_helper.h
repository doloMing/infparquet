/**
 * json_helper.h
 * 
 * This header file provides functions for JSON operations on metadata.
 */

#ifndef INFPARQUET_JSON_HELPER_H
#define INFPARQUET_JSON_HELPER_H

#include "metadata/metadata_types.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Error codes for JSON operations
 */
typedef enum {
    JSON_HELPER_OK = 0,             // No error
    JSON_HELPER_INVALID_PARAMETER,  // Invalid parameter
    JSON_HELPER_MEMORY_ERROR,       // Memory allocation error
    JSON_HELPER_PARSE_ERROR,        // JSON parsing error
    JSON_HELPER_FILE_ERROR,         // File I/O error
    JSON_HELPER_METADATA_ERROR,     // Metadata handling error
    JSON_HELPER_UNKNOWN_ERROR       // Unknown error
} JsonHelperError;

/**
 * Find a field in a JSON string
 * 
 * json: The JSON string to search
 * field: The field to find
 * 
 * Return: Pointer to the value, or NULL if the field is not found
 */
const char* find_json_field(const char* json, const char* field);

/**
 * Find the matching closing bracket for an opening bracket
 * 
 * json: The JSON string containing the opening bracket
 * open_char: The opening bracket character ('[' or '{')
 * close_char: The closing bracket character (']' or '}')
 * 
 * Return: Pointer to the matching closing bracket, or NULL if not found
 */
const char* find_matching_bracket(const char* json, char open_char, char close_char);

/**
 * Extract a string value from a JSON string
 * 
 * json: The JSON string containing the value
 * result: Buffer to store the extracted string
 * max_length: Maximum length of the result buffer
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_string(const char* json, char* result, size_t max_length);

/**
 * Extract an integer value from a JSON string
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_int(const char* json, int* result);

/**
 * Extract a uint32 value from a JSON string
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_uint32(const char* json, uint32_t* result);

/**
 * Extract a uint64 value from a JSON string
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_uint64(const char* json, uint64_t* result);

/**
 * Extract a double value from a JSON string
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_double(const char* json, double* result);

/**
 * Extract a boolean value from a JSON string
 * 
 * json: The JSON string containing the value
 * result: Pointer to a variable to store the extracted value
 * 
 * Return: true if successful, false otherwise
 */
bool extract_json_bool(const char* json, bool* result);

/**
 * Parse a string array from a JSON string
 * 
 * json: The JSON string containing the array
 * strings: Array to store the strings
 * counts: Array to store the counts (optional, can be NULL)
 * max_count: Maximum number of strings to parse
 * 
 * Return: Number of strings parsed, or -1 on error
 */
int parse_string_array(const char* json, char** strings, uint32_t* counts, int max_count);

/**
 * Parse a metadata item from a JSON string
 * 
 * json: The JSON string containing the metadata item
 * item: Pointer to a MetadataItem structure to receive the parsed data
 * 
 * Return: true if successful, false otherwise
 */
bool parse_metadata_item(const char* json, MetadataItem* item);

/**
 * Set an error message
 * 
 * format: The format string
 * ...: The format arguments
 */
void set_error(const char* format, ...);

/**
 * Get the error message
 * 
 * Return: The error message, or NULL if no error has occurred
 */
const char* json_helper_get_error();

/**
 * Serialize metadata to JSON
 * 
 * This function serializes metadata to a JSON string.
 * 
 * metadata: The metadata to serialize
 * 
 * Return: A newly allocated string containing the JSON representation,
 *         or NULL if serialization fails
 */
char* json_serialization_metadata_to_json(const Metadata* metadata);

/**
 * Serialize metadata to a JSON file
 * 
 * metadata: The metadata to serialize
 * file_path: Path to the output file
 * 
 * Return: JSON_HELPER_OK on success, error code otherwise
 */
JsonHelperError json_save_metadata_to_file(const Metadata* metadata, const char* file_path);

/**
 * Deserialize metadata from a JSON string
 * 
 * json_str: The JSON string containing the metadata
 * metadata: Pointer to a variable to receive the deserialized metadata
 * 
 * Return: JSON_HELPER_OK on success, error code otherwise
 */
JsonHelperError json_deserialize_metadata(const char* json_str, Metadata** metadata);

/**
 * Load metadata from a JSON file
 * 
 * file_path: Path to the input file
 * metadata: Pointer to a variable to receive the loaded metadata
 * 
 * Return: JSON_HELPER_OK on success, error code otherwise
 */
JsonHelperError json_load_metadata_from_file(const char* file_path, Metadata** metadata);

/**
 * Free a JSON string
 * 
 * json_str: The JSON string to free
 */
void json_free_string(char* json_str);

#ifdef _WIN32
/**
 * strptime function for Windows
 * 
 * s: The string to parse
 * format: The format string
 * tm: Pointer to a struct tm to receive the parsed time
 * 
 * Return: Pointer to the first character after the parsed date, or NULL on error
 */
char* strptime(const char* s, const char* format, struct tm* tm);
#endif

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_JSON_HELPER_H */ 