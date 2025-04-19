#ifndef INFPARQUET_JSON_SERIALIZATION_H
#define INFPARQUET_JSON_SERIALIZATION_H

#include "metadata_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Error codes for JSON serialization operations
 */
typedef enum {
    JSON_SERIALIZATION_OK = 0,
    JSON_SERIALIZATION_MEMORY_ERROR,
    JSON_SERIALIZATION_INVALID_PARAMETER,
    JSON_SERIALIZATION_PARSE_ERROR,
    JSON_SERIALIZATION_FILE_ERROR,
    JSON_SERIALIZATION_UNKNOWN_ERROR
} JsonSerializationError;

/**
 * Serialize metadata to a JSON string
 * 
 * This function converts the metadata structure to a JSON string.
 * The caller is responsible for freeing the returned string using
 * json_serialization_free_string.
 * 
 * metadata: Pointer to the metadata structure to serialize
 * json_string: Pointer to store the allocated JSON string
 * returns: Error code (JSON_SERIALIZATION_OK on success)
 */
JsonSerializationError json_serialization_metadata_to_json(
    const Metadata* metadata,
    char** json_string
);

/**
 * Parse metadata from a JSON string
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
);

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
);

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
);

/**
 * Free a string allocated by json_serialization_metadata_to_json
 * 
 * This function releases memory allocated for a JSON string.
 * 
 * json_string: The string to free
 */
void json_serialization_free_string(char* json_string);

/**
 * Get the last error message from JSON serialization operations
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any JSON serialization function.
 * 
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* json_serialization_get_error(void);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_JSON_SERIALIZATION_H */ 