/**
 * metadata_parser.h
 * 
 * This header provides functionality for parsing and serializing metadata to/from JSON format.
 */

#ifndef INFPARQUET_METADATA_PARSER_H
#define INFPARQUET_METADATA_PARSER_H

#include "metadata_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Serializes metadata to a JSON string
 * 
 * This function converts a Metadata structure to a JSON string representation.
 * The returned string is allocated dynamically and should be freed by the caller.
 * 
 * @param metadata Pointer to the Metadata structure to serialize
 * @return Dynamically allocated JSON string, or NULL on failure
 */
char* metadataToJson(const Metadata* metadata);

/**
 * Deserializes metadata from a JSON string
 * 
 * This function creates a new Metadata structure from a JSON string.
 * The returned structure is allocated dynamically and should be freed using releaseMetadata().
 * 
 * @param json_str The JSON string to parse
 * @return Dynamically allocated Metadata structure, or NULL on failure
 */
Metadata* metadataFromJson(const char* json_str);

/**
 * Gets the last error message
 * 
 * This function returns the last error message from metadata parsing operations.
 * The returned string is statically allocated and should not be freed.
 * 
 * @return Error message string
 */
const char* metadata_parser_get_error_message();

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_METADATA_PARSER_H */ 