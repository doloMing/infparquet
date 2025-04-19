/**
 * metadata_generator.h
 * 
 * This header defines the interface for generating metadata from Parquet files.
 * It provides functions to analyze Parquet data and create metadata at file,
 * row group, and column levels.
 */

#ifndef INFPARQUET_METADATA_GENERATOR_H
#define INFPARQUET_METADATA_GENERATOR_H

#include "../core/parquet_structure.h"
#include "../core/parquet_reader.h"
#include "metadata_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Error codes for metadata generation operations are defined in metadata_types.h
 * (using the MetadataGeneratorError enum)
 */

/**
 * Options for metadata generation
 */
typedef struct {
    bool generate_base_metadata;           /* Whether to generate base metadata */
    bool generate_custom_metadata;         /* Whether to generate custom metadata */
    const char* custom_metadata_config_path; /* Path to custom metadata configuration file */
    uint32_t max_high_freq_strings;        /* Maximum number of high-frequency strings to track */
    uint32_t max_special_strings;          /* Maximum number of special strings to track */
    uint32_t max_high_freq_categories;     /* Maximum number of high-frequency categories to track */
} MetadataGeneratorOptions;

/**
 * Initialize metadata generator options with default values
 * 
 * options: Pointer to options structure to initialize
 */
void metadata_generator_init_options(MetadataGeneratorOptions* options);

/**
 * Generate metadata for a Parquet file
 * 
 * file: Pointer to ParquetFile structure containing file information
 * reader_context: Context for reading the Parquet file
 * options: Options for metadata generation
 * metadata: Pointer to store the generated metadata
 * 
 * Returns: Error code (METADATA_GEN_OK on success)
 */
MetadataGeneratorError metadata_generator_generate(
    const ParquetFile* file,
    void* reader_context,
    const MetadataGeneratorOptions* options,
    Metadata** metadata
);

/**
 * Generate file level metadata for a parquet file
 * 
 * This function aggregates metadata from all row groups in the parquet file and 
 * generates file level statistics and summaries.
 * 
 * file: The parquet file to generate metadata for
 * config_path: Optional path to custom metadata configuration file (can be NULL)
 * progress_callback: Optional callback function to report progress (can be NULL)
 * progress_data: User data to pass to the progress callback
 * 
 * Returns: The generated file metadata, or NULL on error
 */
Metadata* metadata_generator_generate_file_metadata(
    const ParquetFile* file,
    const char* config_path,
    ProgressCallback progress_callback,
    void* progress_data
);

/**
 * Generate metadata for a row group
 * 
 * file: Pointer to ParquetFile structure containing file information
 * row_group_id: ID of the row group to generate metadata for
 * options: Options for metadata generation (can be NULL for default options)
 * progress_callback: Optional callback function to report progress (can be NULL)
 * 
 * Returns: The generated row group metadata, or NULL on error
 */
Metadata* metadata_generator_generate_row_group_metadata(
    const ParquetFile* file,
    int row_group_id,
    const MetadataGeneratorOptions* options,
    ProgressCallback progress_callback
);

/**
 * Save metadata to a file
 * 
 * metadata: Pointer to metadata structure to save
 * file_path: Path where the metadata will be saved
 * 
 * Returns: Error code (METADATA_GEN_OK on success)
 */
MetadataGeneratorError metadata_generator_save_metadata(
    const Metadata* metadata,
    const char* file_path
);

/**
 * Load metadata from a file
 * 
 * file_path: Path to the metadata file
 * metadata: Pointer to store the loaded metadata
 * 
 * Returns: Error code (METADATA_GEN_OK on success)
 */
MetadataGeneratorError metadata_generator_load_metadata(
    const char* file_path,
    Metadata** metadata
);

/**
 * Free memory allocated for metadata
 * 
 * metadata: Pointer to metadata structure to free
 */
void metadata_generator_free_metadata(Metadata* metadata);

/**
 * Get the last error message from metadata generation operations
 * 
 * Returns: Error message string
 */
const char* metadata_generator_get_error();

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_METADATA_GENERATOR_H */ 