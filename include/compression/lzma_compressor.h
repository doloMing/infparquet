/**
 * lzma_compressor.h
 * 
 * This header file defines the interface for LZMA2 compression functionality.
 * It provides functions to compress data using the LZMA2 algorithm.
 */

#ifndef INFPARQUET_LZMA_COMPRESSOR_H
#define INFPARQUET_LZMA_COMPRESSOR_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Constants for LZMA compression */
#define DEFAULT_COMPRESSION_LEVEL 5 /* Default compression level (1-9) */
#define MAX_COMPRESSION_LEVEL 9     /* Maximum compression level */
#define MIN_COMPRESSION_LEVEL 1     /* Minimum compression level */

/**
 * Callback function type for reporting compression progress
 * 
 * total_size: Total size of the data being compressed
 * processed_size: Size of data processed so far
 * user_data: User-provided data passed to the compression function
 * 
 * Return: true to continue, false to abort
 */
typedef bool (*CompressionProgressCallback)(uint64_t total_size, uint64_t processed_size, void* user_data);

/**
 * Compresses data using LZMA2 algorithm
 * 
 * This function compresses the input data using the LZMA2 algorithm and writes the
 * compressed data to the output buffer.
 * 
 * input_data: Pointer to the data to be compressed
 * input_size: Size of the input data in bytes
 * output_data: Pointer to the buffer where compressed data will be written
 * output_size: Pointer to a variable that will receive the size of the compressed data
 * dictionary_size: Size of the dictionary to use for compression (0 for default)
 * compression_level: Compression level (1-9, where 9 is highest compression)
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int lzma_compress_buffer(const void* input_data, uint64_t input_size,
                         void* output_data, uint64_t* output_size,
                         uint32_t dictionary_size, int compression_level);

/**
 * Compresses data from a file using LZMA2 algorithm
 * 
 * This function reads data from the input file, compresses it using the LZMA2
 * algorithm, and writes the compressed data to the output file.
 * 
 * input_file: Path to the input file
 * output_file: Path to the output file
 * compression_level: Compression level (1-9, where 9 is highest compression)
 * progress_callback: Callback function for reporting progress (can be NULL)
 * user_data: User data to pass to the progress callback (can be NULL)
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int lzma_compress_file(const char* input_file, const char* output_file,
                       int compression_level,
                       CompressionProgressCallback progress_callback,
                       void* user_data);

/**
 * Calculates the maximum possible size of compressed data
 * 
 * This function calculates the maximum size that the compressed data could
 * have after compression. This is useful for allocating a buffer for the
 * compressed data.
 * 
 * input_size: Size of the input data in bytes
 * 
 * Return: Maximum possible size of the compressed data
 */
uint64_t lzma_maximum_compressed_size(uint64_t input_size);

/**
 * Sets the LZMA compression parameters
 * 
 * This function sets various LZMA compression parameters. It should be called
 * before calling lzma_compress_buffer or lzma_compress_file.
 * 
 * threads: Number of threads to use for compression (0 for automatic)
 * memory_limit: Memory limit in bytes (0 for default)
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int lzma_set_compression_parameters(uint32_t threads, uint64_t memory_limit);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_LZMA_COMPRESSOR_H */ 