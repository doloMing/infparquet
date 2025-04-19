/**
 * lzma_decompressor.h
 * 
 * This header file defines the interface for LZMA2 decompression functionality.
 * It provides functions to decompress data compressed with the LZMA2 algorithm.
 */

#ifndef INFPARQUET_LZMA_DECOMPRESSOR_H
#define INFPARQUET_LZMA_DECOMPRESSOR_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Callback function type for reporting decompression progress
 * 
 * total_size: Total size of the data being decompressed
 * processed_size: Size of data processed so far
 * user_data: User-provided data passed to the decompression function
 * 
 * Return: true to continue, false to abort
 */
typedef bool (*DecompressionProgressCallback)(uint64_t total_size, uint64_t processed_size, void* user_data);

/**
 * Decompresses data using LZMA2 algorithm
 * 
 * This function decompresses the input data that was compressed using the LZMA2
 * algorithm and writes the decompressed data to the output buffer.
 * 
 * input_data: Pointer to the compressed data
 * input_size: Size of the compressed data in bytes
 * output_data: Pointer to the buffer where decompressed data will be written
 * output_size: Pointer to a variable that will receive the size of the decompressed data
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int lzma_decompress_buffer(const void* input_data, uint64_t input_size,
                           void* output_data, uint64_t* output_size);

/**
 * Decompresses data from a file using LZMA2 algorithm
 * 
 * This function reads compressed data from the input file, decompresses it using 
 * the LZMA2 algorithm, and writes the decompressed data to the output file.
 * 
 * input_file: Path to the file containing compressed data
 * output_file: Path to the output file where decompressed data will be written
 * progress_callback: Callback function for reporting progress (can be NULL)
 * user_data: User data to pass to the progress callback (can be NULL)
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int lzma_decompress_file(const char* input_file, const char* output_file,
                         DecompressionProgressCallback progress_callback,
                         void* user_data);

/**
 * Gets the size of decompressed data from compressed data
 * 
 * This function examines the header of the compressed data to determine
 * the size of the decompressed data.
 * 
 * input_data: Pointer to the compressed data
 * input_size: Size of the compressed data in bytes
 * 
 * Return: Size of the decompressed data, or 0 if it cannot be determined
 */
uint64_t lzma_get_decompressed_size(const void* input_data, uint64_t input_size);

/**
 * Sets the LZMA decompression parameters
 * 
 * This function sets various LZMA decompression parameters. It should be called
 * before calling lzma_decompress_buffer or lzma_decompress_file.
 * 
 * threads: Number of threads to use for decompression (0 for automatic)
 * memory_limit: Memory limit in bytes (0 for default)
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int lzma_set_decompression_parameters(uint32_t threads, uint64_t memory_limit);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_LZMA_DECOMPRESSOR_H */ 