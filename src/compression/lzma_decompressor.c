#include "compression/lzma_decompressor.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

// Include the LZMA library headers
#include "lzma/LzmaEnc.h"
#include "lzma/LzmaDec.h"
#include "lzma/Alloc.h"
#include "lzma/Types.h"

// Error codes for decompressor
typedef enum {
    LZMA_DECOMPRESSOR_OK = 0,
    LZMA_DECOMPRESSOR_INVALID_PARAMETER,
    LZMA_DECOMPRESSOR_MEMORY_ERROR,
    LZMA_DECOMPRESSOR_COMPRESSION_ERROR,
    LZMA_DECOMPRESSOR_FILE_ERROR
} LzmaDecompressorError;

// Global state
static uint32_t g_threads = 0;
static uint64_t g_memory_limit = 0;

/* Static global for error messages */
static char s_error_message[256];

/* LZMA2 allocation functions */
static void* lzma_alloc(ISzAllocPtr p, size_t size) {
    (void)p; // Unused parameter
    return MyAlloc(size);
}

static void lzma_free(ISzAllocPtr p, void* address) {
    (void)p; // Unused parameter
    MyFree(address);
}

/* LZMA2 allocator */
static ISzAlloc g_alloc = { lzma_alloc, lzma_free };

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
                           void* output_data, uint64_t* output_size) {
    if (!input_data || input_size <= LZMA_PROPS_SIZE + 8 || 
        !output_data || !output_size || *output_size == 0) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid parameters for decompression");
        return 1;  // Invalid parameters
    }
    
    // Get the LZMA properties from the header
    const Byte* props = (const Byte*)input_data;
    
    // Get the uncompressed size from the header
    uint64_t uncompressed_size = 0;
    for (int i = 0; i < 8; i++) {
        uncompressed_size |= (uint64_t)((const Byte*)input_data)[LZMA_PROPS_SIZE + i] << (i * 8);
    }
    
    // Ensure output buffer is large enough
    if (*output_size < uncompressed_size) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Output buffer too small for LZMA decompression");
        return 2;  // Output buffer too small
    }
    
    // Pointer to the actual compressed data (after header)
    const Byte* compressed_data = props + LZMA_PROPS_SIZE + 8;
    
    // Size of the compressed data
    SizeT compressed_size = input_size - (LZMA_PROPS_SIZE + 8);
    
    // Size of the uncompressed data
    SizeT dest_len = uncompressed_size;
    
    // Create decoder state
    CLzmaDec state;
    LzmaDec_Construct(&state);
    
    // Initialize decoder with properties
    SRes res = LzmaDec_Allocate(&state, props, LZMA_PROPS_SIZE, &g_alloc);
    if (res != SZ_OK) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to allocate LZMA decoder: %d", res);
        return 3;  // Decoder allocation failed
    }
    
    LzmaDec_Init(&state);
    
    // Decompress the data
    ELzmaStatus status;
    res = LzmaDec_DecodeToBuf(&state, 
                           (Byte*)output_data, &dest_len, 
                           compressed_data, &compressed_size, 
                           LZMA_FINISH_END, &status);
    
    LzmaDec_Free(&state, &g_alloc);
    
    if (res != SZ_OK) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "LZMA decompression failed with error code %d, status %d", res, status);
        return 4;  // Decompression failed
    }
    
    // Update the size
    *output_size = dest_len;
    
    return 0;  // Success
}

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
                         void* user_data) {
    if (!input_file || !output_file) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid input or output file parameters");
        return 1;  // Invalid parameters
    }
    
    // Open input file
    FILE* in = fopen(input_file, "rb");
    if (!in) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to open input file: %s", input_file);
        return 2;  // Failed to open input file
    }
    
    // Get input file size
    fseek(in, 0, SEEK_END);
    uint64_t input_size = ftell(in);
    fseek(in, 0, SEEK_SET);
    
    // Read input file
    void* input_data = malloc(input_size);
    if (!input_data) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for input data");
        fclose(in);
        return 3;  // Out of memory
    }
    
    if (fread(input_data, 1, input_size, in) != input_size) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to read input file");
        free(input_data);
        fclose(in);
        return 4;  // Failed to read input file
    }
    
    fclose(in);
    
    // Get decompressed size
    uint64_t output_size = lzma_get_decompressed_size(input_data, input_size);
    if (output_size == 0) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to determine decompressed size");
        free(input_data);
        return 5;  // Failed to determine decompressed size
    }
    
    // Allocate output buffer
    void* output_data = malloc(output_size);
    if (!output_data) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for output data");
        free(input_data);
        return 6;  // Out of memory
    }
    
    // Report progress (0%)
    if (progress_callback) {
        if (!progress_callback(input_size, 0, user_data)) {
            free(output_data);
            free(input_data);
            return 7;  // Cancelled by user
        }
    }
    
    // Decompress data
    int result = lzma_decompress_buffer(input_data, input_size, output_data, &output_size);
    if (result != 0) {
        free(output_data);
        free(input_data);
        return 8;  // Decompression failed
    }
    
    // Report progress (50%)
    if (progress_callback) {
        if (!progress_callback(input_size, input_size / 2, user_data)) {
            free(output_data);
            free(input_data);
            return 9;  // Cancelled by user
        }
    }
    
    // Write output file
    FILE* out = fopen(output_file, "wb");
    if (!out) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to open output file: %s", output_file);
        free(output_data);
        free(input_data);
        return 10;  // Failed to open output file
    }
    
    if (fwrite(output_data, 1, output_size, out) != output_size) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to write output file");
        fclose(out);
        free(output_data);
        free(input_data);
        return 11;  // Failed to write output file
    }
    
    fclose(out);
    free(output_data);
    free(input_data);
    
    // Report progress (100%)
    if (progress_callback) {
        if (!progress_callback(input_size, input_size, user_data)) {
            return 12;  // Cancelled by user
        }
    }
    
    return 0;  // Success
}

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
uint64_t lzma_get_decompressed_size(const void* input_data, uint64_t input_size) {
    if (!input_data || input_size < LZMA_PROPS_SIZE + 8) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid parameters or not enough data for LZMA header");
        return 0;  // Invalid parameters or not enough data for LZMA header
    }
    
    // Extract the uncompressed size from the LZMA header
    // In LZMA format, the uncompressed size is stored as an 8-byte
    // little-endian integer after the properties (5 bytes)
    uint64_t uncompressed_size = 0;
    for (int i = 0; i < 8; i++) {
        uncompressed_size |= (uint64_t)((const Byte*)input_data)[LZMA_PROPS_SIZE + i] << (i * 8);
    }
    
    return uncompressed_size;
}

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
int lzma_set_decompression_parameters(uint32_t threads, uint64_t memory_limit) {
    g_threads = threads;
    g_memory_limit = memory_limit;
    return 0;  // Success
}

/**
 * Decompress a memory buffer using LZMA2
 * 
 * This function decompresses the input buffer using the LZMA2 algorithm
 * and returns the decompressed data in a newly allocated buffer.
 * The caller is responsible for freeing the output buffer using
 * lzma_decompressor_free_buffer.
 * 
 * input_buffer: Buffer containing compressed data
 * input_size: Size of the input buffer in bytes
 * output_buffer: Pointer to store the allocated output buffer
 * output_size: Pointer to store the size of the output buffer
 * returns: Error code (LZMA_DECOMPRESSOR_OK on success)
 */
LzmaDecompressorError lzma_decompressor_decompress_buffer(
    const void* input_buffer,
    size_t input_size,
    void** output_buffer,
    size_t* output_size
) {
    if (!input_buffer || input_size <= LZMA_PROPS_SIZE + 8 || !output_buffer || !output_size) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid parameters for buffer decompression");
        return LZMA_DECOMPRESSOR_INVALID_PARAMETER;
    }
    
    // Get the decompressed size from the LZMA header
    uint64_t decompressed_size = lzma_get_decompressed_size(input_buffer, input_size);
    if (decompressed_size == 0) {
        return LZMA_DECOMPRESSOR_INVALID_PARAMETER;
    }
    
    // Allocate output buffer
    *output_buffer = malloc(decompressed_size);
    if (!*output_buffer) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for output buffer of size %" PRIu64, decompressed_size);
        return LZMA_DECOMPRESSOR_MEMORY_ERROR;
    }
    
    // Decompress the data
    uint64_t actual_size = decompressed_size;
    int result = lzma_decompress_buffer(input_buffer, input_size, *output_buffer, &actual_size);
    
    if (result != 0) {
        free(*output_buffer);
        *output_buffer = NULL;
        *output_size = 0;
        return LZMA_DECOMPRESSOR_COMPRESSION_ERROR;
    }
    
    *output_size = (size_t)actual_size;
    return LZMA_DECOMPRESSOR_OK;
}

/**
 * Decompress a file using LZMA2
 * 
 * This function decompresses the input file using the LZMA2 algorithm
 * and writes the decompressed data to the output file.
 * 
 * input_path: Path to the compressed file
 * output_path: Path where the decompressed file will be written
 * returns: Error code (LZMA_DECOMPRESSOR_OK on success)
 */
LzmaDecompressorError lzma_decompressor_decompress_file(
    const char* input_path,
    const char* output_path
) {
    if (!input_path || !output_path) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid input or output path parameters");
        return LZMA_DECOMPRESSOR_INVALID_PARAMETER;
    }
    
    // Open the input file
    FILE* input_file = fopen(input_path, "rb");
    if (!input_file) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to open input file: %s", input_path);
        return LZMA_DECOMPRESSOR_FILE_ERROR;
    }
    
    // Determine the input file size
    fseek(input_file, 0, SEEK_END);
    long input_size = ftell(input_file);
    rewind(input_file);
    
    if (input_size <= 0) {
        fclose(input_file);
        snprintf(s_error_message, sizeof(s_error_message),
                "Invalid input file size: %s", input_path);
        return LZMA_DECOMPRESSOR_FILE_ERROR;
    }
    
    // Allocate memory for the input data
    void* input_buffer = malloc(input_size);
    if (!input_buffer) {
        fclose(input_file);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to allocate memory for input buffer");
        return LZMA_DECOMPRESSOR_MEMORY_ERROR;
    }
    
    // Read the input file into the buffer
    size_t read = fread(input_buffer, 1, input_size, input_file);
    fclose(input_file);
    
    if (read != (size_t)input_size) {
        free(input_buffer);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to read input file: %s", input_path);
        return LZMA_DECOMPRESSOR_FILE_ERROR;
    }
    
    // Decompress the input buffer
    void* output_buffer;
    size_t output_size;
    
    LzmaDecompressorError error = lzma_decompressor_decompress_buffer(
        input_buffer, input_size, &output_buffer, &output_size);
    
    free(input_buffer);
    
    if (error != LZMA_DECOMPRESSOR_OK) {
        return error;
    }
    
    // Open the output file
    FILE* output_file = fopen(output_path, "wb");
    if (!output_file) {
        free(output_buffer);
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to open output file: %s", output_path);
        return LZMA_DECOMPRESSOR_FILE_ERROR;
    }
    
    // Write the decompressed data to the output file
    size_t written = fwrite(output_buffer, 1, output_size, output_file);
    
    free(output_buffer);
    fclose(output_file);
    
    if (written != output_size) {
        snprintf(s_error_message, sizeof(s_error_message),
                "Failed to write output file: %s", output_path);
        return LZMA_DECOMPRESSOR_FILE_ERROR;
    }
    
    return LZMA_DECOMPRESSOR_OK;
}

/**
 * Free a buffer allocated by lzma_decompressor_decompress_buffer
 * 
 * This function releases memory allocated for a decompressed buffer.
 * 
 * buffer: The buffer to free
 */
void lzma_decompressor_free_buffer(void* buffer) {
    if (buffer) {
        free(buffer);
    }
}

/**
 * Get the last error message from the decompressor
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any decompressor function.
 * 
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* lzma_decompressor_get_error(void) {
    if (s_error_message[0] == '\0') {
        return NULL;
    }
    
    return s_error_message;
} 