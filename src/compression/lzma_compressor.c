/**
 * lzma_compressor.c
 * 
 * Implementation of LZMA2 compression functionality.
 */

#include "lzma/LzmaEnc.h"
#include "lzma/LzmaDec.h"
#include "lzma/Alloc.h"
#include "lzma/Types.h"

#include "compression/lzma_compressor.h"
#include "compression/lzma_decompressor.h" /* Include for function declarations only */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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

/* LZMA2 stream callbacks */
typedef struct {
    ISeqInStream  in_stream;
    FILE*         in_file;
} FileInStream;

typedef struct {
    ISeqOutStream out_stream;
    FILE*         out_file;
    uint64_t      processed_size;
    CompressionProgressCallback user_callback;
    uint64_t      total_size;
    void*         user_data;
} FileOutStream;

/* Input stream read callback */
static SRes file_in_read(const ISeqInStream* p, void* buf, size_t* size) {
    FileInStream* stream = (FileInStream*)p;
    if (!stream || !stream->in_file || !buf || !size) {
        return SZ_ERROR_PARAM;
    }
    
    size_t read_size = fread(buf, 1, *size, stream->in_file);
    *size = read_size;
    return (read_size == 0 && ferror(stream->in_file)) ? SZ_ERROR_READ : SZ_OK;
}

/* Output stream write callback */
static size_t file_out_write(const ISeqOutStream* p, const void* buf, size_t size) {
    FileOutStream* stream = (FileOutStream*)p;
    if (!stream || !stream->out_file || !buf) {
        return 0;
    }
    
    size_t written = fwrite(buf, 1, size, stream->out_file);
    
    stream->processed_size += written;
    
    // Call user callback if provided
    if (stream->user_callback) {
        if (!stream->user_callback(stream->total_size, stream->processed_size, stream->user_data)) {
            // User requested cancel
            return 0;
        }
    }
    
    return written;
}

/* LZMA progress callback */
static SRes lzma_progress_callback(void* p, UInt64 in_size, UInt64 out_size) {
    FileOutStream* stream = (FileOutStream*)p;
    if (!stream) {
        return SZ_ERROR_PARAM;
    }
    
    // Update processed size
    stream->processed_size = out_size;
    
    // Call user callback if provided
    if (stream->user_callback) {
        if (!stream->user_callback(stream->total_size, in_size, stream->user_data)) {
            // User requested cancel
            return SZ_ERROR_PROGRESS;
        }
    }
    
    return SZ_OK;
}

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
                         uint32_t dictionary_size, int compression_level) {
    if (!input_data || input_size == 0 || 
        (!output_data && output_size && *output_size > 0) || 
        !output_size ||
        compression_level < MIN_COMPRESSION_LEVEL || 
        compression_level > MAX_COMPRESSION_LEVEL) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Invalid parameters for compression");
        return 1;  // Invalid parameters
    }
    
    // Set up LZMA properties
    CLzmaEncProps props;
    LzmaEncProps_Init(&props);
    
    // Set compression level
    props.level = compression_level;
    
    // Set dictionary size if provided, otherwise use default based on level
    if (dictionary_size > 0) {
        props.dictSize = dictionary_size;
    }
    
    // Set threads if configured
    if (g_threads > 0) {
        props.numThreads = g_threads;
    }
    
    // Set memory limit if configured
    if (g_memory_limit > 0) {
        props.reduceSize = g_memory_limit;
    }
    
    // Prepare encoder
    LzmaEncProps_Normalize(&props);
    
    // Get the size of the properties header
    size_t props_size = LZMA_PROPS_SIZE;
    
    // Size for compressed data includes LZMA props (5 bytes) and size (8 bytes)
    size_t header_size = LZMA_PROPS_SIZE + 8;
    
    // If only calculating the required buffer size
    if (!output_data && output_size) {
        // Maximum compressed size + header
        *output_size = input_size + input_size / 16 + 64 + header_size;
        return 0;
    }
    
    // Ensure output buffer is large enough
    if (*output_size < header_size) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Output buffer too small for LZMA header");
        return 2;
    }
    
    // Pointer to the properties in the output buffer
    Byte* props_data = (Byte*)output_data;
    
    // Pointer to the actual compressed data (after header)
    Byte* compressed_data = props_data + header_size;
    
    // Available size for compressed data
    size_t compressed_size = *output_size - header_size;
    
    // Encode the data
    SRes res = LzmaEncode(
        compressed_data, &compressed_size,
        (const Byte*)input_data, input_size,
        &props, props_data, &props_size, 0,
        NULL, &g_alloc, &g_alloc);
    
    if (res != SZ_OK) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "LZMA compression failed with error code %d", res);
        return 3;
    }
    
    // Store uncompressed size
    for (int i = 0; i < 8; i++) {
        props_data[LZMA_PROPS_SIZE + i] = (Byte)(input_size >> (i * 8));
    }
    
    // Update output size
    *output_size = header_size + compressed_size;
    
    return 0;  // Success
}

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
                       void* user_data) {
    if (!input_file || !output_file || 
        compression_level < MIN_COMPRESSION_LEVEL || 
        compression_level > MAX_COMPRESSION_LEVEL) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "Invalid parameters for file compression");
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
    
    // Open output file
    FILE* out = fopen(output_file, "wb");
    if (!out) {
        fclose(in);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to open output file: %s", output_file);
        return 3;  // Failed to open output file
    }
    
    // Set up file streams
    FileInStream in_stream;
    FileOutStream out_stream;
    
    in_stream.in_stream.Read = file_in_read;
    in_stream.in_file = in;
    
    out_stream.out_stream.Write = file_out_write;
    out_stream.out_file = out;
    out_stream.processed_size = 0;
    out_stream.user_callback = progress_callback;
    out_stream.total_size = input_size;
    out_stream.user_data = user_data;
    
    // Set up LZMA encoder
    CLzmaEncHandle enc = LzmaEnc_Create(&g_alloc);
    if (!enc) {
        fclose(in);
        fclose(out);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to create LZMA encoder");
        return 4;  // Failed to create encoder
    }
    
    // Set up LZMA properties
    CLzmaEncProps props;
    LzmaEncProps_Init(&props);
    
    // Set compression level
    props.level = compression_level;
    
    // Set threads if configured
    if (g_threads > 0) {
        props.numThreads = g_threads;
    }
    
    // Set memory limit if configured
    if (g_memory_limit > 0) {
        props.reduceSize = g_memory_limit;
    }
    
    // Set properties
    SRes res = LzmaEnc_SetProps(enc, &props);
    if (res != SZ_OK) {
        LzmaEnc_Destroy(enc, &g_alloc, &g_alloc);
        fclose(in);
        fclose(out);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to set LZMA properties: %d", res);
        return 5;  // Failed to set properties
    }
    
    // Write LZMA header (properties)
    Byte header[LZMA_PROPS_SIZE];
    size_t header_size = LZMA_PROPS_SIZE;
    res = LzmaEnc_WriteProperties(enc, header, &header_size);
    if (res != SZ_OK || header_size != LZMA_PROPS_SIZE) {
        LzmaEnc_Destroy(enc, &g_alloc, &g_alloc);
        fclose(in);
        fclose(out);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to write LZMA properties: %d", res);
        return 6;  // Failed to write properties
    }
    
    if (fwrite(header, 1, header_size, out) != header_size) {
        LzmaEnc_Destroy(enc, &g_alloc, &g_alloc);
        fclose(in);
        fclose(out);
        snprintf(s_error_message, sizeof(s_error_message), 
                "Failed to write LZMA header");
        return 7;  // Failed to write header
    }
    
    // Write uncompressed size (8 bytes, little-endian)
    for (int i = 0; i < 8; i++) {
        Byte b = (Byte)(input_size >> (i * 8));
        if (fwrite(&b, 1, 1, out) != 1) {
            LzmaEnc_Destroy(enc, &g_alloc, &g_alloc);
            fclose(in);
            fclose(out);
            snprintf(s_error_message, sizeof(s_error_message), 
                    "Failed to write uncompressed size");
            return 8;  // Failed to write size
        }
    }
    
    // Encode the data
    res = LzmaEnc_Encode(enc, 
                         (ISeqOutStream*)&out_stream, 
                         (ISeqInStream*)&in_stream, 
                         NULL, &g_alloc, &g_alloc);
    
    // Clean up
    LzmaEnc_Destroy(enc, &g_alloc, &g_alloc);
    fclose(in);
    fclose(out);
    
    if (res != SZ_OK) {
        snprintf(s_error_message, sizeof(s_error_message), 
                "LZMA compression failed: %d", res);
        return 9;  // Compression failed
    }
    
    return 0;  // Success
}

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
uint64_t lzma_maximum_compressed_size(uint64_t input_size) {
    // LZMA can expand data if it's not compressible
    // Header size: LZMA props (5 bytes) + uncompressed size (8 bytes)
    const uint64_t header_size = LZMA_PROPS_SIZE + 8;
    
    // Worst case expansion: 1.5x original size + overhead
    return input_size + (input_size / 2) + header_size + 64;
}

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
int lzma_set_compression_parameters(uint32_t threads, uint64_t memory_limit) {
    g_threads = threads;
    g_memory_limit = memory_limit;
    return 0;  // Success
}

/* Remove the decompression functions from lzma_compressor.c as they should only be in lzma_decompressor.c */

/**
 * Get the last error message from the LZMA compressor
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any LZMA function.
 * 
 * Return: Pointer to a string describing the last error, or NULL if no error occurred
 */
const char* lzma_get_error_message() {
    return s_error_message[0] != '\0' ? s_error_message : NULL;
} 