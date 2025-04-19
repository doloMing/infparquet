#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <cstring>
#include <filesystem>
#include "framework/infparquet_framework.h"
#include "framework/command_parser.h"

// Retain necessary platform-specific headers
#ifdef _WIN32
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

using namespace infparquet;

/**
 * Progress callback function
 * 
 * percent: Progress percentage (0-100)
 * message: Progress message
 * 
 * Returns: true to continue, false to abort
 */
bool progressCallback(int percent, const std::string& message) {
    const int barWidth = 50;
    int pos = barWidth * percent / 100;
    
    std::cout << "[";
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos) std::cout << "=";
        else if (i == pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << percent << "% " << message << "\r";
    std::cout.flush();
    
    if (percent >= 100) {
        std::cout << std::endl;
    }
    
    return true;
}

// Check if the directory exists and create it if it doesn't
bool ensureDirectoryExists(const std::string& path) {
    if (path.empty()) return false;
    
    try {
        // Use C++17 filesystem library to create directory
        if (std::filesystem::exists(path)) {
            return std::filesystem::is_directory(path);
        }
        return std::filesystem::create_directories(path);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "filesystem error: " << e.what() << std::endl;
        
        // Traditional method as a fallback
#ifdef _WIN32
        return _mkdir(path.c_str()) == 0 || errno == EEXIST;
#else
        return mkdir(path.c_str(), 0755) == 0 || errno == EEXIST;
#endif
    }
}

// Normalize path, removing unnecessary checks
std::string normalizePath(const std::string& path) {
    if (path.empty()) return path;
    
    std::string result = path;
    
    // Ensure path ends with a separator
    if (result.back() != std::filesystem::path::preferred_separator) {
        result += std::filesystem::path::preferred_separator;
    }
    
    return result;
}

/**
 * Main entry point for the InfParquet tool
 */
int main(int argc, char* argv[]) {
    // Create command parser
    CommandParser parser;
    
    // Parse command line arguments
    CommandArgs args = parser.parse(argc, argv);
    
    // Check if command is valid
    if (args.command == CommandType::Invalid) {
        std::cerr << parser.getLastError() << std::endl;
        parser.showUsage("");
        return 1;
    }
    
    // Handle help command
    if (args.command == CommandType::Help) {
        parser.showUsage("");
        return 0;
    }
    
    // Create InfParquet instance
    InfParquet infparquet;
    
    // Set verbose mode if requested
    if (args.verbose) {
        infparquet.setVerbose(true);
    }
    
    // Set progress callback
    infparquet.setProgressCallback([](const std::string& operation, int row_group_index, 
                                  int total_row_groups, int percent_complete) {
        std::string message = operation;
        if (row_group_index >= 0) {
            message += " [RowGroup " + std::to_string(row_group_index + 1) + "/" + 
                      std::to_string(total_row_groups) + "]";
        }
        return progressCallback(percent_complete, message);
    });
    
    // Normalize all paths
    if (!args.input_path.empty()) args.input_path = normalizePath(args.input_path);
    if (!args.output_path.empty()) args.output_path = normalizePath(args.output_path);
    if (!args.custom_metadata_file.empty()) args.custom_metadata_file = normalizePath(args.custom_metadata_file);
    
    // Process command
    bool success = false;
    switch (args.command) {
        case CommandType::Compress: {
            // Configure compression options
            CompressionOptions options;
            options.compression_level = args.compression_level;
            options.generate_base_metadata = args.use_basic_metadata;
            options.generate_custom_metadata = !args.custom_metadata_file.empty();
            options.custom_metadata_config = args.custom_metadata_file;
            options.parallel_tasks = args.threads;
            
            // Load custom metadata from config file if specified
            if (!args.custom_metadata_file.empty()) {
                if (!infparquet.loadCustomMetadataFromJson(args.custom_metadata_file)) {
                    std::cerr << "Error: Failed to load custom metadata configuration: " 
                              << infparquet.getLastError() << std::endl;
                    return 1;
                }
            }
            
            // Ensure output directory exists - now compatible with std::string parameter
            if (!ensureDirectoryExists(args.output_path)) {
                std::cerr << "Error: Failed to create output directory: " << args.output_path << std::endl;
                return 1;
            }
            
            // Compress the file
            std::cout << "Compressing " << args.input_path << " to " << args.output_path << std::endl;
            success = infparquet.compressParquetFile(args.input_path, args.output_path, 
                                                   args.compression_level, args.threads, 
                                                   args.use_basic_metadata);
            break;
        }
        
        case CommandType::Decompress: {
            // Configure decompression options
            DecompressionOptions options;
            options.output_directory = args.output_path;
            options.parallel_tasks = args.threads;
            
            // Ensure output directory exists - now compatible with std::string parameter
            if (!ensureDirectoryExists(args.output_path)) {
                std::cerr << "Error: Failed to create output directory: " << args.output_path << std::endl;
                return 1;
            }
            
            // Decompress the file
            std::cout << "Decompressing from " << args.input_path << " to " << args.output_path << std::endl;
            success = infparquet.decompressParquetFile(args.input_path, args.output_path, args.threads);
            break;
        }
        
        case CommandType::List: {
            // List metadata
            std::cout << "Listing metadata for " << args.input_path << std::endl;
            std::string metadata = infparquet.listMetadata(args.input_path);
            
            if (!metadata.empty()) {
                std::cout << metadata << std::endl;
                success = true;
            } else {
                std::cerr << "Error: Failed to list metadata: " << infparquet.getLastError() << std::endl;
            }
            break;
        }
        
        case CommandType::Query: {
            // Query metadata
            std::cout << "Querying metadata in " << args.input_path << " with query: " << args.query << std::endl;
            std::string result = infparquet.queryMetadata(args.input_path, args.query);
            
            if (!result.empty()) {
                std::cout << result << std::endl;
                success = true;
            } else {
                std::cerr << "Error: Failed to query metadata: " << infparquet.getLastError() << std::endl;
            }
            break;
        }
        
        default:
            std::cerr << "Error: Unknown command" << std::endl;
            parser.showUsage("");
            return 1;
    }
    
    // Check for errors
    if (!success) {
        std::cerr << "Error: " << infparquet.getLastError() << std::endl;
        return 1;
    }
    
    return 0;
} 