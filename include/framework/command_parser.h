/**
 * command_parser.h
 * 
 * This header file defines the interface for parsing command-line arguments
 * for the InfParquet tool. It provides functions to parse and validate
 * command-line arguments.
 */

#ifndef INFPARQUET_COMMAND_PARSER_H
#define INFPARQUET_COMMAND_PARSER_H

#include <string>
#include <vector>
#include <map>
#include <memory>

namespace infparquet {

/**
 * Enumeration of command types supported by the InfParquet tool
 */
enum class CommandType {
    Compress,       /* Compress a Parquet file */
    Decompress,     /* Decompress a compressed Parquet file */
    List,           /* List metadata information */
    Query,          /* Query metadata */
    Help,           /* Display help information */
    Invalid         /* Invalid command */
};

/**
 * Structure containing parsed command-line arguments
 */
struct CommandArgs {
    CommandType command;                             /* Command type */
    std::string input_path;                          /* Input file or directory path */
    std::string output_path;                         /* Output file or directory path */
    int compression_level = 5;                       /* Compression level (1-9) */
    int threads = 0;                                 /* Number of threads (0 for automatic) */
    bool use_basic_metadata = true;                  /* Whether to use basic metadata */
    std::string query;                               /* Query string for metadata querying */
    std::string custom_metadata_file;                /* Path to custom metadata JSON file */
    bool verbose = false;                            /* Whether to enable verbose output */
    std::vector<std::string> custom_metadata_items;  /* List of custom metadata items */
    std::map<std::string, std::string> options;      /* Additional options */
};

/**
 * Class for parsing command-line arguments
 */
class CommandParser {
public:
    /**
     * Constructor
     */
    CommandParser();
    
    /**
     * Destructor
     */
    ~CommandParser();
    
    /**
     * Parses command-line arguments
     * 
     * argc: Number of arguments
     * argv: Array of argument strings
     * 
     * Return: Parsed command arguments
     */
    CommandArgs parse(int argc, char* argv[]);
    
    /**
     * Displays usage information
     * 
     * command: Specific command to show usage for, or empty for general usage
     */
    void showUsage(const std::string& command = "") const;
    
    /**
     * Gets the last error message
     * 
     * Return: Last error message, or empty string if no error occurred
     */
    std::string getLastError() const;
    
    /**
     * Validates parsed command arguments
     * 
     * args: Command arguments to validate
     * 
     * Return: true if valid, false otherwise
     */
    bool validateArgs(const CommandArgs& args);
    
private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
    
    /**
     * Parses a compression command
     * 
     * args: Vector of arguments
     * command_args: Structure to store parsed arguments
     * 
     * Return: true if successful, false otherwise
     */
    bool parseCompressCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    
    /**
     * Parses a decompression command
     * 
     * args: Vector of arguments
     * command_args: Structure to store parsed arguments
     * 
     * Return: true if successful, false otherwise
     */
    bool parseDecompressCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    
    /**
     * Parses a list command
     * 
     * args: Vector of arguments
     * command_args: Structure to store parsed arguments
     * 
     * Return: true if successful, false otherwise
     */
    bool parseListCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    
    /**
     * Parses a query command
     * 
     * args: Vector of arguments
     * command_args: Structure to store parsed arguments
     * 
     * Return: true if successful, false otherwise
     */
    bool parseQueryCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    
    /**
     * Parses a help command
     * 
     * args: Vector of arguments
     * command_args: Structure to store parsed arguments
     * 
     * Return: true if successful, false otherwise
     */
    bool parseHelpCommand(const std::vector<std::string>& args, CommandArgs& command_args);
};

} // namespace infparquet

#endif /* INFPARQUET_COMMAND_PARSER_H */ 