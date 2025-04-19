#include "framework/command_parser.h"
#include "framework/infparquet_framework.h"
#include <string>
#include <vector>
#include <memory>
#include <map>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace infparquet {

// windows platform path processing
#ifdef _WIN32
#define PATH_SEPARATOR '\\'
#define PATH_SEPARATOR_STR "\\"
#define IS_PATH_SEPARATOR(c) ((c) == '\\' || (c) == '/')
#else
#define PATH_SEPARATOR '/'
#define PATH_SEPARATOR_STR "/"
#define IS_PATH_SEPARATOR(c) ((c) == '/')
#endif

// unified path processing function
static char* normalize_path(const char* path) {
    if (!path) return NULL;
    
    size_t len = strlen(path);
    char* normalized = (char*)malloc(len + 1);
    if (!normalized) return NULL;
    
    strcpy(normalized, path);
    
    // unified path separator processing
    for (size_t i = 0; i < len; i++) {
        if (normalized[i] == '/' || normalized[i] == '\\') {
            normalized[i] = PATH_SEPARATOR;
        }
    }
    
    // ensure the path ends with a separator
    if (len > 0 && !IS_PATH_SEPARATOR(normalized[len-1])) {
        normalized = (char*)realloc(normalized, len + 2);
        if (!normalized) return NULL;
        normalized[len] = PATH_SEPARATOR;
        normalized[len+1] = '\0';
    }
    
    return normalized;
}

// Private implementation class definition
struct CommandParser::Impl {
    Impl() {
        buildHelpText();
        buildVersionText();
    }
    
    ~Impl() = default;
    
    // Help text for the command line interface
    std::string help_text;
    
    // Version information
    std::string version_text;
    
    // Last error message
    std::string last_error;
    
    // Build the help text
    void buildHelpText() {
        std::ostringstream ss;
        
        ss << "InfParquet - A specialized Parquet file compression and metadata framework\n\n";
        ss << "Usage:\n";
        ss << "  infparquet <command> [options]\n\n";
        ss << "Commands:\n";
        ss << "  compress <input_file.parquet> --output-dir <output_directory>\n";
        ss << "    Compress a Parquet file using LZMA2 and generate metadata.\n\n";
        ss << "  decompress <metadata_file.meta> --output-dir <output_directory>\n";
        ss << "    Decompress a previously compressed Parquet file.\n\n";
        ss << "  query <metadata_directory> --sql \"<query_string>\"\n";
        ss << "    Query metadata files for specific patterns or values.\n\n";
        ss << "  list <metadata_directory>\n";
        ss << "    List all metadata files in a directory.\n\n";
        ss << "  help\n";
        ss << "    Display this help text.\n\n";
        ss << "  version\n";
        ss << "    Display version information.\n\n";
        ss << "Compression Options:\n";
        ss << "  --level <1-9>             Compression level (1=fastest, 9=highest compression)\n";
        ss << "  --no-base-metadata        Don't generate base metadata\n";
        ss << "  --custom-metadata <file>  Use custom metadata configuration from JSON file\n";
        ss << "  --parallel <N>            Use N parallel tasks (default: auto-detect)\n\n";
        ss << "Decompression Options:\n";
        ss << "  --parallel <N>            Use N parallel tasks (default: auto-detect)\n\n";
        ss << "Examples:\n";
        ss << "  infparquet compress data.parquet --output-dir compressed\n";
        ss << "  infparquet decompress compressed/data.parquet.meta --output-dir decompressed\n";
        ss << "  infparquet query metadata_dir --sql \"SELECT * WHERE column_name = 'value'\"\n";
        ss << "  infparquet list metadata_dir\n";
        
        help_text = ss.str();
    }
    
    // Build the version text
    void buildVersionText() {
        std::ostringstream ss;
        
        ss << "InfParquet version 0.1.0\n";
        ss << "Copyright (c) 2023. All rights reserved.\n";
        
        version_text = ss.str();
    }
    
    // Forward declarations of parsing methods
    bool parseCompressCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    bool parseDecompressCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    bool parseListCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    bool parseQueryCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    bool parseHelpCommand(const std::vector<std::string>& args, CommandArgs& command_args);
    
    // Parse command line arguments
    CommandArgs parse(int argc, char* argv[]) {
        CommandArgs args;
        args.command = CommandType::Invalid;
        
        // Check argument count
        if (argc < 2) {
            last_error = "Error: No command specified";
            return args;
        }
        
        // Convert arguments to vector for easier handling
        std::vector<std::string> arg_vec;
        for (int i = 1; i < argc; ++i) {
            arg_vec.push_back(argv[i]);
        }
        
        // Parse command type
        std::string cmd = arg_vec[0];
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), 
                     [](unsigned char c) { return std::tolower(c); });
        
        arg_vec.erase(arg_vec.begin()); // Remove command argument
        
        // Choose parsing method based on command type
        if (cmd == "compress") {
            args.command = CommandType::Compress;
            if (!parseCompressCommand(arg_vec, args)) {
                return args;
            }
        } else if (cmd == "decompress") {
            args.command = CommandType::Decompress;
            if (!parseDecompressCommand(arg_vec, args)) {
                return args;
            }
        } else if (cmd == "list") {
            args.command = CommandType::List;
            if (!parseListCommand(arg_vec, args)) {
                return args;
            }
        } else if (cmd == "query") {
            args.command = CommandType::Query;
            if (!parseQueryCommand(arg_vec, args)) {
                return args;
            }
        } else if (cmd == "help") {
            args.command = CommandType::Help;
            if (!parseHelpCommand(arg_vec, args)) {
                return args;
            }
        } else {
            last_error = "Error: Unknown command '" + cmd + "'";
            return args;
        }
        
        // Validate parsed arguments
        if (!validateArgs(args)) {
            return args;
        }
        
        // process command related file paths, ensuring cross-platform compatibility
        if (!args.input_path.empty()) {
            char* normalized = normalize_path(args.input_path.c_str());
            if (normalized) {
                args.input_path = normalized;
                free(normalized);
            }
        }
        
        if (!args.output_path.empty()) {
            char* normalized = normalize_path(args.output_path.c_str());
            if (normalized) {
                args.output_path = normalized;
                free(normalized);
            }
        }
        
        return args;
    }
    
    // Validate parsed arguments
    bool validateArgs(const CommandArgs& args) {
        switch (args.command) {
            case CommandType::Compress:
                if (args.input_path.empty()) {
                    last_error = "Error: Compress command missing input file path";
                    return false;
                }
                if (args.output_path.empty()) {
                    last_error = "Error: Compress command missing output directory path";
                    return false;
                }
                if (args.compression_level < 1 || args.compression_level > 9) {
                    last_error = "Error: Compression level must be between 1 and 9";
                    return false;
                }
                break;
                
            case CommandType::Decompress:
                if (args.input_path.empty()) {
                    last_error = "Error: Decompress command missing metadata file path";
                    return false;
                }
                if (args.output_path.empty()) {
                    last_error = "Error: Decompress command missing output directory path";
                    return false;
                }
                break;
                
            case CommandType::List:
                if (args.input_path.empty()) {
                    last_error = "Error: List command missing metadata directory path";
                    return false;
                }
                break;
                
            case CommandType::Query:
                if (args.input_path.empty()) {
                    last_error = "Error: Query command missing metadata directory path";
                    return false;
                }
                if (args.query.empty()) {
                    last_error = "Error: Query command missing query string";
                    return false;
                }
                break;
                
            case CommandType::Help:
                // No additional validation needed
                break;
                
            case CommandType::Invalid:
                last_error = "Error: Invalid command";
                return false;
        }
        
        return true;
    }
};

// Parse compress command arguments
bool CommandParser::Impl::parseCompressCommand(const std::vector<std::string>& args, CommandArgs& command_args) {
    if (args.empty()) {
        last_error = "Error: Compress command missing input file";
        return false;
    }
    
    // Set input file path
    command_args.input_path = args[0];
    
    // Parse options
    for (size_t i = 1; i < args.size(); ++i) {
        const std::string& option = args[i];
        
        if (option == "--output-dir" || option == "-o") {
            if (i + 1 < args.size()) {
                command_args.output_path = args[++i];
            } else {
                last_error = "Error: --output-dir option missing value";
                return false;
            }
        } else if (option == "--level" || option == "-l") {
            if (i + 1 < args.size()) {
                try {
                    command_args.compression_level = std::stoi(args[++i]);
                } catch (const std::exception&) {
                    last_error = "Error: Invalid compression level '" + args[i] + "'";
                    return false;
                }
            } else {
                last_error = "Error: --level option missing value";
                return false;
            }
        } else if (option == "--no-base-metadata") {
            command_args.use_basic_metadata = false;
        } else if (option == "--custom-metadata") {
            if (i + 1 < args.size()) {
                command_args.custom_metadata_file = args[++i];
            } else {
                last_error = "Error: --custom-metadata option missing value";
                return false;
            }
        } else if (option == "--parallel" || option == "-p") {
            if (i + 1 < args.size()) {
                try {
                    command_args.threads = std::stoi(args[++i]);
                } catch (const std::exception&) {
                    last_error = "Error: Invalid number of parallel tasks '" + args[i] + "'";
                    return false;
                }
            } else {
                last_error = "Error: --parallel option missing value";
                return false;
            }
        } else if (option == "--verbose" || option == "-v") {
            command_args.verbose = true;
        } else {
            last_error = "Error: Unknown option '" + option + "'";
            return false;
        }
    }
    
    return true;
}

// Parse decompress command arguments
bool CommandParser::Impl::parseDecompressCommand(const std::vector<std::string>& args, CommandArgs& command_args) {
    if (args.empty()) {
        last_error = "Error: Decompress command missing metadata file";
        return false;
    }
    
    // Set metadata file path
    command_args.input_path = args[0];
    
    // Parse options
    for (size_t i = 1; i < args.size(); ++i) {
        const std::string& option = args[i];
        
        if (option == "--output-dir" || option == "-o") {
            if (i + 1 < args.size()) {
                command_args.output_path = args[++i];
            } else {
                last_error = "Error: --output-dir option missing value";
                return false;
            }
        } else if (option == "--parallel" || option == "-p") {
            if (i + 1 < args.size()) {
                try {
                    command_args.threads = std::stoi(args[++i]);
                } catch (const std::exception&) {
                    last_error = "Error: Invalid number of parallel tasks '" + args[i] + "'";
                    return false;
                }
            } else {
                last_error = "Error: --parallel option missing value";
                return false;
            }
        } else if (option == "--verbose" || option == "-v") {
            command_args.verbose = true;
        } else {
            last_error = "Error: Unknown option '" + option + "'";
            return false;
        }
    }
    
    return true;
}

// Parse list command arguments
bool CommandParser::Impl::parseListCommand(const std::vector<std::string>& args, CommandArgs& command_args) {
    if (args.empty()) {
        last_error = "Error: List command missing metadata directory";
        return false;
    }
    
    // Set metadata directory path
    command_args.input_path = args[0];
    
    // Parse options
    for (size_t i = 1; i < args.size(); ++i) {
        const std::string& option = args[i];
        
        if (option == "--verbose" || option == "-v") {
            command_args.verbose = true;
        } else {
            last_error = "Error: Unknown option '" + option + "'";
            return false;
        }
    }
    
    return true;
}

// Parse query command arguments
bool CommandParser::Impl::parseQueryCommand(const std::vector<std::string>& args, CommandArgs& command_args) {
    if (args.empty()) {
        last_error = "Error: Query command missing metadata directory";
        return false;
    }
    
    // Set metadata directory path
    command_args.input_path = args[0];
    
    // Parse options
    for (size_t i = 1; i < args.size(); ++i) {
        const std::string& option = args[i];
        
        if (option == "--sql" || option == "-s") {
            if (i + 1 < args.size()) {
                command_args.query = args[++i];
            } else {
                last_error = "Error: --sql option missing value";
                return false;
            }
        } else if (option == "--verbose" || option == "-v") {
            command_args.verbose = true;
        } else {
            last_error = "Error: Unknown option '" + option + "'";
            return false;
        }
    }
    
    return true;
}

// Parse help command arguments
bool CommandParser::Impl::parseHelpCommand(const std::vector<std::string>& args, CommandArgs& command_args) {
    // Help command doesn't need extra processing
    return true;
}

// Constructor
CommandParser::CommandParser() 
    : pImpl(std::make_unique<Impl>()) {
}

// Destructor
CommandParser::~CommandParser() = default;

// Parse command line arguments
CommandArgs CommandParser::parse(int argc, char* argv[]) {
    return pImpl->parse(argc, argv);
}

// Display usage information
void CommandParser::showUsage(const std::string& command) const {
    if (command.empty()) {
        std::cout << pImpl->help_text << std::endl;
    } else {
        // Display help for specific command
        std::ostringstream ss;
        
        if (command == "compress") {
            ss << "InfParquet Compress Command:\n";
            ss << "  infparquet compress <input_file.parquet> --output-dir <output_directory> [options]\n\n";
            ss << "Options:\n";
            ss << "  --output-dir, -o <dir>    Specify output directory\n";
            ss << "  --level, -l <1-9>         Compression level (1=fastest, 9=highest compression, default:5)\n";
            ss << "  --no-base-metadata        Don't generate base metadata\n";
            ss << "  --custom-metadata <file>  Use custom metadata configuration (JSON format)\n";
            ss << "  --parallel, -p <N>        Use N parallel tasks (0=auto-detect, default:0)\n";
            ss << "  --verbose, -v             Enable verbose output\n";
        } else if (command == "decompress") {
            ss << "InfParquet Decompress Command:\n";
            ss << "  infparquet decompress <metadata_file.meta> --output-dir <output_directory> [options]\n\n";
            ss << "Options:\n";
            ss << "  --output-dir, -o <dir>    Specify output directory\n";
            ss << "  --parallel, -p <N>        Use N parallel tasks (0=auto-detect, default:0)\n";
            ss << "  --verbose, -v             Enable verbose output\n";
        } else if (command == "list") {
            ss << "InfParquet List Command:\n";
            ss << "  infparquet list <metadata_directory> [options]\n\n";
            ss << "Options:\n";
            ss << "  --verbose, -v             Enable verbose output\n";
        } else if (command == "query") {
            ss << "InfParquet Query Command:\n";
            ss << "  infparquet query <metadata_directory> --sql \"<query_string>\" [options]\n\n";
            ss << "Options:\n";
            ss << "  --sql, -s <query>         SQL-style query string\n";
            ss << "  --verbose, -v             Enable verbose output\n";
        } else {
            ss << "Unknown command: " << command << "\n";
            ss << "Use 'infparquet help' to see available commands\n";
        }
        
        std::cout << ss.str() << std::endl;
    }
}

// Get the last error message
std::string CommandParser::getLastError() const {
    return pImpl->last_error;
}

// Validate parsed arguments
bool CommandParser::validateArgs(const CommandArgs& args) {
    return pImpl->validateArgs(args);
}

} // namespace infparquet 