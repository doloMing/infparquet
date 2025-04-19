#include "metadata/json_helper.h"
#include "metadata/metadata_types.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstring>
#include <cstdarg>

// Include nlohmann/json
#include "json/json.hpp"
using json = nlohmann::json;

// Static error message buffer
static char s_error_message[1024] = {0};

// Set the error message
static void set_error(const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(s_error_message, sizeof(s_error_message), format, args);
    va_end(args);
}

// Convert timestamp metadata to JSON
static void timestamp_to_json(json& j, const TimestampMetadata& metadata) {
    j = {
        {"min_timestamp", metadata.min_timestamp},
        {"max_timestamp", metadata.max_timestamp},
        {"count", metadata.count}
    };
}

// Convert string metadata to JSON
static void string_to_json(json& j, const StringMetadata& metadata) {
    j = {
        {"high_freq_strings", json::array()},
        {"high_freq_counts", json::array()},
        {"high_freq_count", metadata.high_freq_count},
        {"special_strings", json::array()},
        {"special_string_counts", json::array()},
        {"special_string_count", metadata.special_string_count},
        {"total_string_count", metadata.total_string_count},
        {"avg_string_length", metadata.avg_string_length}
    };

    // Add high frequency strings
    for (uint32_t i = 0; i < metadata.high_freq_count; i++) {
        json str_item;
        str_item["string"] = metadata.high_freq_strings[i].string;
        str_item["count"] = metadata.high_freq_strings[i].count;
        j["high_freq_strings"].push_back(str_item);
        j["high_freq_counts"].push_back(metadata.high_freq_strings[i].count);
    }

    // Add special strings
    for (uint32_t i = 0; i < metadata.special_string_count; i++) {
        j["special_strings"].push_back(metadata.special_strings[i]);
        j["special_string_counts"].push_back(metadata.special_string_counts[i]);
    }
}

// Convert numeric metadata to JSON
static void numeric_to_json(json& j, const NumericMetadata& metadata) {
    j = {
        {"min_value", metadata.min_value},
        {"max_value", metadata.max_value},
        {"avg_value", metadata.avg_value},
        {"mode_value", metadata.mode_value},
        {"mode_count", metadata.mode_count},
        {"total_count", metadata.total_count},
        {"null_count", metadata.null_count}
    };
}

// Convert categorical metadata to JSON
static void categorical_to_json(json& j, const CategoricalMetadata& metadata) {
    j = {
        {"categories", json::array()},
        {"category_counts", json::array()},
        {"high_freq_category_count", metadata.high_freq_category_count},
        {"total_category_count", metadata.total_category_count},
        {"total_value_count", metadata.total_value_count}
    };

    // Add categories
    for (uint32_t i = 0; i < metadata.high_freq_category_count; i++) {
        j["categories"].push_back(metadata.categories[i]);
        j["category_counts"].push_back(metadata.category_counts[i]);
    }
}

// Convert custom metadata to JSON
static void custom_metadata_to_json(json& j, const CustomMetadataItem& metadata) {
    j = {
        {"name", metadata.name},
        {"sql_query", metadata.sql_query},
        {"result_matrix", metadata.result_matrix ? metadata.result_matrix : ""},
        {"row_group_count", metadata.row_group_count},
        {"column_count", metadata.column_count}
    };
}

// Convert JSON to timestamp metadata
static void json_to_timestamp(const json& j, TimestampMetadata& metadata) {
    metadata.min_timestamp = j.value("min_timestamp", 0);
    metadata.max_timestamp = j.value("max_timestamp", 0);
    metadata.count = j.value("count", (uint64_t)0);
}

// Convert JSON to string metadata
static void json_to_string(const json& j, StringMetadata& metadata) {
    metadata.high_freq_count = j.value("high_freq_count", 0u);
    metadata.special_string_count = j.value("special_string_count", 0u);
    metadata.total_string_count = j.value("total_string_count", (uint64_t)0);
    metadata.avg_string_length = j.value("avg_string_length", 0u);

    // Get high frequency strings
    const json& high_freq_strings = j.value("high_freq_strings", json::array());
    const json& high_freq_counts = j.value("high_freq_counts", json::array());
    
    size_t count = std::min(high_freq_strings.size(), 
                           static_cast<size_t>(MAX_HIGH_FREQ_STRINGS));
    
    for (size_t i = 0; i < count; i++) {
        if (high_freq_strings[i].is_object()) {
            // New format with objects
            strncpy(metadata.high_freq_strings[i].string, 
                   high_freq_strings[i].value("string", "").c_str(),
                   MAX_STRING_LENGTH - 1);
            metadata.high_freq_strings[i].string[MAX_STRING_LENGTH - 1] = '\0';
            metadata.high_freq_strings[i].count = high_freq_strings[i].value("count", 0u);
            metadata.high_freq_counts[i] = metadata.high_freq_strings[i].count;
        } else {
            // Old format with separate arrays
            strncpy(metadata.high_freq_strings[i].string, 
                   high_freq_strings[i].get<std::string>().c_str(),
                   MAX_STRING_LENGTH - 1);
            metadata.high_freq_strings[i].string[MAX_STRING_LENGTH - 1] = '\0';
            
            if (i < high_freq_counts.size()) {
                metadata.high_freq_counts[i] = high_freq_counts[i].get<uint32_t>();
                metadata.high_freq_strings[i].count = metadata.high_freq_counts[i];
            }
        }
    }

    // Get special strings
    const json& special_strings = j.value("special_strings", json::array());
    const json& special_string_counts = j.value("special_string_counts", json::array());
    
    count = std::min(special_strings.size(), 
                    static_cast<size_t>(MAX_SPECIAL_STRINGS));
    
    for (size_t i = 0; i < count; i++) {
        strncpy(metadata.special_strings[i], special_strings[i].get<std::string>().c_str(),
               MAX_STRING_LENGTH - 1);
        metadata.special_strings[i][MAX_STRING_LENGTH - 1] = '\0';
        
        if (i < special_string_counts.size()) {
            metadata.special_string_counts[i] = special_string_counts[i].get<uint32_t>();
        }
    }
}

// Convert JSON to numeric metadata
static void json_to_numeric(const json& j, NumericMetadata& metadata) {
    metadata.min_value = j.value("min_value", 0.0);
    metadata.max_value = j.value("max_value", 0.0);
    metadata.avg_value = j.value("avg_value", 0.0);
    metadata.mode_value = j.value("mode_value", 0.0);
    metadata.mode_count = j.value("mode_count", (uint64_t)0);
    metadata.total_count = j.value("total_count", (uint64_t)0);
    metadata.null_count = j.value("null_count", 0u);
}

// Convert JSON to categorical metadata
static void json_to_categorical(const json& j, CategoricalMetadata& metadata) {
    metadata.high_freq_category_count = j.value("high_freq_category_count", 0u);
    metadata.total_category_count = j.value("total_category_count", 0u);
    metadata.total_value_count = j.value("total_value_count", (uint64_t)0);

    // Get categories
    const json& categories = j.value("categories", json::array());
    const json& category_counts = j.value("category_counts", json::array());
    
    size_t count = std::min(categories.size(), 
                           static_cast<size_t>(MAX_HIGH_FREQ_CATEGORIES));
    
    for (size_t i = 0; i < count; i++) {
        strncpy(metadata.categories[i], categories[i].get<std::string>().c_str(),
               MAX_STRING_LENGTH - 1);
        metadata.categories[i][MAX_STRING_LENGTH - 1] = '\0';
        
        if (i < category_counts.size()) {
            metadata.category_counts[i] = category_counts[i].get<uint32_t>();
        }
    }
}

// Convert JSON to custom metadata
static CustomMetadataItem* json_to_custom_metadata(const json& j) {
    CustomMetadataItem* item = (CustomMetadataItem*)malloc(sizeof(CustomMetadataItem));
    if (!item) {
        return nullptr;
    }
    
    memset(item, 0, sizeof(CustomMetadataItem));
    
    // Copy name and SQL query
    strncpy(item->name, j.value("name", "").c_str(), MAX_METADATA_ITEM_NAME_LENGTH - 1);
    item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
    
    strncpy(item->sql_query, j.value("sql_query", "").c_str(), MAX_STRING_LENGTH - 1);
    item->sql_query[MAX_STRING_LENGTH - 1] = '\0';
    
    // Get matrix dimensions
    item->row_group_count = j.value("row_group_count", 0u);
    item->column_count = j.value("column_count", 0u);
    
    // Get result matrix
    std::string matrix_str = j.value("result_matrix", "");
    if (!matrix_str.empty()) {
        item->result_matrix = strdup(matrix_str.c_str());
    }
    
    return item;
}

// Convert metadata item to JSON
static void metadata_item_to_json(json& j, const MetadataItem& item) {
    j = {
        {"name", item.name},
        {"type", item.type}
    };
    
    // Add type-specific data
    switch (item.type) {
        case METADATA_TYPE_TIMESTAMP:
            timestamp_to_json(j["value"], item.value.timestamp);
            break;
        case METADATA_TYPE_STRING:
            string_to_json(j["value"], item.value.string);
            break;
        case METADATA_TYPE_NUMERIC:
            numeric_to_json(j["value"], item.value.numeric);
            break;
        case METADATA_TYPE_CATEGORICAL:
            categorical_to_json(j["value"], item.value.categorical);
            break;
        default:
            j["value"] = {};
            break;
    }
}

// Convert JSON to metadata item
static MetadataItem* json_to_metadata_item(const json& j) {
    MetadataItem* item = (MetadataItem*)malloc(sizeof(MetadataItem));
    if (!item) {
        return nullptr;
    }
    
    memset(item, 0, sizeof(MetadataItem));
    
    // Copy name and get type
    strncpy(item->name, j.value("name", "").c_str(), MAX_METADATA_ITEM_NAME_LENGTH - 1);
    item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
    
    item->type = static_cast<MetadataType>(j.value("type", 0));
    
    // Get type-specific data
    if (j.contains("value")) {
        const json& value = j["value"];
        
        switch (item->type) {
            case METADATA_TYPE_TIMESTAMP:
                json_to_timestamp(value, item->value.timestamp);
                break;
            case METADATA_TYPE_STRING:
                json_to_string(value, item->value.string);
                break;
            case METADATA_TYPE_NUMERIC:
                json_to_numeric(value, item->value.numeric);
                break;
            case METADATA_TYPE_CATEGORICAL:
                json_to_categorical(value, item->value.categorical);
                break;
            default:
                break;
        }
    }
    
    return item;
}

// Recursively convert metadata to JSON
static json metadata_to_json(const Metadata* metadata) {
    if (!metadata) {
        return json();
    }
    
    json j = {
        {"file_path", metadata->file_path ? metadata->file_path : ""},
        {"use_basic_metadata", metadata->file_metadata.use_basic_metadata},
        {"basic_metadata", json::array()},
        {"custom_metadata", json::array()},
        {"row_group_metadata", json::array()},
        {"column_metadata", json::array()}
    };
    
    // Add basic metadata items
    for (uint32_t i = 0; i < metadata->file_metadata.basic_metadata_count; i++) {
        json item_json;
        metadata_item_to_json(item_json, metadata->file_metadata.basic_metadata[i]);
        j["basic_metadata"].push_back(item_json);
    }
    
    // Add custom metadata items
    for (uint32_t i = 0; i < metadata->file_metadata.custom_metadata_count; i++) {
        json item_json;
        custom_metadata_to_json(item_json, metadata->file_metadata.custom_metadata[i]);
        j["custom_metadata"].push_back(item_json);
    }
    
    // Add row group metadata
    for (uint32_t i = 0; i < metadata->row_group_metadata_count; i++) {
        json rg_json = {
            {"row_group_index", metadata->row_group_metadata[i].row_group_index},
            {"metadata_items", json::array()}
        };
        
        // Add row group metadata items
        for (uint32_t j = 0; j < metadata->row_group_metadata[i].metadata_count; j++) {
            json item_json;
            metadata_item_to_json(item_json, metadata->row_group_metadata[i].metadata[j]);
            rg_json["metadata_items"].push_back(item_json);
        }
        
        j["row_group_metadata"].push_back(rg_json);
    }
    
    // Add column metadata
    for (uint32_t i = 0; i < metadata->column_metadata_count; i++) {
        json col_json = {
            {"column_index", metadata->column_metadata[i].column_index},
            {"column_name", metadata->column_metadata[i].column_name},
            {"metadata_items", json::array()}
        };
        
        // Add column metadata items
        for (uint32_t j = 0; j < metadata->column_metadata[i].metadata_count; j++) {
            json item_json;
            metadata_item_to_json(item_json, metadata->column_metadata[i].metadata[j]);
            col_json["metadata_items"].push_back(item_json);
        }
        
        j["column_metadata"].push_back(col_json);
    }
    
    return j;
}

// Recursively convert JSON to metadata
static Metadata* json_to_metadata(const json& j) {
    Metadata* metadata = (Metadata*)malloc(sizeof(Metadata));
    if (!metadata) {
        return nullptr;
    }
    
    memset(metadata, 0, sizeof(Metadata));
    
    // Get basic properties
    std::string file_path = j.value("file_path", "");
    metadata->file_path = file_path.empty() ? nullptr : strdup(file_path.c_str());
    
    metadata->file_metadata.use_basic_metadata = j.value("use_basic_metadata", true);
    
    // Get basic metadata items
    const json& basic_metadata = j.value("basic_metadata", json::array());
    if (!basic_metadata.empty()) {
        metadata->file_metadata.basic_metadata_count = basic_metadata.size();
        metadata->file_metadata.basic_metadata = (MetadataItem*)malloc(
            metadata->file_metadata.basic_metadata_count * sizeof(MetadataItem)
        );
        
        if (!metadata->file_metadata.basic_metadata) {
            // Free resources and return
            if (metadata->file_path) free(metadata->file_path);
            free(metadata);
            return nullptr;
        }
        
        for (size_t i = 0; i < basic_metadata.size(); i++) {
            MetadataItem* item = json_to_metadata_item(basic_metadata[i]);
            if (item) {
                memcpy(&metadata->file_metadata.basic_metadata[i], item, sizeof(MetadataItem));
                free(item);
            }
        }
    }
    
    // Get custom metadata items
    const json& custom_metadata = j.value("custom_metadata", json::array());
    if (!custom_metadata.empty()) {
        metadata->file_metadata.custom_metadata_count = custom_metadata.size();
        metadata->file_metadata.custom_metadata = (CustomMetadataItem*)malloc(
            metadata->file_metadata.custom_metadata_count * sizeof(CustomMetadataItem)
        );
        
        if (!metadata->file_metadata.custom_metadata) {
            // Free resources and return
            if (metadata->file_path) free(metadata->file_path);
            if (metadata->file_metadata.basic_metadata) free(metadata->file_metadata.basic_metadata);
            free(metadata);
            return nullptr;
        }
        
        for (size_t i = 0; i < custom_metadata.size(); i++) {
            CustomMetadataItem* item = json_to_custom_metadata(custom_metadata[i]);
            if (item) {
                memcpy(&metadata->file_metadata.custom_metadata[i], item, sizeof(CustomMetadataItem));
                // Need to handle the dynamically allocated result_matrix
                if (item->result_matrix) {
                    metadata->file_metadata.custom_metadata[i].result_matrix = item->result_matrix;
                    // Don't free item->result_matrix as it's now owned by the metadata
                    item->result_matrix = nullptr;
                }
                free(item);
            }
        }
    }
    
    // Get row group metadata
    const json& row_group_metadata = j.value("row_group_metadata", json::array());
    if (!row_group_metadata.empty()) {
        metadata->row_group_metadata_count = row_group_metadata.size();
        metadata->row_group_metadata = (RowGroupMetadata*)malloc(
            metadata->row_group_metadata_count * sizeof(RowGroupMetadata)
        );
        
        if (!metadata->row_group_metadata) {
            // Free resources and return
            if (metadata->file_path) free(metadata->file_path);
            if (metadata->file_metadata.basic_metadata) free(metadata->file_metadata.basic_metadata);
            if (metadata->file_metadata.custom_metadata) free(metadata->file_metadata.custom_metadata);
            free(metadata);
            return nullptr;
        }
        
        for (size_t i = 0; i < row_group_metadata.size(); i++) {
            const json& rg_json = row_group_metadata[i];
            RowGroupMetadata* rg = &metadata->row_group_metadata[i];
            
            rg->row_group_index = rg_json.value("row_group_index", 0u);
            
            // Get metadata items
            const json& metadata_items = rg_json.value("metadata_items", json::array());
            if (!metadata_items.empty()) {
                rg->metadata_count = metadata_items.size();
                rg->metadata = (MetadataItem*)malloc(rg->metadata_count * sizeof(MetadataItem));
                
                if (!rg->metadata) {
                    // Skip this row group's metadata
                    rg->metadata_count = 0;
                    continue;
                }
                
                for (size_t j = 0; j < metadata_items.size(); j++) {
                    MetadataItem* item = json_to_metadata_item(metadata_items[j]);
                    if (item) {
                        memcpy(&rg->metadata[j], item, sizeof(MetadataItem));
                        free(item);
                    }
                }
            }
        }
    }
    
    // Get column metadata
    const json& column_metadata = j.value("column_metadata", json::array());
    if (!column_metadata.empty()) {
        metadata->column_metadata_count = column_metadata.size();
        metadata->column_metadata = (ColumnMetadata*)malloc(
            metadata->column_metadata_count * sizeof(ColumnMetadata)
        );
        
        if (!metadata->column_metadata) {
            // Free resources and return (but keep row_group_metadata)
            if (metadata->file_path) free(metadata->file_path);
            if (metadata->file_metadata.basic_metadata) free(metadata->file_metadata.basic_metadata);
            if (metadata->file_metadata.custom_metadata) free(metadata->file_metadata.custom_metadata);
            free(metadata);
            return nullptr;
        }
        
        for (size_t i = 0; i < column_metadata.size(); i++) {
            const json& col_json = column_metadata[i];
            ColumnMetadata* col = &metadata->column_metadata[i];
            
            col->column_index = col_json.value("column_index", 0u);
            
            strncpy(col->column_name, col_json.value("column_name", "").c_str(), 
                   MAX_METADATA_ITEM_NAME_LENGTH - 1);
            col->column_name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
            
            // Get metadata items
            const json& metadata_items = col_json.value("metadata_items", json::array());
            if (!metadata_items.empty()) {
                col->metadata_count = metadata_items.size();
                col->metadata = (MetadataItem*)malloc(col->metadata_count * sizeof(MetadataItem));
                
                if (!col->metadata) {
                    // Skip this column's metadata
                    col->metadata_count = 0;
                    continue;
                }
                
                for (size_t j = 0; j < metadata_items.size(); j++) {
                    MetadataItem* item = json_to_metadata_item(metadata_items[j]);
                    if (item) {
                        memcpy(&col->metadata[j], item, sizeof(MetadataItem));
                        free(item);
                    }
                }
            }
        }
    }
    
    return metadata;
}

// Public API functions
JsonHelperError json_serialize_metadata(const Metadata* metadata, char** json_string) {
    if (!metadata || !json_string) {
        set_error("Invalid parameters");
        return JSON_HELPER_INVALID_PARAMETER;
    }
    
    try {
        // Convert metadata to JSON
        json j = metadata_to_json(metadata);
        
        // Serialize to string
        std::string str = j.dump();
        
        // Allocate memory for the result
        *json_string = strdup(str.c_str());
        if (!*json_string) {
            set_error("Failed to allocate memory for JSON string");
            return JSON_HELPER_MEMORY_ERROR;
        }
        
        return JSON_HELPER_OK;
    } catch (const std::exception& e) {
        set_error("JSON error: %s", e.what());
        return JSON_HELPER_UNKNOWN_ERROR;
    }
}

JsonHelperError json_deserialize_metadata(const char* json_string, Metadata** metadata) {
    if (!json_string || !metadata) {
        set_error("Invalid parameters");
        return JSON_HELPER_INVALID_PARAMETER;
    }
    
    try {
        // Parse JSON string
        json j = json::parse(json_string);
        
        // Convert JSON to metadata
        *metadata = json_to_metadata(j);
        if (!*metadata) {
            set_error("Failed to allocate memory for metadata");
            return JSON_HELPER_MEMORY_ERROR;
        }
        
        return JSON_HELPER_OK;
    } catch (const json::parse_error& e) {
        set_error("JSON parse error: %s", e.what());
        return JSON_HELPER_PARSE_ERROR;
    } catch (const std::exception& e) {
        set_error("JSON error: %s", e.what());
        return JSON_HELPER_UNKNOWN_ERROR;
    }
}

JsonHelperError json_save_metadata_to_file(const Metadata* metadata, const char* file_path) {
    if (!metadata || !file_path) {
        set_error("Invalid parameters");
        return JSON_HELPER_INVALID_PARAMETER;
    }
    
    try {
        // Convert metadata to JSON
        json j = metadata_to_json(metadata);
        
        // Open file for writing
        std::ofstream file(file_path, std::ios::binary);
        if (!file) {
            set_error("Failed to open file for writing: %s", file_path);
            return JSON_HELPER_FILE_ERROR;
        }
        
        // Write JSON to file (using binary format to save space)
        std::vector<uint8_t> binary = json::to_msgpack(j);
        file.write(reinterpret_cast<const char*>(binary.data()), binary.size());
        
        if (!file) {
            set_error("Failed to write to file: %s", file_path);
            return JSON_HELPER_FILE_ERROR;
        }
        
        return JSON_HELPER_OK;
    } catch (const std::exception& e) {
        set_error("JSON error: %s", e.what());
        return JSON_HELPER_UNKNOWN_ERROR;
    }
}

JsonHelperError json_load_metadata_from_file(const char* file_path, Metadata** metadata) {
    if (!file_path || !metadata) {
        set_error("Invalid parameters");
        return JSON_HELPER_INVALID_PARAMETER;
    }
    
    try {
        // Open file for reading
        std::ifstream file(file_path, std::ios::binary);
        if (!file) {
            set_error("Failed to open file for reading: %s", file_path);
            return JSON_HELPER_FILE_ERROR;
        }
        
        // Read file into buffer
        std::vector<uint8_t> binary(
            (std::istreambuf_iterator<char>(file)),
            std::istreambuf_iterator<char>()
        );
        
        if (binary.empty()) {
            set_error("Empty file: %s", file_path);
            return JSON_HELPER_FILE_ERROR;
        }
        
        // Parse binary data
        json j = json::from_msgpack(binary);
        
        // Convert JSON to metadata
        *metadata = json_to_metadata(j);
        if (!*metadata) {
            set_error("Failed to allocate memory for metadata");
            return JSON_HELPER_MEMORY_ERROR;
        }
        
        return JSON_HELPER_OK;
    } catch (const json::parse_error& e) {
        set_error("JSON parse error: %s", e.what());
        return JSON_HELPER_PARSE_ERROR;
    } catch (const std::exception& e) {
        set_error("JSON error: %s", e.what());
        return JSON_HELPER_UNKNOWN_ERROR;
    }
}

JsonHelperError json_parse_custom_metadata_config(const char* file_path, 
                                               std::vector<std::string>& names,
                                               std::vector<std::string>& queries) {
    if (!file_path) {
        set_error("Invalid parameters");
        return JSON_HELPER_INVALID_PARAMETER;
    }
    
    try {
        // Clear output vectors
        names.clear();
        queries.clear();
        
        // Open and read the file
        std::ifstream file(file_path);
        if (!file) {
            set_error("Failed to open file for reading: %s", file_path);
            return JSON_HELPER_FILE_ERROR;
        }
        
        // Parse JSON
        json j;
        file >> j;
        
        // Check if the file contains custom_metadata array
        if (!j.contains("custom_metadata") || !j["custom_metadata"].is_array()) {
            set_error("Invalid custom metadata config: missing or invalid 'custom_metadata' array");
            return JSON_HELPER_PARSE_ERROR;
        }
        
        // Process each custom metadata item
        const json& items = j["custom_metadata"];
        for (const auto& item : items) {
            // Check if item has name and query
            if (!item.contains("name") || !item.contains("query")) {
                continue;  // Skip this item
            }
            
            std::string name = item["name"].get<std::string>();
            std::string query = item["query"].get<std::string>();
            
            names.push_back(name);
            queries.push_back(query);
            
            // Limit to MAX_CUSTOM_METADATA_ITEMS
            if (names.size() >= MAX_CUSTOM_METADATA_ITEMS) {
                break;
            }
        }
        
        return JSON_HELPER_OK;
    } catch (const json::parse_error& e) {
        set_error("JSON parse error: %s", e.what());
        return JSON_HELPER_PARSE_ERROR;
    } catch (const std::exception& e) {
        set_error("JSON error: %s", e.what());
        return JSON_HELPER_UNKNOWN_ERROR;
    }
}

const char* json_helper_get_last_error() {
    return s_error_message[0] != '\0' ? s_error_message : NULL;
} 