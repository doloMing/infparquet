/**
 * metadata_parser.cpp
 * 
 * This file implements functionality for parsing and serializing metadata to/from JSON format.
 */

#include "metadata/metadata_parser.h"
#include "metadata/metadata_types.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "json/json.hpp"

/* Use nlohmann::json library */
using json = nlohmann::json;

/* Forward declaration */
extern "C" void releaseMetadata(Metadata* metadata);

/* Static error message buffer */
static char g_error_message[256] = {0};

/**
 * Returns the last error message
 */
const char* metadata_parser_get_error_message() {
    return g_error_message[0] != '\0' ? g_error_message : NULL;
}

/**
 * Serializes a timestamp metadata item to a JSON object
 */
static void serializeTimestampMetadata(json& j, const TimestampMetadata* metadata) {
    j["min_timestamp"] = metadata->min_timestamp;
    j["max_timestamp"] = metadata->max_timestamp;
    j["count"] = metadata->count;
}

/**
 * Serializes a string metadata item to a JSON object
 */
static void serializeStringMetadata(json& j, const StringMetadata* metadata) {
    json high_freq_array = json::array();
    for (uint32_t i = 0; i < metadata->high_freq_count; i++) {
        json str_item;
        str_item["string"] = metadata->high_freq_strings[i].string;
        str_item["count"] = metadata->high_freq_strings[i].count;
        high_freq_array.push_back(str_item);
    }
    j["high_freq_strings"] = high_freq_array;
    
    json special_array = json::array();
    for (uint32_t i = 0; i < metadata->special_string_count; i++) {
        json str_item;
        str_item["string"] = metadata->special_strings[i];
        str_item["count"] = metadata->special_string_counts[i];
        special_array.push_back(str_item);
    }
    j["special_strings"] = special_array;
    
    j["total_count"] = metadata->total_string_count;
    j["avg_length"] = metadata->avg_string_length;
}

/**
 * Serializes a numeric metadata item to a JSON object
 */
static void serializeNumericMetadata(json& j, const NumericMetadata* metadata) {
    j["min_value"] = metadata->min_value;
    j["max_value"] = metadata->max_value;
    j["avg_value"] = metadata->avg_value;
    j["mode_value"] = metadata->mode_value;
    j["mode_count"] = metadata->mode_count;
    j["total_count"] = metadata->total_count;
    j["null_count"] = metadata->null_count;
}

/**
 * Serializes a categorical metadata item to a JSON object
 */
static void serializeCategoricalMetadata(json& j, const CategoricalMetadata* metadata) {
    json categories_array = json::array();
    for (uint32_t i = 0; i < metadata->high_freq_category_count; i++) {
        json cat_item;
        cat_item["category"] = metadata->categories[i];
        cat_item["count"] = metadata->category_counts[i];
        categories_array.push_back(cat_item);
    }
    j["categories"] = categories_array;
    
    j["total_category_count"] = metadata->total_category_count;
    j["total_value_count"] = metadata->total_value_count;
}

/**
 * Serializes a metadata item to a JSON object
 */
static void serializeMetadataItem(json& j, const MetadataItem* item) {
    j["name"] = item->name;
    
    switch (item->type) {
        case METADATA_TYPE_TIMESTAMP:
            j["type"] = "timestamp";
            serializeTimestampMetadata(j["value"], &item->value.timestamp);
            break;
            
        case METADATA_TYPE_STRING:
            j["type"] = "string";
            serializeStringMetadata(j["value"], &item->value.string);
            break;
            
        case METADATA_TYPE_NUMERIC:
            j["type"] = "numeric";
            serializeNumericMetadata(j["value"], &item->value.numeric);
            break;
            
        case METADATA_TYPE_CATEGORICAL:
            j["type"] = "categorical";
            serializeCategoricalMetadata(j["value"], &item->value.categorical);
            break;
            
        default:
            j["type"] = "unknown";
            break;
    }
}

/**
 * Serializes a custom metadata item to a JSON object
 */
static void serializeCustomMetadataItem(json& j, const CustomMetadataItem* item) {
    j["name"] = item->name;
    j["sql_query"] = item->sql_query;
    j["row_group_count"] = item->row_group_count;
    j["column_count"] = item->column_count;
    
    if (item->result_matrix) {
        j["result_matrix"] = item->result_matrix;
    } else {
        j["result_matrix"] = "";
    }
}

/**
 * Serializes file metadata to a JSON object
 */
static void serializeFileMetadata(json& j, const FileMetadata* metadata) {
    j["use_basic_metadata"] = metadata->use_basic_metadata;
    
    json basic_array = json::array();
    for (uint32_t i = 0; i < metadata->basic_metadata_count; i++) {
        json item_obj;
        serializeMetadataItem(item_obj, &metadata->basic_metadata[i]);
        basic_array.push_back(item_obj);
    }
    j["basic_metadata"] = basic_array;
    
    json custom_array = json::array();
    for (uint32_t i = 0; i < metadata->custom_metadata_count; i++) {
        json item_obj;
        serializeCustomMetadataItem(item_obj, &metadata->custom_metadata[i]);
        custom_array.push_back(item_obj);
    }
    j["custom_metadata"] = custom_array;
}

/**
 * Serializes row group metadata to a JSON object
 */
static void serializeRowGroupMetadata(json& j, const RowGroupMetadata* metadata) {
    j["row_group_index"] = metadata->row_group_index;
    
    json items_array = json::array();
    for (uint32_t i = 0; i < metadata->metadata_count; i++) {
        json item_obj;
        serializeMetadataItem(item_obj, &metadata->metadata[i]);
        items_array.push_back(item_obj);
    }
    j["metadata_items"] = items_array;
}

/**
 * Serializes column metadata to a JSON object
 */
static void serializeColumnMetadata(json& j, const ColumnMetadata* metadata) {
    j["column_index"] = metadata->column_index;
    j["column_name"] = metadata->column_name;
    
    json items_array = json::array();
    for (uint32_t i = 0; i < metadata->metadata_count; i++) {
        json item_obj;
        serializeMetadataItem(item_obj, &metadata->metadata[i]);
        items_array.push_back(item_obj);
    }
    j["metadata_items"] = items_array;
}

/**
 * Deserializes a timestamp metadata item from a JSON object
 */
static bool deserializeTimestampMetadata(const json& j, TimestampMetadata* metadata) {
    try {
        metadata->min_timestamp = j["min_timestamp"].get<time_t>();
        metadata->max_timestamp = j["max_timestamp"].get<time_t>();
        metadata->count = j["count"].get<uint64_t>();
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing timestamp metadata: %s", e.what());
        return false;
    }
}

/**
 * Deserializes a string metadata item from a JSON object
 */
static bool deserializeStringMetadata(const json& j, StringMetadata* metadata) {
    try {
        // Initialize with zeros
        memset(metadata, 0, sizeof(StringMetadata));
        
        // Parse high frequency strings
        const json& high_freq = j["high_freq_strings"];
        for (size_t i = 0; i < high_freq.size() && i < MAX_HIGH_FREQ_STRINGS; i++) {
            strncpy(metadata->high_freq_strings[i].string, 
                   high_freq[i]["string"].get<std::string>().c_str(), 
                   MAX_STRING_LENGTH - 1);
            metadata->high_freq_strings[i].string[MAX_STRING_LENGTH - 1] = '\0';
            metadata->high_freq_counts[i] = high_freq[i]["count"].get<uint32_t>();
            metadata->high_freq_strings[i].count = high_freq[i]["count"].get<uint32_t>();
        }
        metadata->high_freq_count = (high_freq.size() > MAX_HIGH_FREQ_STRINGS) ? 
                                   MAX_HIGH_FREQ_STRINGS : high_freq.size();
        
        // Parse special strings
        const json& special = j["special_strings"];
        for (size_t i = 0; i < special.size() && i < MAX_SPECIAL_STRINGS; i++) {
            strncpy(metadata->special_strings[i], 
                   special[i]["string"].get<std::string>().c_str(), 
                   MAX_STRING_LENGTH - 1);
            metadata->special_strings[i][MAX_STRING_LENGTH - 1] = '\0';
            metadata->special_string_counts[i] = special[i]["count"].get<uint32_t>();
        }
        metadata->special_string_count = (special.size() > MAX_SPECIAL_STRINGS) ? 
                                        MAX_SPECIAL_STRINGS : special.size();
        
        // Parse counts
        metadata->total_string_count = j["total_count"].get<uint64_t>();
        metadata->avg_string_length = j["avg_length"].get<uint32_t>();
        
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing string metadata: %s", e.what());
        return false;
    }
}

/**
 * Deserializes a numeric metadata item from a JSON object
 */
static bool deserializeNumericMetadata(const json& j, NumericMetadata* metadata) {
    try {
        metadata->min_value = j["min_value"].get<double>();
        metadata->max_value = j["max_value"].get<double>();
        metadata->avg_value = j["avg_value"].get<double>();
        metadata->mode_value = j["mode_value"].get<double>();
        metadata->mode_count = j["mode_count"].get<uint64_t>();
        metadata->total_count = j["total_count"].get<uint64_t>();
        metadata->null_count = j["null_count"].get<uint32_t>();
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing numeric metadata: %s", e.what());
        return false;
    }
}

/**
 * Deserializes a categorical metadata item from a JSON object
 */
static bool deserializeCategoricalMetadata(const json& j, CategoricalMetadata* metadata) {
    try {
        // Initialize with zeros
        memset(metadata, 0, sizeof(CategoricalMetadata));
        
        // Parse categories
        const json& categories = j["categories"];
        for (size_t i = 0; i < categories.size() && i < MAX_HIGH_FREQ_CATEGORIES; i++) {
            strncpy(metadata->categories[i], 
                   categories[i]["category"].get<std::string>().c_str(), 
                   MAX_STRING_LENGTH - 1);
            metadata->categories[i][MAX_STRING_LENGTH - 1] = '\0';
            metadata->category_counts[i] = categories[i]["count"].get<uint32_t>();
        }
        metadata->high_freq_category_count = (categories.size() > MAX_HIGH_FREQ_CATEGORIES) ? 
                                           MAX_HIGH_FREQ_CATEGORIES : categories.size();
        
        // Parse counts
        metadata->total_category_count = j["total_category_count"].get<uint32_t>();
        metadata->total_value_count = j["total_value_count"].get<uint64_t>();
        
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing categorical metadata: %s", e.what());
        return false;
    }
}

/**
 * Deserializes a metadata item from a JSON object
 */
static bool deserializeMetadataItem(const json& j, MetadataItem* item) {
    try {
        // Get name
        strncpy(item->name, j["name"].get<std::string>().c_str(), MAX_METADATA_ITEM_NAME_LENGTH - 1);
        item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
        
        // Get type
        std::string type_str = j["type"].get<std::string>();
        
        // Set the appropriate type and deserialize the value
        if (type_str == "timestamp") {
            item->type = METADATA_TYPE_TIMESTAMP;
            if (!deserializeTimestampMetadata(j["value"], &item->value.timestamp)) {
                return false;
            }
        } else if (type_str == "string") {
            item->type = METADATA_TYPE_STRING;
            if (!deserializeStringMetadata(j["value"], &item->value.string)) {
                return false;
            }
        } else if (type_str == "numeric") {
            item->type = METADATA_TYPE_NUMERIC;
            if (!deserializeNumericMetadata(j["value"], &item->value.numeric)) {
                return false;
            }
        } else if (type_str == "categorical") {
            item->type = METADATA_TYPE_CATEGORICAL;
            if (!deserializeCategoricalMetadata(j["value"], &item->value.categorical)) {
                return false;
            }
        } else {
            snprintf(g_error_message, sizeof(g_error_message), 
                    "Unknown metadata type: %s", type_str.c_str());
            return false;
        }
        
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing metadata item: %s", e.what());
        return false;
    }
}

/**
 * Deserializes a custom metadata item from a JSON object
 */
static bool deserializeCustomMetadataItem(const json& j, CustomMetadataItem* item) {
    try {
        // Get name
        strncpy(item->name, j["name"].get<std::string>().c_str(), MAX_METADATA_ITEM_NAME_LENGTH - 1);
        item->name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
        
        // Get SQL query
        strncpy(item->sql_query, j["sql_query"].get<std::string>().c_str(), MAX_STRING_LENGTH - 1);
        item->sql_query[MAX_STRING_LENGTH - 1] = '\0';
        
        // Get row group and column counts
        item->row_group_count = j["row_group_count"].get<uint32_t>();
        item->column_count = j["column_count"].get<uint32_t>();
        
        // Get result matrix
        std::string matrix = j["result_matrix"].get<std::string>();
        if (!matrix.empty()) {
            item->result_matrix = strdup(matrix.c_str());
            if (!item->result_matrix) {
                snprintf(g_error_message, sizeof(g_error_message), 
                        "Failed to allocate memory for result matrix");
                return false;
            }
        } else {
            item->result_matrix = NULL;
        }
        
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing custom metadata item: %s", e.what());
        return false;
    }
}

/**
 * Deserializes file metadata from a JSON object
 */
static bool deserializeFileMetadata(const json& j, FileMetadata* metadata) {
    try {
        // Initialize with zeros
        memset(metadata, 0, sizeof(FileMetadata));
        
        // Get use_basic_metadata flag
        metadata->use_basic_metadata = j["use_basic_metadata"].get<bool>();
        
        // Parse basic metadata
        const json& basic = j["basic_metadata"];
        if (basic.size() > 0) {
            metadata->basic_metadata = (MetadataItem*)malloc(basic.size() * sizeof(MetadataItem));
            if (!metadata->basic_metadata) {
                snprintf(g_error_message, sizeof(g_error_message), 
                        "Failed to allocate memory for basic metadata");
                return false;
            }
            
            for (size_t i = 0; i < basic.size(); i++) {
                if (!deserializeMetadataItem(basic[i], &metadata->basic_metadata[i])) {
                    return false;
                }
            }
            
            metadata->basic_metadata_count = basic.size();
        }
        
        // Parse custom metadata
        const json& custom = j["custom_metadata"];
        if (custom.size() > 0) {
            metadata->custom_metadata = (CustomMetadataItem*)malloc(custom.size() * sizeof(CustomMetadataItem));
            if (!metadata->custom_metadata) {
                snprintf(g_error_message, sizeof(g_error_message), 
                        "Failed to allocate memory for custom metadata");
                return false;
            }
            
            for (size_t i = 0; i < custom.size(); i++) {
                if (!deserializeCustomMetadataItem(custom[i], &metadata->custom_metadata[i])) {
                    return false;
                }
            }
            
            metadata->custom_metadata_count = custom.size();
        }
        
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing file metadata: %s", e.what());
        return false;
    }
}

/**
 * Deserializes row group metadata from a JSON object
 */
static bool deserializeRowGroupMetadata(const json& j, RowGroupMetadata* metadata) {
    try {
        // Initialize with zeros
        memset(metadata, 0, sizeof(RowGroupMetadata));
        
        // Get row group index
        metadata->row_group_index = j["row_group_index"].get<uint32_t>();
        
        // Parse metadata items
        const json& items = j["metadata_items"];
        if (items.size() > 0) {
            metadata->metadata = (MetadataItem*)malloc(items.size() * sizeof(MetadataItem));
            if (!metadata->metadata) {
                snprintf(g_error_message, sizeof(g_error_message), 
                        "Failed to allocate memory for row group metadata items");
                return false;
            }
            
            for (size_t i = 0; i < items.size(); i++) {
                if (!deserializeMetadataItem(items[i], &metadata->metadata[i])) {
                    return false;
                }
            }
            
            metadata->metadata_count = items.size();
        }
        
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing row group metadata: %s", e.what());
        return false;
    }
}

/**
 * Deserializes column metadata from a JSON object
 */
static bool deserializeColumnMetadata(const json& j, ColumnMetadata* metadata) {
    try {
        // Initialize with zeros
        memset(metadata, 0, sizeof(ColumnMetadata));
        
        // Get column index and name
        metadata->column_index = j["column_index"].get<uint32_t>();
        strncpy(metadata->column_name, j["column_name"].get<std::string>().c_str(), 
               MAX_METADATA_ITEM_NAME_LENGTH - 1);
        metadata->column_name[MAX_METADATA_ITEM_NAME_LENGTH - 1] = '\0';
        
        // Parse metadata items
        const json& items = j["metadata_items"];
        if (items.size() > 0) {
            metadata->metadata = (MetadataItem*)malloc(items.size() * sizeof(MetadataItem));
            if (!metadata->metadata) {
                snprintf(g_error_message, sizeof(g_error_message), 
                        "Failed to allocate memory for column metadata items");
                return false;
            }
            
            for (size_t i = 0; i < items.size(); i++) {
                if (!deserializeMetadataItem(items[i], &metadata->metadata[i])) {
                    return false;
                }
            }
            
            metadata->metadata_count = items.size();
        }
        
        return true;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error parsing column metadata: %s", e.what());
        return false;
    }
}

/**
 * Deserializes metadata from a JSON string
 */
Metadata* metadataFromJson(const char* json_str) {
    if (!json_str) {
        snprintf(g_error_message, sizeof(g_error_message), "NULL JSON string provided");
        return NULL;
    }
    
    // Parse JSON
    json j;
    try {
        j = json::parse(json_str);
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Failed to parse JSON: %s", e.what());
        return NULL;
    }
    
    // Create metadata object
    Metadata* metadata = (Metadata*)malloc(sizeof(Metadata));
    if (!metadata) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Failed to allocate memory for metadata");
        return NULL;
    }
    
    // Initialize with zeros
    memset(metadata, 0, sizeof(Metadata));
    
    try {
        // Get file path
        if (j.contains("file_path") && !j["file_path"].is_null()) {
            std::string path = j["file_path"].get<std::string>();
            metadata->file_path = strdup(path.c_str());
            if (!metadata->file_path) {
                snprintf(g_error_message, sizeof(g_error_message), 
                        "Failed to allocate memory for file path");
                releaseMetadata(metadata);
                return NULL;
            }
        }
        
        // Parse file metadata
        if (!deserializeFileMetadata(j["file_metadata"], &metadata->file_metadata)) {
            releaseMetadata(metadata);
            return NULL;
        }
        
        // Parse row group metadata
        if (j.contains("row_group_metadata") && j["row_group_metadata"].is_array()) {
            const json& row_groups = j["row_group_metadata"];
            if (row_groups.size() > 0) {
                metadata->row_group_metadata = (RowGroupMetadata*)malloc(
                    row_groups.size() * sizeof(RowGroupMetadata));
                if (!metadata->row_group_metadata) {
                    snprintf(g_error_message, sizeof(g_error_message), 
                            "Failed to allocate memory for row group metadata");
                    releaseMetadata(metadata);
                    return NULL;
                }
                
                for (size_t i = 0; i < row_groups.size(); i++) {
                    if (!deserializeRowGroupMetadata(row_groups[i], &metadata->row_group_metadata[i])) {
                        releaseMetadata(metadata);
                        return NULL;
                    }
                }
                
                metadata->row_group_metadata_count = row_groups.size();
            }
        }
        
        // Parse column metadata
        if (j.contains("column_metadata") && j["column_metadata"].is_array()) {
            const json& columns = j["column_metadata"];
            if (columns.size() > 0) {
                metadata->column_metadata = (ColumnMetadata*)malloc(
                    columns.size() * sizeof(ColumnMetadata));
                if (!metadata->column_metadata) {
                    snprintf(g_error_message, sizeof(g_error_message), 
                            "Failed to allocate memory for column metadata");
                    releaseMetadata(metadata);
                    return NULL;
                }
                
                for (size_t i = 0; i < columns.size(); i++) {
                    if (!deserializeColumnMetadata(columns[i], &metadata->column_metadata[i])) {
                        releaseMetadata(metadata);
                        return NULL;
                    }
                }
                
                metadata->column_metadata_count = columns.size();
            }
        }
        
        return metadata;
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Error deserializing metadata: %s", e.what());
        releaseMetadata(metadata);
        return NULL;
    }
}

/**
 * Serializes metadata to a JSON string
 */
char* metadataToJson(const Metadata* metadata) {
    if (!metadata) {
        snprintf(g_error_message, sizeof(g_error_message), "NULL metadata pointer provided");
        return NULL;
    }
    
    // Create JSON object
    json j;
    
    // Add file path if available
    if (metadata->file_path) {
        j["file_path"] = metadata->file_path;
    } else {
        j["file_path"] = nullptr;
    }
    
    // Serialize file metadata
    serializeFileMetadata(j["file_metadata"], &metadata->file_metadata);
    
    // Serialize row group metadata
    json row_groups = json::array();
    for (uint32_t i = 0; i < metadata->row_group_metadata_count; i++) {
        json row_group;
        serializeRowGroupMetadata(row_group, &metadata->row_group_metadata[i]);
        row_groups.push_back(row_group);
    }
    j["row_group_metadata"] = row_groups;
    
    // Serialize column metadata
    json columns = json::array();
    for (uint32_t i = 0; i < metadata->column_metadata_count; i++) {
        json column;
        serializeColumnMetadata(column, &metadata->column_metadata[i]);
        columns.push_back(column);
    }
    j["column_metadata"] = columns;
    
    // Serialize to string
    std::string json_str = j.dump(2);  // Pretty print with indent of 2
    
    // Create a copy to return
    char* result = strdup(json_str.c_str());
    if (!result) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Failed to allocate memory for JSON string");
        return NULL;
    }
    
    return result;
} 