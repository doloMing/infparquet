/**
 * sql_query_parser.cpp
 * 
 * This file implements a simple SQL query parser for metadata queries.
 * It supports basic SELECT queries with WHERE clauses for filtering metadata.
 */

#include "metadata/sql_query_parser.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <algorithm>
#include <iostream>
#include <cctype>
#include <cstring>
#include <cstdarg>

#ifdef _WIN32
#define strcasecmp _stricmp
#else
#include <strings.h>
#endif

// Static error message buffer
static char s_error_message[1024] = {0};

// Set the error message
static void set_error(const char* format, ...) {
    va_list args;
    va_start(args, format);
    vsnprintf(s_error_message, sizeof(s_error_message), format, args);
    va_end(args);
}

// Helper function to trim whitespace from a string
static std::string trim(const std::string& str) {
    const auto begin = str.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return "";
    }
    
    const auto end = str.find_last_not_of(" \t\r\n");
    return str.substr(begin, end - begin + 1);
}

// Helper function to convert string to lowercase
static std::string tolower_str(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

// Helper function to tokenize a string by a delimiter
static std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(trim(token));
    }
    
    return tokens;
}

// Split conditions by a logical operator
static std::vector<std::string> split_conditions(const std::string& conditions, const std::string& op) {
    std::vector<std::string> result;
    std::string lower_op = tolower_str(op);
    
    // Handle quoted strings and parentheses
    size_t pos = 0;
    size_t start = 0;
    bool in_quotes = false;
    int paren_level = 0;
    
    while (pos < conditions.length()) {
        char c = conditions[pos];
        
        // Handle quotes
        if (c == '\'' || c == '"') {
            in_quotes = !in_quotes;
            pos++;
            continue;
        }
        
        // Handle parentheses
        if (c == '(') {
            paren_level++;
            pos++;
            continue;
        }
        
        if (c == ')') {
            paren_level--;
            pos++;
            continue;
        }
        
        // Skip if inside quotes or parentheses
        if (in_quotes || paren_level > 0) {
            pos++;
            continue;
        }
        
        // Check for operator
        if (pos + lower_op.length() <= conditions.length() && 
            tolower_str(conditions.substr(pos, lower_op.length())) == lower_op) {
            // Found operator
            result.push_back(trim(conditions.substr(start, pos - start)));
            pos += lower_op.length();
            start = pos;
        } else {
            pos++;
        }
    }
    
    // Add the last part
    if (start < conditions.length()) {
        result.push_back(trim(conditions.substr(start)));
    }
    
    return result;
}

// Parse a single condition
bool parse_single_condition(const char* condition_str, SQLCondition* condition) {
    if (!condition_str || !condition) {
        set_error("Invalid parameters for condition");
        return false;
    }
    
    // Convert to std::string for easier handling
    std::string cond = trim(condition_str);
    
    // Look for comparison operators
    static const struct {
        const char* op_str;
        SQLComparisonOperator op;
    } operators[] = {
        {"=", SQL_COMP_EQUAL},
        {"<>", SQL_COMP_NOT_EQUAL},
        {"!=", SQL_COMP_NOT_EQUAL},
        {"<", SQL_COMP_LESS_THAN},
        {"<=", SQL_COMP_LESS_EQUAL},
        {">", SQL_COMP_GREATER_THAN},
        {">=", SQL_COMP_GREATER_EQUAL},
        {"like", SQL_COMP_LIKE},
        {"not like", SQL_COMP_NOT_LIKE}
    };
    
    // Find the operator
    size_t op_pos = std::string::npos;
    size_t op_len = 0;
    SQLComparisonOperator op = SQL_COMP_EQUAL;
    
    for (const auto& op_info : operators) {
        // Special case for LIKE and NOT LIKE (case insensitive)
        if (strcasecmp(op_info.op_str, "like") == 0 || strcasecmp(op_info.op_str, "not like") == 0) {
            size_t pos = cond.find(tolower_str(op_info.op_str));
            if (pos != std::string::npos) {
                op_pos = pos;
                op_len = strlen(op_info.op_str);
                op = op_info.op;
                break;
            }
        } else {
            // Regular operators
            size_t pos = cond.find(op_info.op_str);
            if (pos != std::string::npos) {
                op_pos = pos;
                op_len = strlen(op_info.op_str);
                op = op_info.op;
                break;
            }
        }
    }
    
    if (op_pos == std::string::npos) {
        set_error("No comparison operator found in condition: %s", condition_str);
        return false;
    }
    
    // Extract column name and value
    std::string column = trim(cond.substr(0, op_pos));
    std::string value = trim(cond.substr(op_pos + op_len));
    
    // Check if value is quoted
    if ((value.front() == '\'' && value.back() == '\'') || 
        (value.front() == '"' && value.back() == '"')) {
        value = value.substr(1, value.length() - 2);
    }
    
    // Set condition fields
    condition->column = strdup(column.c_str());
    condition->value = strdup(value.c_str());
    condition->comp_op = op;
    
    if (!condition->column || !condition->value) {
        // Clean up if allocation failed
        if (condition->column) {
            free(condition->column);
            condition->column = nullptr;
        }
        
        if (condition->value) {
            free(condition->value);
            condition->value = nullptr;
        }
        
        set_error("Failed to allocate memory for condition parts");
        return false;
    }
    
    return true;
}

// Parse WHERE conditions
bool parse_where_conditions(const char* conditions, SQLQueryInfo* query_info) {
    if (!conditions || !query_info) {
        set_error("Invalid parameters for WHERE conditions");
        return false;
    }
    
    // Convert to std::string for easier handling
    std::string where_str = trim(conditions);
    
    // Split by AND
    std::vector<std::string> and_conditions = split_conditions(where_str, " and ");
    
    // Count total conditions
    size_t total_conditions = 0;
    for (const auto& and_condition : and_conditions) {
        // Split by OR
        std::vector<std::string> or_conditions = split_conditions(and_condition, " or ");
        total_conditions += or_conditions.size();
    }
    
    // Allocate memory for conditions
    query_info->conditions = (SQLCondition*)malloc(total_conditions * sizeof(SQLCondition));
    if (!query_info->conditions) {
        set_error("Failed to allocate memory for conditions");
        return false;
    }
    
    // Initialize conditions
    memset(query_info->conditions, 0, total_conditions * sizeof(SQLCondition));
    
    // Parse each condition
    size_t condition_index = 0;
    
    for (size_t i = 0; i < and_conditions.size(); i++) {
        // Split by OR
        std::vector<std::string> or_conditions = split_conditions(and_conditions[i], " or ");
        
        // Set logical operator for all but the first group
        SQLLogicalOperator logical_op = (i == 0) ? SQL_LOGICAL_NONE : SQL_LOGICAL_AND;
        
        for (size_t j = 0; j < or_conditions.size(); j++) {
            SQLCondition* condition = &query_info->conditions[condition_index++];
            
            // Set logical operator (within an AND group)
            condition->logical_op = (j == 0) ? logical_op : SQL_LOGICAL_OR;
            
            // Parse the condition
            if (!parse_single_condition(or_conditions[j].c_str(), condition)) {
                // Error message already set by parse_single_condition
                
                // Clean up allocated memory
                free(query_info->conditions);
                query_info->conditions = nullptr;
                
                return false;
            }
        }
    }
    
    query_info->condition_count = total_conditions;
    return true;
}

// Parse SQL query into structured components
bool parse_sql_query(const char* query, SQLQueryInfo* query_info) {
    if (!query || !query_info) {
        set_error("Invalid parameters");
        return false;
    }
    
    // Initialize query info
    memset(query_info, 0, sizeof(SQLQueryInfo));
    
    // Convert to std::string for easier handling
    std::string sql = trim(query);
    
    // Check if it's a SELECT query
    if (tolower_str(sql.substr(0, 6)) != "select") {
        set_error("Query must start with SELECT");
        return false;
    }
    
    // Extract the parts of the query
    size_t from_pos = sql.find(" from ", 6);
    if (from_pos == std::string::npos) {
        set_error("Missing FROM clause");
        return false;
    }
    
    // Extract SELECT part
    std::string select_part = sql.substr(6, from_pos - 6);
    select_part = trim(select_part);
    
    // Parse selected columns
    std::vector<std::string> columns;
    if (select_part == "*") {
        // Select all columns
        query_info->select_all = true;
    } else {
        // Split by commas
        columns = split(select_part, ',');
        if (columns.empty()) {
            set_error("No columns specified in SELECT");
            return false;
        }
        
        // Copy columns to query info
        query_info->select_columns = (char**)malloc(columns.size() * sizeof(char*));
        if (!query_info->select_columns) {
            set_error("Failed to allocate memory for columns");
            return false;
        }
        
        for (size_t i = 0; i < columns.size(); i++) {
            query_info->select_columns[i] = strdup(columns[i].c_str());
            if (!query_info->select_columns[i]) {
                // Clean up previously allocated memory
                for (size_t j = 0; j < i; j++) {
                    free(query_info->select_columns[j]);
                }
                free(query_info->select_columns);
                query_info->select_columns = nullptr;
                
                set_error("Failed to allocate memory for column name");
                return false;
            }
        }
        
        query_info->select_column_count = columns.size();
    }
    
    // Extract FROM part
    size_t where_pos = sql.find(" where ", from_pos + 6);
    std::string from_part;
    
    if (where_pos == std::string::npos) {
        // No WHERE clause
        from_part = sql.substr(from_pos + 6);
    } else {
        from_part = sql.substr(from_pos + 6, where_pos - (from_pos + 6));
    }
    
    from_part = trim(from_part);
    query_info->from_table = strdup(from_part.c_str());
    
    // Extract WHERE part if present
    if (where_pos != std::string::npos) {
        std::string where_part = sql.substr(where_pos + 7);
        where_part = trim(where_part);
        
        // Parse WHERE conditions
        bool success = parse_where_conditions(where_part.c_str(), query_info);
        if (!success) {
            // Error message already set by parse_where_conditions
            
            // Clean up allocated memory
            if (query_info->select_columns) {
                for (size_t i = 0; i < query_info->select_column_count; i++) {
                    free(query_info->select_columns[i]);
                }
                free(query_info->select_columns);
                query_info->select_columns = nullptr;
            }
            
            if (query_info->from_table) {
                free(query_info->from_table);
                query_info->from_table = nullptr;
            }
            
            return false;
        }
    }
    
    return true;
}

// Helper function to match wildcards for LIKE operator
static bool simple_pattern_match(const char* value, const char* pattern) {
    if (!value || !pattern) return false;
    
    // Simple wildcard matching algorithm
    // '%' matches any sequence of characters
    // '_' matches any single character
    
    const char* v = value;
    const char* p = pattern;
    
    const char* v_backup = nullptr;
    const char* p_backup = nullptr;
    
    while (*v) {
        if (*p == '%') {
            // Remember the position for backtracking
            p_backup = ++p;
            v_backup = v;
        } else if (*p == '_' || *p == *v) {
            // Character matches
            p++;
            v++;
        } else if (p_backup) {
            // Mismatch, but we have a backup from a '%'
            p = p_backup;
            v = ++v_backup;
        } else {
            // Mismatch and no backup
            return false;
        }
    }
    
    // Skip any trailing '%'
    while (*p == '%') p++;
    
    // Match succeeds if we've consumed the entire pattern
    return *p == '\0';
}

// Evaluate a condition against a metadata item
static bool evaluate_condition(const SQLCondition* condition, const MetadataContainer* metadata) {
    if (!condition || !metadata) return false;
    
    // Find the column in the metadata
    int column_index = -1;
    for (int i = 0; i < metadata->count; i++) {
        if (strcmp(metadata->keys[i], condition->column) == 0) {
            column_index = i;
            break;
        }
    }
    
    // If column doesn't exist in metadata, condition fails
    if (column_index == -1) return false;
    
    // Get the value from metadata
    const char* metadata_value = metadata->values[column_index];
    if (!metadata_value) return false;
    
    // Compare values based on comparison operator
    switch (condition->comp_op) {
        case SQL_COMP_EQUAL:
            return strcmp(metadata_value, condition->value) == 0;
            
        case SQL_COMP_NOT_EQUAL:
            return strcmp(metadata_value, condition->value) != 0;
            
        case SQL_COMP_GREATER_THAN:
            // Attempt numeric comparison if possible
            if (isdigit(metadata_value[0]) || 
                (metadata_value[0] == '-' && isdigit(metadata_value[1]))) {
                return atof(metadata_value) > atof(condition->value);
            }
            return strcmp(metadata_value, condition->value) > 0;
            
        case SQL_COMP_LESS_THAN:
            // Attempt numeric comparison if possible
            if (isdigit(metadata_value[0]) || 
                (metadata_value[0] == '-' && isdigit(metadata_value[1]))) {
                return atof(metadata_value) < atof(condition->value);
            }
            return strcmp(metadata_value, condition->value) < 0;
            
        case SQL_COMP_GREATER_EQUAL:
            // Attempt numeric comparison if possible
            if (isdigit(metadata_value[0]) || 
                (metadata_value[0] == '-' && isdigit(metadata_value[1]))) {
                return atof(metadata_value) >= atof(condition->value);
            }
            return strcmp(metadata_value, condition->value) >= 0;
            
        case SQL_COMP_LESS_EQUAL:
            // Attempt numeric comparison if possible
            if (isdigit(metadata_value[0]) || 
                (metadata_value[0] == '-' && isdigit(metadata_value[1]))) {
                return atof(metadata_value) <= atof(condition->value);
            }
            return strcmp(metadata_value, condition->value) <= 0;
            
        case SQL_COMP_LIKE:
            return simple_pattern_match(metadata_value, condition->value);
            
        case SQL_COMP_NOT_LIKE:
            return !simple_pattern_match(metadata_value, condition->value);
            
        default:
            return false;
    }
}

// Evaluate all conditions on a metadata item
static bool evaluate_conditions(const SQLQueryInfo* query_info, const MetadataContainer* metadata) {
    if (!query_info || !metadata || !query_info->conditions) {
        return true;  // No conditions, always matches
    }
    
    bool result = true;
    
    for (size_t i = 0; i < query_info->condition_count; i++) {
        const SQLCondition* condition = &query_info->conditions[i];
        
        if (i == 0 || condition->logical_op == SQL_LOGICAL_NONE) {
            // First condition, just evaluate it
            result = evaluate_condition(condition, metadata);
        } else if (condition->logical_op == SQL_LOGICAL_AND) {
            // AND with previous result
            result = result && evaluate_condition(condition, metadata);
        } else if (condition->logical_op == SQL_LOGICAL_OR) {
            // OR with previous result
            result = result || evaluate_condition(condition, metadata);
        }
    }
    
    return result;
}

// Execute a SQL query against metadata
bool execute_sql_query(const SQLQueryInfo* query_info, 
                      const MetadataCollection* metadata, 
                      SQLResultSet* result_set) {
    if (!query_info || !metadata || !result_set) {
        set_error("Invalid parameters for query execution");
        return false;
    }
    
    // Initialize result set
    memset(result_set, 0, sizeof(SQLResultSet));
    
    // Count matching metadata items
    int matching_count = 0;
    for (int i = 0; i < metadata->count; i++) {
        if (evaluate_conditions(query_info, &metadata->items[i])) {
            matching_count++;
        }
    }
    
    if (matching_count == 0) {
        // No matches, return empty result set
        return true;
    }
    
    // Determine columns for result set
    std::vector<std::string> result_columns;
    
    if (query_info->select_all) {
        // Use all columns from the first matching item
        for (int i = 0; i < metadata->count; i++) {
            if (evaluate_conditions(query_info, &metadata->items[i])) {
                MetadataContainer* item = &metadata->items[i];
                
                for (int j = 0; j < item->count; j++) {
                    result_columns.push_back(item->keys[j]);
                }
                
                break;  // Just use the first matching item for columns
            }
        }
    } else {
        // Use specified columns
        for (size_t i = 0; i < query_info->select_column_count; i++) {
            result_columns.push_back(query_info->select_columns[i]);
        }
    }
    
    // Allocate memory for result set
    result_set->column_count = result_columns.size();
    result_set->row_count = matching_count;
    
    // Allocate column names
    result_set->column_names = (char**)malloc(result_set->column_count * sizeof(char*));
    if (!result_set->column_names) {
        set_error("Failed to allocate memory for result set columns");
        return false;
    }
    
    // Copy column names
    for (int i = 0; i < result_set->column_count; i++) {
        result_set->column_names[i] = strdup(result_columns[i].c_str());
        if (!result_set->column_names[i]) {
            set_error("Failed to allocate memory for column name");
            
            // Clean up
            for (int j = 0; j < i; j++) {
                free(result_set->column_names[j]);
            }
            free(result_set->column_names);
            result_set->column_names = nullptr;
            
            return false;
        }
    }
    
    // Allocate rows
    result_set->rows = (MetadataRow*)malloc(result_set->row_count * sizeof(MetadataRow));
    if (!result_set->rows) {
        set_error("Failed to allocate memory for result rows");
        
        // Clean up
        for (int i = 0; i < result_set->column_count; i++) {
            free(result_set->column_names[i]);
        }
        free(result_set->column_names);
        result_set->column_names = nullptr;
        
        return false;
    }
    
    // Initialize rows
    memset(result_set->rows, 0, result_set->row_count * sizeof(MetadataRow));
    
    // Populate rows
    int row_index = 0;
    
    for (int i = 0; i < metadata->count; i++) {
        MetadataContainer* item = &metadata->items[i];
        
        if (evaluate_conditions(query_info, item)) {
            MetadataRow* row = &result_set->rows[row_index++];
            
            // Set row properties
            row->count = result_set->column_count;
            
            // Allocate columns and values
            row->columns = (char**)malloc(row->count * sizeof(char*));
            row->values = (char**)malloc(row->count * sizeof(char*));
            
            if (!row->columns || !row->values) {
                set_error("Failed to allocate memory for row data");
                
                // Clean up
                if (row->columns) free(row->columns);
                if (row->values) free(row->values);
                
                // Clean up previous rows
                for (int j = 0; j < row_index - 1; j++) {
                    MetadataRow* prev_row = &result_set->rows[j];
                    
                    for (int k = 0; k < prev_row->count; k++) {
                        free(prev_row->columns[k]);
                        free(prev_row->values[k]);
                    }
                    
                    free(prev_row->columns);
                    free(prev_row->values);
                }
                
                // Clean up result set
                free(result_set->rows);
                
                for (int j = 0; j < result_set->column_count; j++) {
                    free(result_set->column_names[j]);
                }
                free(result_set->column_names);
                
                return false;
            }
            
            // Initialize arrays
            memset(row->columns, 0, row->count * sizeof(char*));
            memset(row->values, 0, row->count * sizeof(char*));
            
            // Populate columns and values
            for (int j = 0; j < row->count; j++) {
                // Copy column name
                row->columns[j] = strdup(result_columns[j].c_str());
                
                // Find value in metadata item
                const char* value = "";
                for (int k = 0; k < item->count; k++) {
                    if (strcmp(item->keys[k], result_columns[j].c_str()) == 0) {
                        value = item->values[k];
                        break;
                    }
                }
                
                // Copy value
                row->values[j] = strdup(value);
                
                if (!row->columns[j] || !row->values[j]) {
                    set_error("Failed to allocate memory for row data");
                    
                    // Clean up
                    for (int k = 0; k <= j; k++) {
                        if (row->columns[k]) free(row->columns[k]);
                        if (row->values[k]) free(row->values[k]);
                    }
                    
                    free(row->columns);
                    free(row->values);
                    
                    // Clean up previous rows
                    for (int k = 0; k < row_index - 1; k++) {
                        MetadataRow* prev_row = &result_set->rows[k];
                        
                        for (int l = 0; l < prev_row->count; l++) {
                            free(prev_row->columns[l]);
                            free(prev_row->values[l]);
                        }
                        
                        free(prev_row->columns);
                        free(prev_row->values);
                    }
                    
                    // Clean up result set
                    free(result_set->rows);
                    
                    for (int k = 0; k < result_set->column_count; k++) {
                        free(result_set->column_names[k]);
                    }
                    free(result_set->column_names);
                    
                    return false;
                }
            }
        }
    }
    
    return true;
}

// Free resources associated with a result set
void free_sql_result_set(SQLResultSet* result_set) {
    if (!result_set) {
        return;
    }
    
    // Free rows
    if (result_set->rows) {
        for (int i = 0; i < result_set->row_count; i++) {
            MetadataRow* row = &result_set->rows[i];
            
            if (row->columns) {
                for (int j = 0; j < row->count; j++) {
                    if (row->columns[j]) {
                        free(row->columns[j]);
                    }
                }
                free(row->columns);
            }
            
            if (row->values) {
                for (int j = 0; j < row->count; j++) {
                    if (row->values[j]) {
                        free(row->values[j]);
                    }
                }
                free(row->values);
            }
        }
        
        free(result_set->rows);
        result_set->rows = nullptr;
    }
    
    // Free column names
    if (result_set->column_names) {
        for (int i = 0; i < result_set->column_count; i++) {
            free(result_set->column_names[i]);
        }
        free(result_set->column_names);
        result_set->column_names = nullptr;
    }
    
    // Reset counts
    result_set->row_count = 0;
    result_set->column_count = 0;
}

// Free query info resources
void free_sql_query_info(SQLQueryInfo* query_info) {
    if (!query_info) {
        return;
    }
    
    // Free selected columns
    if (query_info->select_columns) {
        for (size_t i = 0; i < query_info->select_column_count; i++) {
            free(query_info->select_columns[i]);
        }
        free(query_info->select_columns);
        query_info->select_columns = nullptr;
    }
    
    // Free from table
    if (query_info->from_table) {
        free(query_info->from_table);
        query_info->from_table = nullptr;
    }
    
    // Free conditions
    if (query_info->conditions) {
        for (size_t i = 0; i < query_info->condition_count; i++) {
            SQLCondition* condition = &query_info->conditions[i];
            
            if (condition->column) {
                free(condition->column);
            }
            
            if (condition->value) {
                free(condition->value);
            }
        }
        
        free(query_info->conditions);
        query_info->conditions = nullptr;
    }
    
    // Reset counts
    query_info->select_column_count = 0;
    query_info->select_all = false;
    query_info->condition_count = 0;
}

// Get the last error message
const char* get_sql_error_message() {
    return s_error_message[0] != '\0' ? s_error_message : nullptr;
} 