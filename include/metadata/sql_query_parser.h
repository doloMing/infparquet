/**
 * sql_query_parser.h
 * 
 * A simple SQL query parser for filtering InfParquet metadata.
 */

#ifndef SQL_QUERY_PARSER_H
#define SQL_QUERY_PARSER_H

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Comparison operators for SQL conditions
 */
typedef enum {
    SQL_COMP_UNKNOWN = 0,
    SQL_COMP_EQUAL,        /* = */
    SQL_COMP_NOT_EQUAL,    /* != or <> */
    SQL_COMP_GREATER_THAN,      /* > */
    SQL_COMP_LESS_THAN,         /* < */
    SQL_COMP_GREATER_EQUAL, /* >= */
    SQL_COMP_LESS_EQUAL,   /* <= */
    SQL_COMP_LIKE,         /* LIKE */
    SQL_COMP_NOT_LIKE      /* NOT LIKE */
} SQLComparisonOperator;

/**
 * Logical operators for combining multiple conditions
 */
typedef enum {
    SQL_LOGICAL_NONE = 0,
    SQL_LOGICAL_AND,
    SQL_LOGICAL_OR
} SQLLogicalOperator;

/**
 * Structure for a single SQL condition
 * For example in "column_name = 'value' AND other_column > 10",
 * we have two conditions.
 */
typedef struct {
    char* column;                 /* Column name in the condition */
    char* value;                  /* Value to compare against */
    SQLComparisonOperator comp_op; /* Comparison operator */
    SQLLogicalOperator logical_op; /* Logical operator that connects to the next condition */
} SQLCondition;

/**
 * Structure to represent a row of data
 * A row consists of multiple key-value pairs
 */
typedef struct {
    char** columns;              /* Array of column names */
    char** values;               /* Array of values (as strings) */
    int count;                   /* Number of columns/values in the row */
} MetadataRow;

/**
 * Structure to represent a result set from a query
 * A result set consists of multiple rows
 */
typedef struct {
    MetadataRow* rows;           /* Array of rows */
    int row_count;               /* Number of rows in the result */
    int column_count;            /* Number of columns in each row */
    char** column_names;         /* Names of the columns in the result set */
} SQLResultSet;

/**
 * Structure for holding the parsed SQL query information
 */
typedef struct {
    char** select_columns;       /* Array of selected column names */
    size_t select_column_count;  /* Number of selected columns */
    bool select_all;             /* Whether SELECT * was used */
    char* from_table;            /* Table name in FROM clause */
    SQLCondition* conditions;    /* Array of WHERE conditions */
    size_t condition_count;      /* Number of conditions */
} SQLQueryInfo;

/**
 * Container for metadata that can be queried
 */
typedef struct {
    char** keys;                 /* Array of keys (column names) */
    char** values;               /* Array of values */
    int count;                   /* Number of key-value pairs */
} MetadataContainer;

/**
 * Container for a collection of metadata items
 */
typedef struct {
    MetadataContainer* items;    /* Array of metadata containers */
    int count;                   /* Number of metadata items */
} MetadataCollection;

/**
 * Parses a SQL query string into the SQLQueryInfo structure.
 * Only basic queries with SELECT, FROM, and WHERE clauses are supported.
 * 
 * @param query_str The SQL query string to parse
 * @param query_info Pointer to SQLQueryInfo structure to store parsing results
 * @return true if parsing was successful, false otherwise
 */
bool parse_sql_query(const char* query_str, SQLQueryInfo* query_info);

/**
 * Parses WHERE conditions part of a SQL query.
 * Supports basic conditions with AND and OR operators.
 * 
 * @param where_str String containing WHERE conditions
 * @param query_info Pointer to SQLQueryInfo structure to store parsing results
 * @return true if parsing was successful, false otherwise
 */
bool parse_where_conditions(const char* where_str, SQLQueryInfo* query_info);

/**
 * Parses a single SQL condition like "column_name = 'value'".
 * 
 * @param condition_str String containing a single condition
 * @param condition Pointer to SQLCondition structure to store parsing results
 * @return true if parsing was successful, false otherwise
 */
bool parse_single_condition(const char* condition_str, SQLCondition* condition);

/**
 * Executes a SQL query against a metadata collection
 * 
 * @param query_info The parsed SQL query information
 * @param metadata The metadata collection to query
 * @param result_set Pointer to store the query results
 * @return true if the query was executed successfully, false otherwise
 */
bool execute_sql_query(const SQLQueryInfo* query_info, 
                      const MetadataCollection* metadata, 
                      SQLResultSet* result_set);

/**
 * Frees all resources allocated in the SQLQueryInfo structure.
 * 
 * @param query_info Pointer to SQLQueryInfo structure to free
 */
void free_sql_query_info(SQLQueryInfo* query_info);

/**
 * Frees all resources allocated in the SQLResultSet structure.
 * 
 * @param result_set Pointer to SQLResultSet structure to free
 */
void free_sql_result_set(SQLResultSet* result_set);

/**
 * Returns the last error message from the SQL parser.
 * 
 * @return Error message string or NULL if no error occurred
 */
const char* get_sql_error_message();

#ifdef __cplusplus
}
#endif

#endif /* SQL_QUERY_PARSER_H */ 