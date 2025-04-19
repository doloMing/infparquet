/**
 * parallel_processor.h
 * 
 * This header file defines the interface for parallel processing of compression
 * and decompression tasks. It provides functions to distribute work across multiple
 * threads for improved performance.
 */

#ifndef INFPARQUET_PARALLEL_PROCESSOR_H
#define INFPARQUET_PARALLEL_PROCESSOR_H

#include <stdint.h>
#include <stdbool.h>
#include "../core/parquet_structure.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Error codes for parallel processor functions
 */
typedef enum {
    PARALLEL_PROCESSOR_OK = 0,
    PARALLEL_PROCESSOR_INVALID_PARAMETER,
    PARALLEL_PROCESSOR_MEMORY_ERROR,
    PARALLEL_PROCESSOR_THREAD_ERROR,
    PARALLEL_PROCESSOR_TASK_ERROR
} ParallelProcessorError;

/**
 * Function type for parallel tasks that process row groups
 * 
 * task_data: Task-specific data
 * result: Pointer to store the result of the task
 * 
 * returns: Error code (0 on success)
 */
typedef int (*ParallelTaskFunction)(void* task_data, void** result);

/**
 * Function type for cleaning up task resources
 * 
 * task_data: Task-specific data
 * result: Result of the task
 */
typedef void (*ParallelTaskCleanupFunction)(void* task_data, void* result);

/**
 * Callback function type for processing a work item
 * 
 * item_index: Index of the work item being processed
 * total_items: Total number of work items
 * user_data: User-provided data passed to the processor function
 * 
 * Return: 0 on success, non-zero error code on failure
 */
typedef int (*WorkItemProcessor)(uint32_t item_index, uint32_t total_items, void* user_data);

/**
 * Callback function type for reporting processing progress
 * 
 * item_index: Index of the work item being processed
 * total_items: Total number of work items
 * percent_complete: Percentage of the current item that's complete (0-100)
 * user_data: User-provided data passed to the processor function
 * 
 * Return: true to continue, false to abort
 */
typedef bool (*ProcessingProgressCallback)(uint32_t item_index, uint32_t total_items, 
                                          int percent_complete, void* user_data);

/**
 * Execute a task in parallel for each row group in a parquet file
 * 
 * This function processes the row groups of a parquet file in parallel,
 * executing the specified task function for each row group.
 * 
 * file: Structure of the parquet file to process
 * task_function: Function to execute for each row group
 * task_data: Array of task-specific data, one element per row group
 * cleanup_function: Function to clean up task resources (can be NULL)
 * task_results: Pointer to array to store task results (allocated by the function)
 * returns: Error code (PARALLEL_PROCESSOR_OK on success)
 */
ParallelProcessorError parallel_processor_process_row_groups(
    const ParquetFile* file,
    ParallelTaskFunction task_function,
    void** task_data,
    ParallelTaskCleanupFunction cleanup_function,
    void*** task_results
);

/**
 * Free resources allocated by parallel_processor_process_row_groups
 * 
 * This function releases memory allocated for task results.
 * 
 * task_results: Array of task results to free
 * count: Number of elements in the array
 */
void parallel_processor_free_results(void** task_results, int count);

/**
 * Get the last error message from the parallel processor
 * 
 * This function returns a string describing the last error that occurred.
 * The returned string is valid until the next call to any parallel processor function.
 * 
 * returns: A string describing the last error, or NULL if no error occurred
 */
const char* parallel_processor_get_error(void);

/**
 * Maximum number of parallel tasks to run simultaneously
 * 
 * This function returns the maximum number of tasks that can run in parallel.
 * This is typically based on the number of available CPU cores.
 * 
 * returns: Maximum number of parallel tasks
 */
int parallel_processor_get_max_tasks(void);

/**
 * Set the maximum number of parallel tasks to run simultaneously
 * 
 * This function sets the maximum number of tasks that can run in parallel.
 * If max_tasks is 0, the function will use the number of available CPU cores.
 * 
 * max_tasks: Maximum number of parallel tasks (0 for auto-detection)
 * returns: The actual number of parallel tasks set
 */
int parallel_processor_set_max_tasks(int max_tasks);

/**
 * Processes multiple work items in parallel
 * 
 * This function distributes work items across multiple threads for parallel processing.
 * Each work item is processed by calling the provided processor function.
 * 
 * processor: Function to process each work item
 * num_items: Number of items to process
 * max_threads: Maximum number of threads to use (0 for automatic)
 * progress_callback: Callback function for reporting progress (can be NULL)
 * user_data: User data to pass to the processor and progress callback functions
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int parallel_process_items(WorkItemProcessor processor, 
                          uint32_t num_items,
                          uint32_t max_threads,
                          ProcessingProgressCallback progress_callback,
                          void* user_data);

/**
 * Gets the optimal number of threads for the current system
 * 
 * This function returns the recommended number of threads to use for parallel processing
 * based on the number of available CPU cores.
 * 
 * Return: Recommended number of threads
 */
uint32_t parallel_get_optimal_threads();

/**
 * Sets the priority of the worker threads
 * 
 * This function sets the priority level for the worker threads created by
 * the parallel processor.
 * 
 * priority: Thread priority (0 = lowest, 5 = normal, 10 = highest)
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int parallel_set_thread_priority(int priority);

/**
 * Structure for parallel processing configuration
 */
typedef struct {
    uint32_t max_threads;              /* Maximum number of threads to use */
    uint32_t min_items_per_thread;     /* Minimum number of items per thread */
    uint32_t thread_stack_size;        /* Stack size for each thread in bytes */
    bool preserve_item_order;          /* Whether to preserve the order of items */
} ParallelProcessorConfig;

/**
 * Sets the configuration for the parallel processor
 * 
 * This function configures the behavior of the parallel processor.
 * 
 * config: Pointer to the configuration structure
 * 
 * Return: 0 on success, non-zero error code on failure
 */
int parallel_set_config(const ParallelProcessorConfig* config);

/**
 * Gets the default configuration for the parallel processor
 * 
 * This function fills the provided configuration structure with default values.
 * 
 * config: Pointer to the configuration structure to be filled
 */
void parallel_get_default_config(ParallelProcessorConfig* config);

#ifdef __cplusplus
}
#endif

#endif /* INFPARQUET_PARALLEL_PROCESSOR_H */ 