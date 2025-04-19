/**
 * parallel_processor.cpp
 * 
 * This file implements the functions declared in parallel_processor.h for
 * parallel processing of compression and decompression tasks, using the C++
 * standard thread library instead of pthreads.
 */

#include "compression/parallel_processor.h"
#include <thread>
#include <vector>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <cstdio>
#include <memory>

/* Static error message buffer */
static char g_error_message[256] = {0};

/* Static configuration */
static ParallelProcessorConfig g_config = {
    0,              /* max_threads: 0 means auto-detect */
    1,              /* min_items_per_thread: Default to at least 1 item per thread */
    0,              /* thread_stack_size: 0 means use system default */
    true            /* preserve_item_order: Default to preserving order */
};

/* Helper structure for thread work */
struct ThreadWork {
    uint32_t thread_id;
    uint32_t num_threads;
    uint32_t items_per_thread;
    uint32_t start_item;
    uint32_t end_item;
    uint32_t total_items;
    WorkItemProcessor processor;
    ProcessingProgressCallback progress_callback;
    void* user_data;
    int result;
};

/* Structure for row group task */
struct RowGroupTask {
    uint32_t thread_id;
    const ParquetFile* file;
    uint32_t row_group_index;
    ParallelTaskFunction task_function;
    void* task_data;
    void* task_result;
    int result;
};

/**
 * Determine the number of available CPU cores
 */
static uint32_t get_cpu_cores() {
    unsigned int num_cores = std::thread::hardware_concurrency();
    return (num_cores > 0) ? num_cores : 1; // Default to 1 if detection fails
}

/**
 * Worker thread function for processing items
 */
static void process_items_thread(ThreadWork* work) {
    if (!work) return;
    
    uint32_t item;
    int result = 0;
    
    for (item = work->start_item; item < work->end_item && result == 0; item++) {
        /* Call processor function for this item */
        result = work->processor(item, work->total_items, work->user_data);
        
        /* Report progress if callback is provided */
        if (work->progress_callback) {
            /* Calculate progress as a percentage: current item / total items */
            int percent = (int)(((item - work->start_item + 1) * 100) / 
                              (work->end_item - work->start_item));
            
            if (!work->progress_callback(item, work->total_items, percent, work->user_data)) {
                /* User requested abort */
                result = PARALLEL_PROCESSOR_TASK_ERROR;
                break;
            }
        }
    }
    
    work->result = result;
}

/**
 * Worker thread function for processing row groups
 */
static void process_row_group_thread(RowGroupTask* task) {
    if (!task) return;
    
    /* Execute the task for this row group */
    task->result = task->task_function(task->task_data, &task->task_result);
}

/**
 * Gets the optimal number of threads for the current system
 */
uint32_t parallel_get_optimal_threads() {
    uint32_t cores = get_cpu_cores();
    
    /* Default to number of cores, with a minimum of 1 and maximum of 32 */
    return (cores > 0) ? (cores > 32 ? 32 : cores) : 1;
}

/**
 * Set the maximum number of parallel tasks to run simultaneously
 */
int parallel_processor_set_max_tasks(int max_tasks) {
    if (max_tasks < 0) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Invalid maximum tasks value: %d", max_tasks);
        return -1;
    }
    
    if (max_tasks == 0) {
        /* Auto-detect */
        g_config.max_threads = parallel_get_optimal_threads();
    } else {
        g_config.max_threads = max_tasks;
    }
    
    return g_config.max_threads;
}

/**
 * Maximum number of parallel tasks to run simultaneously
 */
int parallel_processor_get_max_tasks(void) {
    if (g_config.max_threads == 0) {
        /* Auto-detect */
        return parallel_get_optimal_threads();
    }
    
    return g_config.max_threads;
}

/**
 * Sets the priority of the worker threads
 */
int parallel_set_thread_priority(int priority) {
    /* Thread priority is not directly supported in C++ std::thread */
    /* But we keep the function for API compatibility */
    if (priority < 0 || priority > 10) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Invalid thread priority: %d", priority);
        return -1;
    }
    
    return 0;
}

/**
 * Sets the configuration for the parallel processor
 */
int parallel_set_config(const ParallelProcessorConfig* config) {
    if (!config) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Invalid configuration pointer");
        return -1;
    }
    
    g_config = *config;
    
    /* Validate configuration */
    if (g_config.min_items_per_thread < 1) {
        g_config.min_items_per_thread = 1;
    }
    
    /* If max_threads is 0, use auto-detection */
    if (g_config.max_threads == 0) {
        g_config.max_threads = parallel_get_optimal_threads();
    }
    
    return 0;
}

/**
 * Gets the default configuration for the parallel processor
 */
void parallel_get_default_config(ParallelProcessorConfig* config) {
    if (!config) {
        return;
    }
    
    config->max_threads = 0;              /* 0 means auto-detect */
    config->min_items_per_thread = 1;     /* Default to at least 1 item per thread */
    config->thread_stack_size = 0;        /* 0 means use system default */
    config->preserve_item_order = true;   /* Default to preserving order */
}

/**
 * Processes multiple work items in parallel
 */
int parallel_process_items(WorkItemProcessor processor, 
                         uint32_t num_items,
                         uint32_t max_threads,
                         ProcessingProgressCallback progress_callback,
                         void* user_data) {
    if (!processor || num_items == 0) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Invalid parameters for parallel processing");
        return PARALLEL_PROCESSOR_INVALID_PARAMETER;
    }
    
    /* Determine number of threads to use */
    uint32_t available_threads = (max_threads > 0) ? max_threads : g_config.max_threads;
    if (available_threads == 0) {
        available_threads = parallel_get_optimal_threads();
    }
    
    /* Don't use more threads than items */
    uint32_t threads_to_use = (available_threads > num_items) ? num_items : available_threads;
    
    /* Allocate work structures */
    std::vector<ThreadWork> work(threads_to_use);
    
    /* Calculate items per thread */
    uint32_t base_items_per_thread = num_items / threads_to_use;
    uint32_t remainder = num_items % threads_to_use;
    
    /* Make sure each thread has at least the minimum number of items */
    if (base_items_per_thread < g_config.min_items_per_thread) {
        threads_to_use = num_items / g_config.min_items_per_thread;
        if (threads_to_use == 0) threads_to_use = 1;
        
        base_items_per_thread = num_items / threads_to_use;
        remainder = num_items % threads_to_use;
        
        /* Resize the work vector */
        work.resize(threads_to_use);
    }
    
    /* Initialize work structures */
    uint32_t start_item = 0;
    int result = 0;
    
    for (uint32_t i = 0; i < threads_to_use; i++) {
        work[i].thread_id = i;
        work[i].num_threads = threads_to_use;
        work[i].start_item = start_item;
        work[i].items_per_thread = base_items_per_thread + (i < remainder ? 1 : 0);
        work[i].end_item = start_item + work[i].items_per_thread;
        work[i].total_items = num_items;
        work[i].processor = processor;
        work[i].progress_callback = progress_callback;
        work[i].user_data = user_data;
        work[i].result = 0;
        
        start_item += work[i].items_per_thread;
    }
    
    /* Create and start threads */
    std::vector<std::thread> threads;
    threads.reserve(threads_to_use);
    
    try {
        for (uint32_t i = 0; i < threads_to_use; i++) {
            threads.emplace_back(process_items_thread, &work[i]);
        }
        
        /* Wait for all threads to finish */
        for (auto& thread : threads) {
            thread.join();
        }
        
        /* Collect results */
        for (const auto& w : work) {
            if (w.result != 0) {
                result = w.result;
                break;
            }
        }
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Thread error: %s", e.what());
        return PARALLEL_PROCESSOR_THREAD_ERROR;
    }
    
    return result;
}

/**
 * Execute a task in parallel for each row group in a parquet file
 */
ParallelProcessorError parallel_processor_process_row_groups(
    const ParquetFile* file,
    ParallelTaskFunction task_function,
    void** task_data,
    ParallelTaskCleanupFunction cleanup_function,
    void*** task_results
) {
    if (!file || !task_function || !task_data || !task_results) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Invalid parameters for row group processing");
        return PARALLEL_PROCESSOR_INVALID_PARAMETER;
    }
    
    /* Nothing to do if there are no row groups */
    if (file->row_group_count == 0) {
        *task_results = NULL;
        return PARALLEL_PROCESSOR_OK;
    }
    
    /* Determine number of threads to use */
    uint32_t available_threads = g_config.max_threads;
    if (available_threads == 0) {
        available_threads = parallel_get_optimal_threads();
    }
    
    /* Don't use more threads than row groups */
    uint32_t threads_to_use = (available_threads > file->row_group_count) ? 
                            file->row_group_count : available_threads;
    
    /* Allocate task structures */
    std::vector<RowGroupTask> tasks(file->row_group_count);
    
    /* Allocate results array */
    void** results = (void**)malloc(file->row_group_count * sizeof(void*));
    if (!results) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Failed to allocate memory for row group results");
        return PARALLEL_PROCESSOR_MEMORY_ERROR;
    }
    
    /* Initialize tasks */
    for (uint32_t i = 0; i < file->row_group_count; i++) {
        tasks[i].thread_id = i % threads_to_use;
        tasks[i].file = file;
        tasks[i].row_group_index = i;
        tasks[i].task_function = task_function;
        tasks[i].task_data = task_data[i];
        tasks[i].task_result = NULL;
        tasks[i].result = 0;
    }
    
    /* Process row groups in batches */
    uint32_t batches = (file->row_group_count + threads_to_use - 1) / threads_to_use;
    ParallelProcessorError error = PARALLEL_PROCESSOR_OK;
    
    try {
        for (uint32_t batch = 0; batch < batches; batch++) {
            uint32_t start_idx = batch * threads_to_use;
            uint32_t end_idx = start_idx + threads_to_use;
            if (end_idx > file->row_group_count) end_idx = file->row_group_count;
            uint32_t batch_size = end_idx - start_idx;
            
            /* Create and start threads for this batch */
            std::vector<std::thread> threads;
            threads.reserve(batch_size);
            
            for (uint32_t i = 0; i < batch_size; i++) {
                uint32_t task_idx = start_idx + i;
                threads.emplace_back(process_row_group_thread, &tasks[task_idx]);
            }
            
            /* Wait for threads in this batch to finish */
            for (auto& thread : threads) {
                thread.join();
            }
            
            /* Store results */
            for (uint32_t i = 0; i < batch_size; i++) {
                uint32_t task_idx = start_idx + i;
                results[task_idx] = tasks[task_idx].task_result;
                
                /* Check for errors */
                if (tasks[task_idx].result != 0) {
                    error = PARALLEL_PROCESSOR_TASK_ERROR;
                    snprintf(g_error_message, sizeof(g_error_message), 
                            "Task for row group %u failed with error code %d", 
                            task_idx, tasks[task_idx].result);
                }
            }
            
            /* Stop if an error occurred */
            if (error != PARALLEL_PROCESSOR_OK) {
                break;
            }
        }
    } catch (const std::exception& e) {
        snprintf(g_error_message, sizeof(g_error_message), 
                "Thread error: %s", e.what());
        
        /* Clean up if cleanup function is provided */
        if (cleanup_function) {
            for (uint32_t i = 0; i < file->row_group_count; i++) {
                if (tasks[i].task_result) {
                    cleanup_function(task_data[i], tasks[i].task_result);
                }
            }
        }
        
        free(results);
        return PARALLEL_PROCESSOR_THREAD_ERROR;
    }
    
    /* Return results even if some tasks failed */
    *task_results = results;
    return error;
}

/**
 * Free resources allocated by parallel_processor_process_row_groups
 */
void parallel_processor_free_results(void** task_results, int count) {
    if (task_results) {
        free(task_results);
    }
}

/**
 * Get the last error message from the parallel processor
 */
const char* parallel_processor_get_error(void) {
    return g_error_message;
} 