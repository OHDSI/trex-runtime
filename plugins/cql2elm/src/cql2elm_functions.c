#include "duckdb_extension.h"
#include "cql2elm_functions.h"
#include <dlfcn.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <errno.h>

#ifdef CQL2ELM_EMBEDDED_NATIVE_LIB
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "cql2elm_native_embedded.h"
#endif

#define CQL2ELM_WORKER_STACK_SIZE (16 * 1024 * 1024)

DUCKDB_EXTENSION_EXTERN

typedef struct graal_isolate graal_isolate_t;
typedef struct graal_isolatethread graal_isolatethread_t;

typedef char *(*cql2elm_translate_fn)(graal_isolatethread_t *, char *cql_text);
typedef int (*graal_create_isolate_fn)(void *params, graal_isolate_t **isolate, graal_isolatethread_t **thread);
typedef int (*graal_attach_thread_fn)(graal_isolate_t *isolate, graal_isolatethread_t **thread);
typedef int (*graal_detach_thread_fn)(graal_isolatethread_t *thread);

static void *cql2elm_lib_handle = NULL;
static graal_isolate_t *cql2elm_isolate = NULL;
static graal_isolatethread_t *cql2elm_thread = NULL;
static cql2elm_translate_fn cql2elm_translate = NULL;
static graal_create_isolate_fn graal_create_isolate_ptr = NULL;
static graal_attach_thread_fn graal_attach_thread_ptr = NULL;
static graal_detach_thread_fn graal_detach_thread_ptr = NULL;

static pthread_once_t cql2elm_init_once = PTHREAD_ONCE_INIT;
static int cql2elm_init_success = 0;

typedef struct {
    char* cql_text;
    char* result;
} cql2elm_work_t;

static void* cql2elm_worker_thread(void* arg) {
    cql2elm_work_t* work = (cql2elm_work_t*)arg;

    graal_isolatethread_t* thread = NULL;
    if (graal_attach_thread_ptr(cql2elm_isolate, &thread) != 0 || !thread) {
        work->result = NULL;
        return NULL;
    }

    work->result = cql2elm_translate(thread, work->cql_text);
    if (graal_detach_thread_ptr(thread) != 0) {
        fprintf(stderr, "cql2elm: graal_detach_thread failed\n");
    }
    return NULL;
}

static char* cql2elm_run_with_large_stack(char* cql_text) {
    cql2elm_work_t work = {cql_text, NULL};
    pthread_t thread;
    pthread_attr_t attr;

    if (pthread_attr_init(&attr) != 0) {
        /* Fallback: run on current thread */
        graal_isolatethread_t* t = NULL;
        if (graal_attach_thread_ptr(cql2elm_isolate, &t) == 0 && t) {
            char* r = cql2elm_translate(t, cql_text);
            graal_detach_thread_ptr(t);
            return r;
        }
        return NULL;
    }

    if (pthread_attr_setstacksize(&attr, CQL2ELM_WORKER_STACK_SIZE) != 0) {
        pthread_attr_destroy(&attr);
        return NULL;
    }

    if (pthread_create(&thread, &attr, cql2elm_worker_thread, &work) != 0) {
        pthread_attr_destroy(&attr);
        return NULL;
    }

    int rc;
    while ((rc = pthread_join(thread, NULL)) == EINTR) {
    }
    pthread_attr_destroy(&attr);
    return work.result;
}

#ifdef CQL2ELM_EMBEDDED_NATIVE_LIB
static void *LoadEmbeddedCql2ElmLibrary() {
    if (!cql2elm_native_blob || cql2elm_native_blob_len == 0) return NULL;
    char tmpl[] = "/tmp/cql2elm-native-XXXXXX.so";
    int fd = mkstemps(tmpl, 3);
    if (fd < 0) return NULL;
    size_t remaining = cql2elm_native_blob_len;
    const unsigned char *ptr = cql2elm_native_blob;
    while (remaining > 0) {
        ssize_t w = write(fd, ptr, remaining);
        if (w <= 0) { close(fd); unlink(tmpl); return NULL; }
        ptr += w; remaining -= w;
    }
    fsync(fd);
    void *handle = dlopen(tmpl, RTLD_LAZY | RTLD_LOCAL);
    unlink(tmpl);
    close(fd);
    return handle;
}
#endif

static void Cql2ElmInitOnce(void) {
#ifdef CQL2ELM_EMBEDDED_NATIVE_LIB
    cql2elm_lib_handle = LoadEmbeddedCql2ElmLibrary();
#endif

    if (!cql2elm_lib_handle) {
        const char *candidates[] = {
            "./cql2elm-be/native-libs/libcql2elm-native.so",
            "./cql2elm-be/native-libs/linux-x86_64/libcql2elm-native.so",
            "libcql2elm-native.so"
        };
        for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); i++) {
            cql2elm_lib_handle = dlopen(candidates[i], RTLD_LAZY | RTLD_LOCAL);
            if (cql2elm_lib_handle) break;
        }
    }

    if (!cql2elm_lib_handle) return;

    void *sym_translate = dlsym(cql2elm_lib_handle, "cql2elm_translate");
    if (!sym_translate) return;
    void *sym_create = dlsym(cql2elm_lib_handle, "graal_create_isolate");
    if (!sym_create) return;
    void *sym_attach = dlsym(cql2elm_lib_handle, "graal_attach_thread");
    if (!sym_attach) return;
    void *sym_detach = dlsym(cql2elm_lib_handle, "graal_detach_thread");
    if (!sym_detach) return;

    graal_create_isolate_ptr = (graal_create_isolate_fn)sym_create;
    graal_attach_thread_ptr = (graal_attach_thread_fn)sym_attach;
    graal_detach_thread_ptr = (graal_detach_thread_fn)sym_detach;

    int rc = graal_create_isolate_ptr(NULL, &cql2elm_isolate, &cql2elm_thread);
    if (rc != 0 || !cql2elm_thread) return;

    cql2elm_translate = (cql2elm_translate_fn)sym_translate;

    cql2elm_init_success = 1;
}

static int EnsureCql2ElmLoaded() {
    pthread_once(&cql2elm_init_once, Cql2ElmInitOnce);
    return cql2elm_init_success;
}

static char* get_string_from_vector(duckdb_vector vector, idx_t row) {
    duckdb_string_t *string_data = (duckdb_string_t*)duckdb_vector_get_data(vector);
    duckdb_string_t str = string_data[row];
    uint32_t len = duckdb_string_t_length(str);
    const char* data = duckdb_string_t_data(&str);

    char* result = (char*)duckdb_malloc(len + 1);
    if (result) {
        memcpy(result, data, len);
        result[len] = '\0';
    }
    return result;
}

static void set_string_in_vector(duckdb_vector vector, idx_t row, const char* str) {
    duckdb_vector_assign_string_element(vector, row, str);
}

static void Cql2ElmTranslateFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    if (!EnsureCql2ElmLoaded()) {
        duckdb_scalar_function_set_error(info, "cql_to_elm: failed to load cql2elm native library");
        return;
    }

    idx_t input_size = duckdb_data_chunk_get_size(input);
    duckdb_vector cql_vector = duckdb_data_chunk_get_vector(input, 0);
    uint64_t* cql_validity = duckdb_vector_get_validity(cql_vector);
    uint64_t* result_validity = NULL;

    if (cql_validity) {
        duckdb_vector_ensure_validity_writable(output);
        result_validity = duckdb_vector_get_validity(output);
    }

    for (idx_t row = 0; row < input_size; row++) {
        if (cql_validity && !duckdb_validity_row_is_valid(cql_validity, row)) {
            if (!result_validity) {
                duckdb_vector_ensure_validity_writable(output);
                result_validity = duckdb_vector_get_validity(output);
            }
            duckdb_validity_set_row_invalid(result_validity, row);
            continue;
        }

        char* cql_text = get_string_from_vector(cql_vector, row);
        if (!cql_text) {
            if (!result_validity) {
                duckdb_vector_ensure_validity_writable(output);
                result_validity = duckdb_vector_get_validity(output);
            }
            duckdb_validity_set_row_invalid(result_validity, row);
            continue;
        }

        char* elm_json = cql2elm_run_with_large_stack(cql_text);
        if (elm_json) {
            if (strncmp(elm_json, "{\"error\":", 9) == 0) {
                duckdb_scalar_function_set_error(info, elm_json);
                duckdb_free(cql_text);
                return;
            }
            set_string_in_vector(output, row, elm_json);
        } else {
            duckdb_scalar_function_set_error(info, "cql_to_elm: translation returned NULL");
            duckdb_free(cql_text);
            return;
        }

        duckdb_free(cql_text);
    }
}

void RegisterCql2ElmTranslateFunction(duckdb_connection connection) {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "trex_fhir_cql_translate");

    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_scalar_function_add_parameter(function, varchar_type);
    duckdb_scalar_function_set_return_type(function, varchar_type);
    duckdb_destroy_logical_type(&varchar_type);

    duckdb_scalar_function_set_function(function, Cql2ElmTranslateFunction);
    duckdb_register_scalar_function(connection, function);
    duckdb_destroy_scalar_function(&function);
}
