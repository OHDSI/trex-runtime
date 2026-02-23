#ifndef __CQL2ELM_NATIVE_H
#define __CQL2ELM_NATIVE_H

#include <graal_isolate_dynamic.h>


#if defined(__cplusplus)
extern "C" {
#endif

typedef char* (*cql2elm_translate_fn_t)(graal_isolatethread_t*, char*);

typedef int (*run_main_fn_t)(int argc, char** argv);

#if defined(__cplusplus)
}
#endif
#endif
