#ifndef __CQL2ELM_NATIVE_H
#define __CQL2ELM_NATIVE_H

#include <graal_isolate.h>


#if defined(__cplusplus)
extern "C" {
#endif

char* cql2elm_translate(graal_isolatethread_t*, char*);

int run_main(int argc, char** argv);

#if defined(__cplusplus)
}
#endif
#endif
