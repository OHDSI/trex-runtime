#include "duckdb_extension.h"
#include "cql2elm_functions.h"

DUCKDB_EXTENSION_EXTERN

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access) {
    RegisterCql2ElmTranslateFunction(connection);
    return true;
}
