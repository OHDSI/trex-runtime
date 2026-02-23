package org.trex;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface TrexEngine extends Library {
    // Pre-load libduckdb (transitive dependency of libtrexsql_engine) so the
    // dynamic linker can resolve it when libtrexsql_engine.so is loaded.
    // JNA extracts from classpath (linux-x86-64/libduckdb.so) and dlopen
    // registers the SONAME (libduckdb.so.1.4).
    class Loader {
        static {
            Native.load("duckdb", Library.class);
        }
        static void ensure() {}
    }

    TrexEngine INSTANCE = createInstance();

    static TrexEngine createInstance() {
        Loader.ensure();
        return Native.load("trexsql_engine", TrexEngine.class);
    }

    String trexsql_last_error();
    void trexsql_free_string(Pointer s);

    Pointer trexsql_open(String path, int flags);
    void trexsql_close(Pointer db);

    int trexsql_execute(Pointer db, String sql);
    Pointer trexsql_query(Pointer db, String sql);

    int trexsql_result_column_count(Pointer r);
    String trexsql_result_column_name(Pointer r, int col);
    int trexsql_result_next(Pointer r);
    int trexsql_result_is_null(Pointer r, int col);
    Pointer trexsql_result_get_string(Pointer r, int col);
    long trexsql_result_get_long(Pointer r, int col);
    double trexsql_result_get_double(Pointer r, int col);
    void trexsql_result_close(Pointer r);

    Pointer trexsql_appender_create(Pointer db, String schema, String table);
    int trexsql_appender_end_row(Pointer a);
    int trexsql_appender_append_null(Pointer a);
    int trexsql_appender_append_string(Pointer a, String val);
    int trexsql_appender_append_long(Pointer a, long val);
    int trexsql_appender_append_int(Pointer a, int val);
    int trexsql_appender_append_double(Pointer a, double val);
    int trexsql_appender_append_boolean(Pointer a, int val);
    int trexsql_appender_flush(Pointer a);
    int trexsql_appender_close(Pointer a);
}
