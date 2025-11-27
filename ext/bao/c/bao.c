#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <getopt.h>
#include <duckdb.h>

static duckdb_database db = NULL;
static duckdb_connection conn = NULL;
static volatile sig_atomic_t keep_running = 1;

void signal_handler(int sig) {
    (void)sig;
    keep_running = 0;
    printf("\n\nShutting down...\n");
}

int has_avx_support(void) {
    FILE *f = fopen("/proc/cpuinfo", "r");
    if (!f) return 0;

    char line[1024];
    int has_avx = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strstr(line, " avx ") || strstr(line, "\tavx\t") ||
            strstr(line, " avx\n") || strstr(line, "\tavx\n")) {
            has_avx = 1;
            break;
        }
    }
    fclose(f);
    return has_avx;
}

int ends_with(const char *str, const char *suffix) {
    size_t str_len = strlen(str);
    size_t suffix_len = strlen(suffix);
    if (suffix_len > str_len) return 0;
    return strcmp(str + str_len - suffix_len, suffix) == 0;
}

int load_extensions(duckdb_connection conn, const char *extensions_path) {
    DIR *dir = opendir(extensions_path);
    if (!dir) {
        fprintf(stderr, "Warning: Could not open extensions directory: %s\n", extensions_path);
        return 0;
    }

    int avx_support = has_avx_support();
    struct dirent *entry;
    char path[4096];
    char sql[8192];

    while ((entry = readdir(dir)) != NULL) {
        if (ends_with(entry->d_name, ".duckdb_extension")) {
            if (strstr(entry->d_name, "llama") && !avx_support) {
                fprintf(stderr, "Skipping llama extension (no AVX support)\n");
                continue;
            }

            snprintf(path, sizeof(path), "%s/%s", extensions_path, entry->d_name);
            printf("Loading extension: %s\n", entry->d_name);
            snprintf(sql, sizeof(sql), "LOAD '%s'", path);

            if (duckdb_query(conn, sql, NULL) == DuckDBError) {
                fprintf(stderr, "Failed to load extension: %s\n", path);
            }
        }

        struct stat st;
        snprintf(path, sizeof(path), "%s/%s", extensions_path, entry->d_name);
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode) && entry->d_name[0] != '.') {
            DIR *subdir = opendir(path);
            if (subdir) {
                struct dirent *subentry;
                while ((subentry = readdir(subdir)) != NULL) {
                    if (ends_with(subentry->d_name, ".duckdb_extension")) {
                        // Skip llama if no AVX
                        if (strstr(subentry->d_name, "llama") && !avx_support) {
                            fprintf(stderr, "Skipping llama extension (no AVX support)\n");
                            continue;
                        }

                        char ext_path[4096];
                        snprintf(ext_path, sizeof(ext_path), "%s/%s", path, subentry->d_name);

                        // Extract extension name
                        char ext_name[256];
                        strncpy(ext_name, subentry->d_name, sizeof(ext_name) - 1);
                        char *dot = strstr(ext_name, ".duckdb_extension");
                        if (dot) *dot = '\0';

                        printf("Loading extension: %s\n", ext_name);
                        snprintf(sql, sizeof(sql), "LOAD '%s'", ext_path);

                        if (duckdb_query(conn, sql, NULL) == DuckDBError) {
                            fprintf(stderr, "Failed to load extension: %s\n", ext_path);
                        }
                    }
                }
                closedir(subdir);
            }
        }
    }

    closedir(dir);
    return 0;
}

char *build_trexas_config(const char *host, int port, const char *main_path,
                          const char *event_worker_path, const char *tls_cert,
                          const char *tls_key, int tls_port, int enable_inspector,
                          const char *inspector_type, const char *inspector_host,
                          int inspector_port, int allow_main_inspector) {
    static char config[4096];
    int offset = 0;

    offset += snprintf(config + offset, sizeof(config) - offset,
                       "{\"host\":\"%s\",\"port\":%d,\"main_service_path\":\"%s\"",
                       host, port, main_path);

    if (event_worker_path && event_worker_path[0]) {
        offset += snprintf(config + offset, sizeof(config) - offset,
                           ",\"event_worker_path\":\"%s\"", event_worker_path);
    }

    if (tls_cert && tls_cert[0]) {
        offset += snprintf(config + offset, sizeof(config) - offset,
                           ",\"tls_cert_path\":\"%s\"", tls_cert);
    }

    if (tls_key && tls_key[0]) {
        offset += snprintf(config + offset, sizeof(config) - offset,
                           ",\"tls_key_path\":\"%s\"", tls_key);
    }

    if (tls_cert && tls_cert[0]) {
        offset += snprintf(config + offset, sizeof(config) - offset,
                           ",\"tls_port\":%d", tls_port);
    }

    if (enable_inspector) {
        offset += snprintf(config + offset, sizeof(config) - offset,
                           ",\"inspector\":\"%s:%s:%d\"",
                           inspector_type, inspector_host, inspector_port);
    }

    if (allow_main_inspector) {
        offset += snprintf(config + offset, sizeof(config) - offset,
                           ",\"allow_main_inspector\":true");
    }

    snprintf(config + offset, sizeof(config) - offset, "}");
    return config;
}

void print_usage(const char *prog) {
    printf("Usage: %s [options]\n\n", prog);
    printf("Options:\n");
    printf("  --trexas-host <host>        Trexas server host (default: 0.0.0.0)\n");
    printf("  --trexas-port <port>        Trexas server port (default: 9876)\n");
    printf("  --pgwire-host <host>        PgWire server host (default: 0.0.0.0)\n");
    printf("  --pgwire-port <port>        PgWire server port (default: 5433)\n");
    printf("  --main-path <path>          Path to main service directory (default: ./main)\n");
    printf("  --event-worker-path <path>  Path to event worker directory\n");
    printf("  --tls-cert <path>           Path to TLS certificate file\n");
    printf("  --tls-key <path>            Path to TLS private key file\n");
    printf("  --tls-port <port>           TLS port (default: 9443)\n");
    printf("  --enable-inspector          Enable Trexas inspector\n");
    printf("  --inspector-type <type>     Inspector type (default: inspect)\n");
    printf("  --inspector-host <host>     Inspector host (default: 0.0.0.0)\n");
    printf("  --inspector-port <port>     Inspector port (default: 9229)\n");
    printf("  --allow-main-inspector      Allow inspector in main worker\n");
    printf("  -h, --help                  Show this help message\n");
}

int main(int argc, char *argv[]) {
    const char *trexas_host = "0.0.0.0";
    int trexas_port = 9876;
    const char *pgwire_host = "0.0.0.0";
    int pgwire_port = 5433;
    const char *main_path = "./main";
    const char *event_worker_path = NULL;
    const char *tls_cert = NULL;
    const char *tls_key = NULL;
    int tls_port = 9443;
    int enable_inspector = 0;
    const char *inspector_type = "inspect";
    const char *inspector_host = "0.0.0.0";
    int inspector_port = 9229;
    int allow_main_inspector = 0;

    static struct option long_options[] = {
        {"trexas-host", required_argument, 0, 0},
        {"trexas-port", required_argument, 0, 0},
        {"pgwire-host", required_argument, 0, 0},
        {"pgwire-port", required_argument, 0, 0},
        {"main-path", required_argument, 0, 0},
        {"event-worker-path", required_argument, 0, 0},
        {"tls-cert", required_argument, 0, 0},
        {"tls-key", required_argument, 0, 0},
        {"tls-port", required_argument, 0, 0},
        {"enable-inspector", no_argument, 0, 0},
        {"inspector-type", required_argument, 0, 0},
        {"inspector-host", required_argument, 0, 0},
        {"inspector-port", required_argument, 0, 0},
        {"allow-main-inspector", no_argument, 0, 0},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;
    while ((opt = getopt_long(argc, argv, "h", long_options, &option_index)) != -1) {
        if (opt == 0) {
            const char *name = long_options[option_index].name;
            if (strcmp(name, "trexas-host") == 0) trexas_host = optarg;
            else if (strcmp(name, "trexas-port") == 0) trexas_port = atoi(optarg);
            else if (strcmp(name, "pgwire-host") == 0) pgwire_host = optarg;
            else if (strcmp(name, "pgwire-port") == 0) pgwire_port = atoi(optarg);
            else if (strcmp(name, "main-path") == 0) main_path = optarg;
            else if (strcmp(name, "event-worker-path") == 0) event_worker_path = optarg;
            else if (strcmp(name, "tls-cert") == 0) tls_cert = optarg;
            else if (strcmp(name, "tls-key") == 0) tls_key = optarg;
            else if (strcmp(name, "tls-port") == 0) tls_port = atoi(optarg);
            else if (strcmp(name, "enable-inspector") == 0) enable_inspector = 1;
            else if (strcmp(name, "inspector-type") == 0) inspector_type = optarg;
            else if (strcmp(name, "inspector-host") == 0) inspector_host = optarg;
            else if (strcmp(name, "inspector-port") == 0) inspector_port = atoi(optarg);
            else if (strcmp(name, "allow-main-inspector") == 0) allow_main_inspector = 1;
        } else if (opt == 'h') {
            print_usage(argv[0]);
            return 0;
        }
    }

    printf("🦕 Starting TREX\n");

    // Get required environment variables
    const char *pgwire_password = getenv("TREX_SQL_PASSWORD");
    if (!pgwire_password || !pgwire_password[0]) {
        fprintf(stderr, "Error: TREX_SQL_PASSWORD environment variable is not set\n");
        return 1;
    }

    const char *extensions_path = getenv("TREX_EXTENSIONS_PATH");
    if (!extensions_path) {
        extensions_path = "node_modules/@trex";
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    duckdb_config config;
    if (duckdb_create_config(&config) == DuckDBError) {
        fprintf(stderr, "Failed to create DuckDB config\n");
        return 1;
    }

    duckdb_set_config(config, "allow_unsigned_extensions", "true");

    char *error = NULL;
    if (duckdb_open_ext(NULL, &db, config, &error) == DuckDBError) {
        fprintf(stderr, "Failed to open DuckDB: %s\n", error ? error : "unknown error");
        duckdb_free(error);
        duckdb_destroy_config(&config);
        return 1;
    }
    duckdb_destroy_config(&config);

    if (duckdb_connect(db, &conn) == DuckDBError) {
        fprintf(stderr, "Failed to connect to DuckDB\n");
        duckdb_close(&db);
        return 1;
    }

    load_extensions(conn, extensions_path);

    printf("\n🚀 Starting servers...\n");

    char sql[8192];
    duckdb_result result;

    snprintf(sql, sizeof(sql),
             "SELECT start_pgwire_server('%s', %d, '%s', '') as result",
             pgwire_host, pgwire_port, pgwire_password);

    if (duckdb_query(conn, sql, &result) == DuckDBError) {
        fprintf(stderr, "Failed to start pgwire server: %s\n", duckdb_result_error(&result));
        duckdb_destroy_result(&result);
        duckdb_disconnect(&conn);
        duckdb_close(&db);
        return 1;
    }

    char *pgwire_result_str = duckdb_value_varchar(&result, 0, 0);
    printf("PgWire server: %s\n", pgwire_result_str);
    duckdb_free(pgwire_result_str);
    duckdb_destroy_result(&result);

    char *trexas_config = build_trexas_config(trexas_host, trexas_port, main_path,
                                               event_worker_path, tls_cert, tls_key,
                                               tls_port, enable_inspector, inspector_type,
                                               inspector_host, inspector_port,
                                               allow_main_inspector);

    snprintf(sql, sizeof(sql),
             "SELECT trex_start_server_with_config('%s') as result", trexas_config);

    if (duckdb_query(conn, sql, &result) == DuckDBError) {
        fprintf(stderr, "Failed to start trexas server: %s\n", duckdb_result_error(&result));
        duckdb_destroy_result(&result);
        duckdb_disconnect(&conn);
        duckdb_close(&db);
        return 1;
    }

    char *trexas_result_str = duckdb_value_varchar(&result, 0, 0);
    printf("Trexas server: %s\n", trexas_result_str);
    duckdb_free(trexas_result_str);
    duckdb_destroy_result(&result);

    printf("\n✅ Servers started successfully\n");
    printf("Trexas listening on %s%s:%d%s%s\n",
           tls_cert ? "https://" : "http://",
           trexas_host, trexas_port,
           enable_inspector ? " (inspector enabled)" : "",
           event_worker_path ? " (with event worker)" : " (without event worker)");
    printf("PgWire listening on %s:%d\n", pgwire_host, pgwire_port);
    printf("\nPress Ctrl+C to stop\n");

    while (keep_running) {
        sleep(1);
    }

    duckdb_disconnect(&conn);
    duckdb_close(&db);

    return 0;
}
