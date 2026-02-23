---
sidebar_position: 2
---

# tpm — Plugin Manager

The `tpm` extension provides NPM-compatible package management for trexsql plugins. It can resolve, install, list, and delete plugin packages from a configured registry.

## Functions

### `trex_plugin(name)`

Fetch package metadata from the registry.

| Parameter | Type | Description |
|-----------|------|-------------|
| name | VARCHAR | Package name |

**Returns:** TABLE(column0 VARCHAR)

```sql
SELECT * FROM trex_plugin('@trex/my-plugin');
```

### `trex_plugin_info(package_name)`

Get detailed package information in JSON format.

| Parameter | Type | Description |
|-----------|------|-------------|
| package_name | VARCHAR | Package name |

**Returns:** TABLE(package_info VARCHAR) — JSON

```sql
SELECT * FROM trex_plugin_info('@trex/my-plugin');
```

### `trex_plugin_resolve(package_spec)`

Resolve a package specification to a specific version.

| Parameter | Type | Description |
|-----------|------|-------------|
| package_spec | VARCHAR | Package name with optional version (e.g. `@trex/plugin@^1.0.0`) |

**Returns:** TABLE(resolve_info VARCHAR) — JSON

```sql
SELECT * FROM trex_plugin_resolve('@trex/my-plugin@^1.0.0');
```

### `trex_plugin_install(package_spec, install_dir)`

Install a single plugin package.

| Parameter | Type | Description |
|-----------|------|-------------|
| package_spec | VARCHAR | Package name with version |
| install_dir | VARCHAR | Installation directory |

**Returns:** TABLE(install_results VARCHAR) — JSON

```sql
SELECT * FROM trex_plugin_install('@trex/my-plugin@1.2.0', './plugins');
```

### `trex_plugin_install_with_deps(package_spec, install_dir)`

Install a plugin package with all its dependencies.

| Parameter | Type | Description |
|-----------|------|-------------|
| package_spec | VARCHAR | Package name with version |
| install_dir | VARCHAR | Installation directory |

**Returns:** TABLE(install_results VARCHAR) — JSON, multiple rows

```sql
SELECT * FROM trex_plugin_install_with_deps('@trex/my-plugin@1.2.0', './plugins');
```

### `trex_plugin_tree(package_spec)`

Display the dependency tree for a package.

| Parameter | Type | Description |
|-----------|------|-------------|
| package_spec | VARCHAR | Package name with version |

**Returns:** TABLE(tree_info VARCHAR) — JSON, multiple rows

```sql
SELECT * FROM trex_plugin_tree('@trex/my-plugin@1.2.0');
```

### `trex_plugin_list(install_dir)`

List all installed plugins in a directory.

| Parameter | Type | Description |
|-----------|------|-------------|
| install_dir | VARCHAR | Plugin installation directory |

**Returns:** TABLE(list_info VARCHAR) — JSON, multiple rows

```sql
SELECT * FROM trex_plugin_list('./plugins');
```

### `trex_plugin_seed(install_dir)`

Seed plugins from the configured feed into the installation directory.

| Parameter | Type | Description |
|-----------|------|-------------|
| install_dir | VARCHAR | Plugin installation directory |

**Returns:** TABLE(seed_results VARCHAR) — JSON, multiple rows

```sql
SELECT * FROM trex_plugin_seed('./plugins');
```

### `trex_plugin_delete(package_name, install_dir)`

Delete an installed plugin.

| Parameter | Type | Description |
|-----------|------|-------------|
| package_name | VARCHAR | Package name to remove |
| install_dir | VARCHAR | Plugin installation directory |

**Returns:** TABLE(delete_results VARCHAR) — JSON

```sql
SELECT * FROM trex_plugin_delete('@trex/my-plugin', './plugins');
```
