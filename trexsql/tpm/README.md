# TPM - Trex Package Manager for DuckDB
A Rust-based DuckDB extension that provides comprehensive npm package management functionality directly from SQL. Built on DuckDB's C Extension API, TPM (Trex Package Manager) enables querying package metadata, installing packages with dependencies, visualizing dependency trees, and more - all using SQL queries.

Features:
- No DuckDB build required
- No C++ or C code required
- CI/CD chain preconfigured
- (Coming soon) Works with community extensions

## Cloning

Clone the repo with submodules

```shell
git clone --recurse-submodules <repo>
```

## Dependencies
In principle, these extensions can be compiled with the Rust toolchain alone. However, this template relies on some additional
tooling to make life a little easier and to be able to share CI/CD infrastructure with extension templates for other languages:

- Python3
- Python3-venv
- [Make](https://www.gnu.org/software/make)
- Git

Installing these dependencies will vary per platform:
- For Linux, these come generally pre-installed or are available through the distro-specific package manager.
- For MacOS, [homebrew](https://formulae.brew.sh/).
- For Windows, [chocolatey](https://community.chocolatey.org/).

## Building
After installing the dependencies, building is a two-step process. Firstly run:
```shell
make configure
```
This will ensure a Python venv is set up with DuckDB and DuckDB's test runner installed. Additionally, depending on configuration,
DuckDB will be used to determine the correct platform for which you are compiling.

Then, to build the extension run:
```shell
make debug
```
This delegates the build process to cargo, which will produce a shared library in `target/debug/<shared_lib_name>`. After this step,
a script is run to transform the shared library into a loadable extension by appending a binary footer. The resulting extension is written
to the `build/debug` directory.

To create optimized release binaries, simply run `make release` instead.

### Running the extension
To run the extension code, start `duckdb` with `-unsigned` flag. This will allow you to load the local extension file.

```sh
duckdb -unsigned
```

After loading the extension by the file path, you can use the functions provided by the extension.

### Demo Function

```sql
LOAD './build/debug/extension/tpm/tpm.duckdb_extension';
SELECT * FROM tpm('Jane');
```

```
┌──────────────┐
│   column0    │
│   varchar    │
├──────────────┤
│ TPM Jane 📦  │
└──────────────┘
```

### Package Management Functions

The extension includes **seven package management functions** for complete npm package management:

**Get package information:**
```sql
SELECT * FROM tpm_info('lodash');
```

**Resolve package to specific version (supports semver ranges):**
```sql
-- Exact version
SELECT * FROM tpm_resolve('chalk@4.1.2');

-- Caret range (^)
SELECT * FROM tpm_resolve('is-number@^7.0.0');

-- Tilde range (~)
SELECT * FROM tpm_resolve('chalk@~4.1.0');
```

**Download and install package:**
```sql
SELECT * FROM tpm_install('is-number@7.0.0', '/tmp/npm_cache');

-- Verify installation
SELECT * FROM read_json_auto('/tmp/npm_cache/is-number/7.0.0/package.json');
```

**Install package with all dependencies:**
```sql
-- Install chalk and all its dependencies recursively
SELECT
  json_extract_string(install_results, '$.package') as package,
  json_extract_string(install_results, '$.version') as version,
  json_extract_string(install_results, '$.success') as success
FROM tpm_install_with_deps('chalk@4.1.2', '/tmp/npm_packages');
```

**Visualize dependency tree:**
```sql
-- Show package dependency tree without installing
SELECT
  json_extract_string(tree_info, '$.tree_line') as tree
FROM tpm_tree('chalk@4.1.2');
```

**List installed packages:**
```sql
-- See what's installed in a directory
SELECT
  json_extract_string(list_info, '$.package') as package,
  json_extract_string(list_info, '$.version') as version
FROM tpm_list('/tmp/npm_packages')
ORDER BY package;
```

**Features:**
- ✅ Semver version range resolution (^, ~, >, <, etc.)
- ✅ SHA-1 integrity verification for all downloads
- ✅ Recursive dependency installation
- ✅ Automatic tarball extraction
- ✅ Dependency tree visualization
- ✅ List installed packages
- ✅ Scoped package support (@org/package)
- ✅ JSON output for easy parsing

## Testing
This extension uses the DuckDB Python client for testing. This should be automatically installed in the `make configure` step.
The tests themselves are written in the SQLLogicTest format, just like most of DuckDB's tests. A sample test can be found in
`test/sql/<extension_name>.test`. To run the tests using the *debug* build:

```shell
make test_debug
```

or for the *release* build:
```shell
make test_release
```

### Version switching
Testing with different DuckDB versions is really simple:

First, run
```
make clean_all
```
to ensure the previous `make configure` step is deleted.

Then, run
```
DUCKDB_TEST_VERSION=v1.3.2 make configure
```
to select a different duckdb version to test with

Finally, build and test with
```
make debug
make test_debug
```

### Known issues
This is a bit of a footgun, but the extensions produced by this template may (or may not) be broken on windows on python3.11
with the following error on extension load:
```shell
IO Error: Extension '<name>.duckdb_extension' could not be loaded: The specified module could not be found
```
This was resolved by using python 3.12
