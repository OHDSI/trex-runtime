//TODO: re-add fs interface changed and needs to be adjusted
//import { existsSync } from "ext:deno_node/_fs/_fs_exists.ts";
//import { statSync } from "ext:deno_node/_fs/_fs_stat.ts";

export const DUCKDB_FILE_DATABASE_CODE = "cdw_config_svc";
export const DUCKDB_FILE_SCHEMA_NAME = "validation_schema";

const DYNAMICALLY_GENERATED_DIR = "/usr/src/cdw_data/dynamically_generated";
const BUILT_IN_DIR = "/usr/src/cdw_data/built_in";

export const resolve_cdw_config_duckdb_file_path = () => {
  /*
		Checks if there is a duckdb file in DYNAMICALLY_GENERATED_DIR, if there is a file there, use it.
		Else fallback to using the built in duckdb file in BUILT_IN_DIR
		*/
  const DUCKDB_FILE_NAME = `${DUCKDB_FILE_DATABASE_CODE}_${DUCKDB_FILE_SCHEMA_NAME}`;
  const DYNAMICALLY_GENERATED_DUCKDB_FILE_PATH = `${DYNAMICALLY_GENERATED_DIR}/${DUCKDB_FILE_NAME}`;
  const BUILT_IN_DUCKDB_FILE_PATH = `${BUILT_IN_DIR}/${DUCKDB_FILE_NAME}`;

  let cdw_duckdb_file_path;

  //if (existsSync(DYNAMICALLY_GENERATED_DUCKDB_FILE_PATH)) {
  //  cdw_duckdb_file_path = DYNAMICALLY_GENERATED_DUCKDB_FILE_PATH;
  //} else {
  cdw_duckdb_file_path = BUILT_IN_DUCKDB_FILE_PATH;
  //}
  console.log(`Using cdw_svc duckdb file from ${cdw_duckdb_file_path}`);
  return [cdw_duckdb_file_path, 0 /*statSync(cdw_duckdb_file_path).mtime*/];
};
