import { core } from "ext:core/mod.js";
import { TrexConnection } from './pgconnection.js';
//import { HanaConnection } from './hdbconnection.js';
import { resolve_cdw_config_duckdb_file_path, DUCKDB_FILE_DATABASE_CODE, DUCKDB_FILE_SCHEMA_NAME } from "./cdw_svc.js"

//import * as hdb from './hdb.js';
//import * as p from './postgres.js';

const ops = core.ops;

const {
	op_prompt,
	op_prompt_next,
	op_add_replication,
	op_copy_tables,
	op_install_plugin,
	op_execute_query,
	op_atlas,
	op_exit,
	op_get_dbc,
	op_set_dbc,
	op_execute_query_stream,
	op_execute_query_stream_next
} = ops;

export { op_add_replication, op_exit };

export async function executeQueryStream(database, sql, params = []) {
    const nparams = params.map(v => {
        if(typeof(v) === 'string' || v instanceof String) {
            try {
                const d = Date.parse(v);    
                if(/^\d\d\d\d-\d\d-\d\d/.test(v) && d) {
                    return {"DateTime": d};
                }
            } catch (e) {}
            return {"String": v}
        }
        return {"Number": v};
    });
    
    const streamId = op_execute_query_stream(database, sql, nparams);

    return new ReadableStream({
        async start(controller) {
            try {
                while (true) {
                    const chunk = await op_execute_query_stream_next(streamId);
                    if (chunk === null) {
                        controller.close();
                        break;
                    }
                    
                    // Check if the chunk is an error message
                    try {
                        const parsed = JSON.parse(chunk);
                        if (parsed.error) {
                            controller.error(new Error(parsed.error));
                            break;
                        }
                    } catch (e) {
                        // Not JSON, continue normally
                    }
                    
                    controller.enqueue(chunk);
                }
            } catch (error) {
                console.error("Stream error:", error);
                controller.error(error);
            }
        }
    });
}

export async function prompt(xprompt, model = null) {
    const streamId = op_prompt(xprompt, 2048, model);

    return new ReadableStream({
        async start(controller) {
            while (true) {
                const chunk = await op_prompt_next(streamId);
                if (chunk === null) {
                    controller.close();
                    break;
                }
                controller.enqueue(chunk);
            }
        }
    });
}

export class DatabaseManager {
	static #dbm;

	// Information regarding attached cdw-svc duckdb file
	#attached_cdw_svc_file_path = null;
	#attached_cdw_svc_file_mtime = null;

	#contructor() {}

	static getDatabaseManager() {
		if(!DatabaseManager.#dbm) {
			DatabaseManager.#dbm = new DatabaseManager();
		}
		return DatabaseManager.#dbm;
	}

	setCredentials(credentials) {
		const dbc = JSON.parse(op_get_dbc());
		op_set_dbc(JSON.stringify({credentials: credentials, publications: dbc.publications}));
		this.#updatePublications();
	}
	#setPublications(pub) {
		const dbc = JSON.parse(op_get_dbc());
		op_set_dbc(JSON.stringify({credentials: dbc.credentials, publications: pub}));

	}

	 // This is temporary workaround to enable communication with Postgres since cohort tables are only populated in postgres and not in duckdb yet. Once we enable the write mode on duckdb for cohort tables, then this can be removed.
	#add_postgres(
		name, credentials
    ) {
        
		op_execute_query("memory","INSTALL postgres",[]);
		op_execute_query("memory","LOAD postgres",[]);
		op_execute_query("memory",
        `ATTACH IF NOT EXISTS 'host=${credentials.host} port=${credentials.port} dbname=${credentials.databaseName} user=${credentials.user} password=${credentials.password}' AS ${name} (TYPE postgres)`, []
        );
    }

	#add_bigquery(
		name, credentials
    ) {
		op_execute_query("memory","INSTALL bigquery FROM community",[]);
		op_execute_query("memory","LOAD bigquery",[]);
		op_execute_query("memory",
        `ATTACH IF NOT EXISTS 'project=${credentials.project} dataset=${credentials.dataset}' AS ${name} (TYPE bigquery, READ_ONLY)`, []
        );
			}

    add_cdw_config_duckdb_connection() {
    /*
		Checks if there is a duckdb file in /usr/src/cdw_data/dynamically_generated, if there is a file there, use it.
		Else fallback to using the built in duckdb file in /usr/src/cdw_data/built_in
		*/
    const [duckdb_file_path, file_mtime] =
      resolve_cdw_config_duckdb_file_path();

    if (
      this.#attached_cdw_svc_file_path === null || // File not attached yet
      this.#attached_cdw_svc_file_mtime === null || // File not attached yet
      duckdb_file_path !== this.#attached_cdw_svc_file_path || // There is a new dynamically created cdw-svc duckdb file
      file_mtime > this.#attached_cdw_svc_file_mtime // There is a new dynamically created cdw-svc duckdb file
    ) {
      op_execute_query(
        "memory",
        `DETACH DATABASE IF EXISTS ${DUCKDB_FILE_SCHEMA_NAME}`,
        []
      );
      op_execute_query(
        "memory",
        `ATTACH IF NOT EXISTS '${duckdb_file_path}' AS ${DUCKDB_FILE_SCHEMA_NAME} (READ_ONLY)`,
        []
      );
    }
    this.#attached_cdw_svc_file_path = duckdb_file_path;
    this.#attached_cdw_svc_file_mtime = file_mtime;
  }


	#updatePublications() {
		for(const c of this.getCredentials()) {
			const adminCredentials = c.credentials.filter(c => c.userScope === 'Admin')[0];

			if(c.dialect == 'postgres' && c.publications && c.publications.length > 0 ) {
				console.log(`TREX PUB FOUND ${c.id}`)
				for(const p of c.publications) {
					const key = `${c.id}_${p.publication}`
					if(!(key in this.getPublications)) {
						op_add_replication(p.publication, p.slot, key, c.host, c.port, c.name, adminCredentials.username, adminCredentials.password);
						this.#add_postgres(`${key}_trexpg`, {host: c.host, port: c.port, databaseName: c.name, user: adminCredentials.username, password: adminCredentials.password});
						const pub = this.getPublications();
						pub[key] = true;
						this.#setPublications(pub);
					}
				}
			} else if (c.vocab_schemas && c.vocab_schemas.length > 0 && c.dialect == 'postgres') {
				console.log(`TREX NO PUB FOUND ${c.id}`)
				const key = `${c.id}`
				if(!(key in this.getPublications)) {
					this.#add_postgres(`${key}_trexpg`, {host: c.host, port: c.port, databaseName: c.name, user: adminCredentials.username, password: adminCredentials.password});
					const schemas = c.vocab_schemas.map(x => `'${x}'`).join(",");
					const res = JSON.parse(op_execute_query(`${key}_trexpg`,`select table_schema as schema,table_name as name from information_schema.tables where table_type = 'BASE TABLE' and table_schema in (${schemas})`, []));
					op_copy_tables(res, key, c.host, c.port, c.name, adminCredentials.username, adminCredentials.password);
					const pub = this.getPublications();
					pub[key] = true;
					this.#setPublications(pub);
				}
			} else if (c.dialect == 'bigquery') {
				console.log(`TREX ADD BQ ${c.id}`)
				const key = `${c.id}`
				if(!(key in this.getPublications)) {
					this.#add_bigquery(`${key}`, {project: c.host, dataset: c.name});
					const pub = this.getPublications();
					pub[key] = true;
					this.#setPublications(pub);
				}
			} else {
				console.log(`TREX DB NOT SUPPORTED ${c.id}`)
			}
		}
	}

	getFirstPublication(db_id) {
		try {
			const tmp =  this.getCredentials().filter(c => c.id === db_id)[0].publications[0]
			if(tmp)
				return `${db_id}_${tmp.publication}`
		} catch(e) {
		}
		return `${db_id}`
	}


	getPublications() {
		return JSON.parse(op_get_dbc()).publications;
	}

	getCredentials() {
		return JSON.parse(op_get_dbc()).credentials;
	}

}

export class UserDatabaseManager {
	#dbm;
	#userWorker
	constructor(userWorker) {
		this.#dbm = DatabaseManager.getDatabaseManager();
		this.#userWorker = userWorker;
	}

	getDatabases() {
		return this.#dbm.getCredentials().map(x => {
			return x.id;
		})
	}

	getDatabaseCredentials() {
		return this.#dbm.getCredentials();
	}
	
	getFirstPublication(db_id) {
		return this.#dbm.getFirstPublication(db_id);
	}


	getConnection(db_id, schema, vocab_schema, translationMap) {
		return new TrexConnection(new TrexDB(db_id), new TrexDB(`${db_id}_trexpg`), schema,vocab_schema,translationMap);
	}
}



export class TrexDB {
	#database;
	constructor(database) {
		const dbm = DatabaseManager.getDatabaseManager();

		if (database === DUCKDB_FILE_DATABASE_CODE) {
      this.#database = DUCKDB_FILE_DATABASE_CODE;
			dbm.add_cdw_config_duckdb_connection()
      return;
    }

		if(database in dbm.getPublications()) {
			this.#database = database;
		} else {
			this.#database = dbm.getFirstPublication(database.replace("_trexpg", ""));
			if(database.endsWith("_trexpg")){
				this.#database = this.#database+"_trexpg";
			}
		}
		
	}

	get database() {
		return this.#database;
	}

	execute(sql, params) {

		return new Promise((resolve, reject) => {
			try {
				const nparams= params.map(v => {
					if(typeof(v) === 'string' || v instanceof String) {
						try {
							const d = Date.parse(v);	
							if(/^\d\d\d\d-\d\d-\d\d/.test(v) && d) {
								return {"DateTime": d};
							}
						} catch (e) {}
						return {"String": v}

					}
					return {"Number": v};
				});
				//console.log(nparams);
				console.log(`DB: ${this.#database} SQL: ${sql}`);
				resolve(JSON.parse(op_execute_query(this.#database, sql, nparams)));
			} catch(e) {
				reject(e);
			}
		});
	}
	atlas_query(atlas) {

		return new Promise((resolve, reject) => {
			try {
				//console.log(nparams);
				console.log(`DB: ${this.#database} ATLAS: ${atlas}`);
				resolve({sql: op_atlas(this.#database, atlas)});
			} catch(e) {
				reject(e);
			}
		});
	}
}

export class PluginManager {
	#path;
	constructor(path) {
		this.#path = path;
	}

	install(pkg) {
		op_install_plugin(pkg, this.#path);
	}
}

