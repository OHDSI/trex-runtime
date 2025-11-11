import { core } from "ext:core/mod.js";
import { TrexConnection } from './dbconnection.js';

const ops = core.ops;

const CDW_DUCKDB_FILE_DATABASE_CODE = "cdw_config_svc";
const CDW_DUCKDB_FILE_SCHEMA_NAME = "validation_schema";
const CDW_BUILT_IN_DIR = "/usr/src/cdw_data/built_in";

const {
	op_prompt,
	op_prompt_next,
	op_add_replication,
	op_install_plugin,
	op_execute_query,
	op_exit,
	op_get_dbc,
	op_set_dbc,
	op_execute_query_stream,
	op_execute_query_stream_next,
	op_req,
	op_req_listen,
	op_req_next,
	op_req_respond
} = ops;

export { op_add_replication, op_exit };


function map_params(params) {
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
		return nparams;
	};

export async function executeQueryStream(database, sql, params = []) {
    const nparams = map_params(params);
    
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
		try {
			//const servers = JSON.parse(op_execute_query("memory", "SELECT * FROM pgwire_server_status()", []));
			//for (const server of servers) {
			//try {
			op_execute_query("memory", `SELECT update_db_credentials('${btoa(JSON.stringify(credentials))}')`, []);
			//} catch (e) {
			//	console.error(`Failed to update credentials for all servers`, e);
			//}
			//}
		} catch (e) {
			console.error("Failed to update database credentials:", e);
		}
		this.#updatePublications();
	}
	#setPublications(pub) {
		const dbc = JSON.parse(op_get_dbc());
		op_set_dbc(JSON.stringify({credentials: dbc.credentials, publications: pub}));

	}

	query(sql, params) {
		const nparams= map_params(params);
		return JSON.parse(op_execute_query("memory", sql, nparams));
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

	#add_duckdb(
		name
    ) {
		op_execute_query("memory",
        `ATTACH IF NOT EXISTS './data/cache/${name}.db' AS ${name}`, []
        );
	}



  add_cdw_config_duckdb_connection() {
    /*
		Connects to duckdb file in built in duckdb file in /usr/src/cdw_data/built_in
		*/
    const duckdb_file_path = `${CDW_BUILT_IN_DIR}/${CDW_DUCKDB_FILE_DATABASE_CODE}_${CDW_DUCKDB_FILE_SCHEMA_NAME}`;
    op_execute_query(
      "memory",
      `ATTACH IF NOT EXISTS '${duckdb_file_path}' AS ${CDW_DUCKDB_FILE_SCHEMA_NAME} (READ_ONLY)`,
      []
    );
  }


	#updatePublications() {
		for(const c of this.getCredentials()) {
			const adminCredentials = c.credentials.filter(c => c.userScope === 'Admin')[0];

			/*if(c.dialect == 'postgres' && c.publications && c.publications.length > 0 ) {
				console.log(`TREX PUB FOUND ${c.id}`)
				for(const p of c.publications) {
					const key = `${c.id}_${p.publication}`
					if(!(key in this.getPublications)) {
						op_add_replication(p.publication, p.slot, key, c.host, c.port, c.name, adminCredentials.username, adminCredentials.password);
						this.#add_postgres(`${key}_trexpg`, {host: c.host, port: c.port, databaseName: c.name, user: adminCredentials.username, password: adminCredentials.password});
						this.#add_postgres(`${key}__srcdb`, {host: c.host, port: c.port, databaseName: c.name, user: adminCredentials.username, password: adminCredentials.password});

						const pub = this.getPublications();
						pub[key] = true;
						this.#setPublications(pub);
					}
				}
			} else */
			if (/*c.vocab_schemas && c.vocab_schemas.length > 0 && */c.dialect == 'postgres') {
				console.log(`TREX NO PUB FOUND ${c.id}`)
				const key = `${c.id}`
				if(!(key in this.getPublications)) {
					this.#add_postgres(`${key}_trexpg`, {host: c.host, port: c.port, databaseName: c.name, user: adminCredentials.username, password: adminCredentials.password});
					this.#add_postgres(`${key}__srcdb`, {host: c.host, port: c.port, databaseName: c.name, user: adminCredentials.username, password: adminCredentials.password});
					const schemas = c.vocab_schemas.map(x => `'${x}'`).join(",");
					const res = JSON.parse(op_execute_query(`${key}_trexpg`,`select table_schema as schema,table_name as name from information_schema.tables where table_type = 'BASE TABLE' and table_schema in (${schemas})`, []));
					//op_copy_tables(res, key, c.host, c.port, c.name, adminCredentials.username, adminCredentials.password);
					const pub = this.getPublications();
					pub[key] = true;
					this.#setPublications(pub);
				}
			} else if (c.dialect == 'bigquery') {
				console.log(`TREX ADD BQ ${c.id}`)
				const key = `${c.id}`
				if(!(key in this.getPublications)) {
					this.#add_bigquery(`${key}__srcdb`, {project: c.host, dataset: c.name});
					const pub = this.getPublications();
					pub[key] = true;
					this.#setPublications(pub);
				}
			} else {
				console.log(`TREX DB NOT SUPPORTED ${c.id}`)
				continue;
			}
			this.#add_duckdb(`${c.id}`);
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


	getConnection(db_id, schema, vocab_schema, result_schema, translationMap) {
		const dbc = this.getDatabaseCredentials();
		let dialect = "duckdb";
		if (db_id != CDW_DUCKDB_FILE_DATABASE_CODE) {
			try {
				dialect = dbc.filter(c => c.id === db_id)[0].dialect;
			} catch (e) {
				console.error(`Error getting dialect for ${db_id}: ${e}`);
			}
		}
		if(dialect !== 'hana') {
			return new TrexConnection(new TrexDB(db_id), new TrexDB(`${db_id}`), schema,vocab_schema,result_schema,'duckdb',translationMap);
		} else {
			return new TrexConnection(new HanaDB(db_id), new HanaDB(`${db_id}`), schema,vocab_schema,result_schema,'hana',translationMap);
		}
	}
}



export class TrexDB {
	__database;
	constructor(database) {
		const dbm = DatabaseManager.getDatabaseManager();
		if (database === CDW_DUCKDB_FILE_DATABASE_CODE) {
      this.__database = CDW_DUCKDB_FILE_DATABASE_CODE;
			dbm.add_cdw_config_duckdb_connection()
      return;
    }

		if(database in dbm.getPublications()) {
			this.__database = database;
		} else {
			this.__database = dbm.getFirstPublication(database.replace("_trexpg", ""));
			if(database.endsWith("_trexpg")){
				this.__database = this.__database+"_trexpg";
			}
		}
		
	}

	getdatabase() {
		return this.__database;
	}


	executeWrite(sql, params) {
		return this.execute(sql, params);
	}

	execute(sql, params) {

		return new Promise((resolve, reject) => {
			try {
				const nparams = map_params(params);
				//console.log(nparams);
				console.log(`DB: ${this.__database} SQL: ${sql}`);
				resolve(JSON.parse(op_execute_query(this.__database, sql, nparams)));
			} catch(e) {
				reject(e);
			}
		});
	}

	atlas_query(atlas, cdmSchema, cohortId) {

		return new Promise((resolve, reject) => {
			try {
				const atlasStr = (typeof atlas === 'string') ? atlas : JSON.stringify(atlas);
				const toBase64 = (s) => {
					if (typeof Buffer !== 'undefined' && Buffer.from) {
						return Buffer.from(s, 'utf8').toString('base64');
					}
					const bytes = new TextEncoder().encode(s);
					let binary = '';
					for (const b of bytes) binary += String.fromCharCode(b);
					return btoa(binary);
				};
				const atlasB64 = toBase64(atlasStr);
				let query = `select circe_sql_translate(circe_json_to_sql('${atlasB64}' , '{"cdmSchema":"${cdmSchema}","resultSchema": "${cdmSchema}","targetTable":"cohort","cohortId":"${cohortId}","generateStats":true}'), 'duckdb') as sql`;
				const resultStr = op_execute_query(this.__database, query, []);
				const result = JSON.parse(resultStr);
				
				if (result && result.length > 0 && result[0]) {
					const sqlValue = Object.values(result[0])[0];
					resolve({sql: sqlValue});
				} else {
					resolve({sql: ''});
				}

			} catch(e) {
				reject(e);
			}
		});
	}
}

export class HanaDB extends TrexDB {
	constructor(database) {
		super(database);
	}
	executeWrite(sql, params) {
		return this.execute(sql, params);
	}

	execute(sql, params) {

		return new Promise((resolve, reject) => {
			try {
				const nparams= map_params(params);
				console.log(`DB: ${super.getdatabase()} SQL: ${sql}`);
				const dbm = DatabaseManager.getDatabaseManager();
				const c = dbm.getCredentials().filter(c => c.id === super.getdatabase())[0]
				const adminCredentials = c.credentials.filter(c => c.userScope === 'Admin')[0];
				resolve(JSON.parse(op_execute_query(super.getdatabase(), `select * from hana_scan('${sql}', 'hdbsql://${adminCredentials.username}:${adminCredentials.password}@${c.host}:${c.port}/${c.name}')`, nparams)));
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

export async function req(service, urlOrRequest, options = {}) {
	let request;
	
	if (urlOrRequest instanceof Request) {
		const headers = {};
		for (const [key, value] of urlOrRequest.headers) {
			headers[key] = value;
		}
		
		request = {
			url: urlOrRequest.url,
			method: urlOrRequest.method,
			headers: headers
		};
		
		if (urlOrRequest.body) {
			request.body = await urlOrRequest.text();
		}
	} else {
		request = {
			url: urlOrRequest,
			method: options.method || 'GET',
			headers: options.headers || {}
		};
		
		if (options.body !== undefined) {
			request.body = options.body;
		}
	}

	try {
		const messageToSend = {service: service, request: request};
		const httpResponse = await op_req(messageToSend);
		
		if (httpResponse && typeof httpResponse === 'object' && httpResponse.status && httpResponse.body !== undefined) {
			const response = new Response(httpResponse.body, {
				status: httpResponse.status,
				statusText: httpResponse.statusText,
				headers: httpResponse.headers
			});
			return response;
		}
		
		return httpResponse;
	} catch (error) {
		return {
			ok: false,
			status: 500,
			statusText: 'Internal Server Error',
			headers: {},
			body: { error: error.message }
		};
	}
}

export function reqRespond(requestId, response) {
	return op_req_respond(requestId, response);
}

export function createRequestListener(onMessage) {
	
	return new ReadableStream({
		async start(controller) {
			try {
				const listenerId = await op_req_listen();
				while (true) {
					const message = await op_req_next(listenerId);
					if (message === null) {
						controller.close();
						break;
					}
					
					if (onMessage && typeof onMessage === 'function') {
						const requestId = message.id;
						const originalMessage = message.message;
						if (!originalMessage || !originalMessage.request) {
							continue;
						}
						
						const requestOptions = {
							method: originalMessage.request.method,
							headers: originalMessage.request.headers
						};
						
						if (originalMessage.request.body !== undefined && 
							originalMessage.request.body !== null &&
							originalMessage.request.method !== 'GET' && 
							originalMessage.request.method !== 'HEAD') {
							requestOptions.body = originalMessage.request.body;
						}
						
						const request = new Request(originalMessage.request.url, requestOptions);
						
						const kTokioChannelTag = Symbol.for("kTokioChannelTag");
						request[kTokioChannelTag] = {
							type: "tokio_channel",
							watcherRid: -1,
							streamRid: listenerId,
							channelRid: listenerId
						};
						
						const respond = (response) => op_req_respond(requestId, response);
						
						onMessage({ 
							service: originalMessage.service,
							request: request  // Request object with special tokio channel tag
						}, respond);
					}
					
					controller.enqueue(message);
				}
			} catch (error) {
				console.error("Request listener error:", error);
				controller.error(error);
			}
		}
	});
}

export class TrexHttpClient {
	constructor(service) {
		this.service = service;
	}

	async request(config) {
		const url = config.url || '/';
		const options = {
			method: config.method || 'GET',
			headers: config.headers || {},
			body: config.data || config.body
		};

		if (options.body && typeof options.body === 'object' && !(options.body instanceof FormData)) {
			if (!options.headers['Content-Type']) {
				options.headers['Content-Type'] = 'application/json';
				options.body = JSON.stringify(options.body);
			}
		}

		const response = await req(this.service, url, options);
		
		if (response instanceof Response) {
			try {
				const data = await response.json();

				return {
					data: data,
					status: response.status,
					statusText: response.statusText,
					headers: Object.fromEntries(response.headers.entries()),
					config: config,
					request: new Request(url, options)
				};
			} catch (error) {
				const textData = await response.text().catch(() => "");
				
				return {
					data: {},
					status: response.status,
					statusText: response.statusText,
					headers: Object.fromEntries(response.headers.entries()),
					config: config,
					request: new Request(url, options)
				};
			}
		}
		return response;
	}

	async get(url, config = {}) {
		return await this.request({ ...config, method: 'GET', url });
	}

	async post(url, data, config = {}) {
		return await this.request({ ...config, method: 'POST', url, data });
	}

	async put(url, data, config = {}) {
		return await this.request({ ...config, method: 'PUT', url, data });
	}

	async patch(url, data, config = {}) {
		return await this.request({ ...config, method: 'PATCH', url, data });
	}

	async delete(url, config = {}) {
		return await this.request({ ...config, method: 'DELETE', url });
	}
}

