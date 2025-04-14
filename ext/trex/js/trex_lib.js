import { core } from "ext:core/mod.js";
import { TrexConnection } from './pgconnection.js';
import { HanaConnection } from './hdbconnection.js';

//import * as hdb from './hdb.js';
//import * as p from './postgres.js';

const ops = core.ops;

const {
	op_prompt,
	op_prompt_next,
	op_add_replication,
	op_install_plugin,
	op_execute_query,
	op_exit,
	op_get_dbc,
	op_set_dbc
} = ops;

export { op_add_replication, op_exit };

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
        `ATTACH 'host=${credentials.host} port=${credentials.port} dbname=${credentials.databaseName} user=${credentials.user} password=${credentials.password}' AS ${name} (TYPE postgres)`, []
        );
    }

	#updatePublications() {
		for(const c of this.getCredentials()) {
			if(c.publications) {
				const adminCredentials = c.credentials.filter(c => c.userScope === 'Admin')[0];
				for(const p of c.publications) {
					const key = `${p.publication}`
					if(!(key in this.getPublications)) {
						op_add_replication(p.publication, p.slot, key, c.host, c.port, c.name, adminCredentials.username, adminCredentials.password);
						//this.#add_postgres(`${key}_pg`, {host: c.host, port: c.port, databaseName: c.name, user: adminCredentials.username, password: adminCredentials.password});
						const pub = this.getPublications();
						pub[key] = true;
						this.#setPublications(pub);
					}
				}
			} 
		}
	}

	getFirstPublication(db_id) {
		const tmp =  this.getCredentials().filter(c => c.id === db_id)[0].publications[0]
		return `${tmp.publication}`
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


	getConnection(db_id, schema, vocab_schema, translationMap) {
		return new TrexConnection(new TrexDB(db_id), new TrexDB(`${db_id}_pg`), schema,vocab_schema,translationMap);
	}
}



export class TrexDB {
	#database;
	constructor(database) {
		const dbm = DatabaseManager.getDatabaseManager();
		if(database in dbm.getPublications()) {
			this.#database = database;
		} else {
			this.#database = dbm.getFirstPublication(database.replace("_pg", ""));
			if(database.endsWith("_pg")){
				this.#database = this.#database+"_pg";
			}
		}
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

