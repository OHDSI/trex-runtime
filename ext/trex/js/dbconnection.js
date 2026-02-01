import { executeQueryStream } from './trex_lib.js';

const NOVALUE = "NoValue"

function flattenParameter(parameters) {
    try {
    const flatList = [];
    if (parameters) {
        parameters.forEach((p) => {
            flatList.push(p.value === undefined ? null : p.value);
        });
    }
    return flatList;

    } catch(e) {
        console.log("Error in flattenParameter")
        console.log(e)
    }
}

export class TrexConnection  {

    connection;
    writeConn;
    schema;
    vocabSchemaName;
    resultSchemaName;
    dialect;
    translatefn;

     constructor(
       // conn,
       // database,
        conn,
        writeConn,
        schemaName,
        vocabSchemaName,
        resultSchemaName,
        dialect,
        translatefn, 
    ) {
        this.connection = conn
        this.writeConn = writeConn
        this.schemaName = schemaName;
        this.vocabSchemaName = vocabSchemaName;
        this.resultSchemaName = resultSchemaName;
        this.dialect = dialect;
        this.translatefn = translatefn[dialect];
    }


    parseResults(result) {
        function formatResult(value) {
            // TODO: investigate if more cases are needed to handle DATE, TIMESTAMP and BIT datetypes
            switch (typeof value) {
                case "bigint": //bigint
                    return Number(value) * 1;
                default:
                    return value;
            }
        }
        Object.keys(result).forEach((rowId) => {
            Object.keys(result[rowId]).forEach((colKey) => {
                if (
                    result[rowId][colKey] === null ||
                    typeof result[rowId][colKey] === "undefined"
                ) {
                    result[rowId][colKey] = NOVALUE;
                } else {
                    result[rowId][colKey] = formatResult(result[rowId][colKey]);
                }
            });
        });
        return result;
    }

    async atlas(
        atlas,
        cdmSchema,
        cohortId,
        callback
    ) {
        try {
            // Convert atlas to JSON string if it's an object
            const atlasStr = (typeof atlas === 'string') ? atlas : JSON.stringify(atlas);

            // Convert to base64
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

            // Build options JSON with schema information
            // Use the schema names from the connection configuration
            const resultSchema = this.resultSchemaName || this.schemaName;

            // cohortId must be an integer, not a string
            const cohortIdInt = parseInt(cohortId, 10);
            if (Number.isNaN(cohortIdInt)) {
                callback(new Error("Invalid cohortId: expected an integer"), null);
                return;
            }

            // Build options as object and stringify to ensure proper JSON escaping
            const optionsObj = {
                cdmSchema: cdmSchema,
                resultSchema: resultSchema,
                targetTable: "cohort",
                cohortId: cohortIdInt,
                generateStats: true
            };
            const options = JSON.stringify(optionsObj).replace(/'/g, "''");

            // Use circe_sql_render_translate to get properly rendered and translated SQL for DuckDB dialect
            // Third parameter is additional render options (empty object)
            const sql = `SELECT circe_sql_render_translate(circe_json_to_sql('${atlasB64}', '${options}'), 'duckdb', '{}') AS sql`;
            const result = await this.connection.execute(sql, []);

            // Extract the generated SQL from the result and return in same format as old atlas_query
            if (result && result.length > 0 && result[0].sql) {
                callback(null, {sql: result[0].sql});
            } else {
                callback(new Error("No SQL generated from cohort definition"), null);
            }
        } catch (err) {
            console.error(err);
            callback(new Error(err.message), null);
        }
    }

    async atlas_validate(
        cohortDefinition,
        callback
    ) {
        try {
            // Convert cohort definition to JSON string if it's an object
            const cohortStr = (typeof cohortDefinition === 'string') ? cohortDefinition : JSON.stringify(cohortDefinition);

            // Convert to base64
            const toBase64 = (s) => {
                if (typeof Buffer !== 'undefined' && Buffer.from) {
                    return Buffer.from(s, 'utf8').toString('base64');
                }
                const bytes = new TextEncoder().encode(s);
                let binary = '';
                for (const b of bytes) binary += String.fromCharCode(b);
                return btoa(binary);
            };
            const cohortDefinitionBase64 = toBase64(cohortStr);

            // Execute circe_check_cohort function to validate the cohort definition
            const sql = `SELECT circe_check_cohort('${cohortDefinitionBase64}') AS warnings`;
            const result = await this.connection.execute(sql, []);

            // Parse the JSON warnings from the result
            if (result && result.length > 0 && result[0].warnings) {
                const warnings = JSON.parse(result[0].warnings);
                // Extract just the messages from the warnings array
                const messages = warnings.map(w => ({
                    severity: w.severity,
                    message: w.message
                }));
                callback(null, messages);
            } else {
                // No warnings - return empty array
                callback(null, []);
            }
        } catch (err) {
            console.error(err);
            callback(new Error(err.message), null);
        }
    }

    async execute(
        sql,
        parameters,
        callback
    ) {
        try {
            console.log(`Sql: ${sql}`);
            console.log(
                `parameters: ${JSON.stringify(flattenParameter(parameters))}`
            );
            let temp = sql;
            temp = this.#parseSql(temp, parameters);
            console.log("Duckdb client created");
            console.log(temp);
            const result = await this.connection.execute(
                temp, flattenParameter(parameters)
            );
            callback(null, result);
        } catch (err) {
            console.error(err);
            callback(new Error(console.error(err), err.message), null);
        }
    }

    async execute_write(
        sql,
        parameters,
        callback
    ) {
        try {
            console.log(`Sql: ${sql}`);
            console.log(
                `parameters: ${JSON.stringify(flattenParameter(parameters))}`
            );
            let temp = sql;
            temp = this.#parseSql(temp, parameters);
            console.log("Duckdb client created");
            console.log(temp);
            const result = await this.writeConn.executeWrite(
                temp, flattenParameter(parameters)
            );
            callback(null, result);
        } catch (err) {
            console.error(err);
            callback(new Error(console.error(err), err.message), null);
        }
    }

     #parseSql(temp, parameters) {
        temp = this.#getSqlStatementWithSchemaName(this.schemaName, temp); //THIS HAS TO COME BEFORE
        return this.translatefn(
            temp,
            this.schemaName,
            this.vocabSchemaName,
            this.resultSchemaName,
            parameters
        );
    }

    getTranslatedSql(sql, schemaName, parameters) {
        return this.#parseSql(sql, parameters);
    }

    executeQuery(
        sql,
        parameters,
        callback
    ) {
        try {
            this.execute(sql, parameters, (err, resultSet) => {
                if (err) {
                    console.error(err);
                    callback(err, null);
                } else {
                    const result = this.parseResults(resultSet);
                    callback(null, result);
                }
            });
        } catch (err) {
            callback(new Error(console.error(err), err.message), null);
        }
    }

    executeStreamQuery(
        sql,
        parameters,
        callback,
        schemaName = ""
    ) {
        try {
            console.log(`Stream Sql: ${sql}`);
            console.log(
                `Stream parameters: ${JSON.stringify(flattenParameter(parameters))}`
            );
            let temp = sql;
            temp = this.#parseSql(temp, parameters);
            console.log("Duckdb client created for streaming");
            console.log(temp);

            executeQueryStream(this.connection.__database, temp, flattenParameter(parameters))
                .then(stream => {
                    callback(null, stream);
                })
                .catch(err => {
                    console.error(err);
                    callback(new Error(err.message), null);
                });
        } catch (err) {
            console.error(err);
            callback(new Error(err.message), null);
        }
    }

    executeUpdate(
        sql,
        parameters,
        callback
    ) {
        try {
            this.execute_write(sql, parameters, (err, result) => {
                if (err) {
                    console.error(err)
                    callback(err, null);
                } else {
                    callback(null, result);
                }
            });
        } catch (error) {
            callback(error, null);
        }
    }

    executeProc(
        procedure,
        parameters,
        callback
    ) {
        throw new Error("executeProc is not yet implemented");
    }

    commit(callback) {
        /*this.conn.exec("COMMIT", (commitError) => {
            if (commitError) {
                throw commitError;
            }
            if (callback) {
                callback(null, null);
            }
        });*/
        throw new Error("commit is not yet implemented");

    }

    setAutoCommitToFalse() {
        throw new Error("setAutoCommitToFalse is not yet implemented");
    }

   

   

    async close() {
        //await this.database.close();
        console.log(`Duckdb database connection has been closed`);
    }

    executeBulkUpdate(
        sql,
        parameters,
        callback
    ) {
        throw "executeBulkUpdate is not yet implemented";
    }

    executeBulkInsert(
        sql,
        parameters,
        callback
    ) {
        throw "executeBulkInsert is not yet implemented";
    }

    setCurrentUserToDbSession(
        user,
        callback
    ) {
        callback(null, null);
    }

    setTemporalSystemTimeToDbSession(
        systemTime,
        cb
    ) {
        cb(null, null);
    }

    rollback(callback) {
        throw "rollback is not yet implemented";
    }

     #getSqlStatementWithSchemaName(
        schemaName,
        sql
    ) {
        /*let duckdbNativeSchemName = null;

        //TODO: Unify implementation between patient list and Add to cohort
        if (this.conn["duckdbNativeDBName"]) {
            duckdbNativeSchemName = `${this.conn["duckdbNativeDBName"]}.${this.conn["studyAnalyticsCredential"].schema}`;
        } else {
            duckdbNativeSchemName = this["duckdbNativeDBName"];
        }
        //If inner join is happening between duckdb and native database for ex: postgres, then the replaced example would be <ALIAS_NATIVE_DBNAME>.<SCHEMANAME>.COHORT
        if (duckdbNativeSchemName) {
            sql = sql.replace(
                /\$\$SCHEMA\$\$.COHORT_DEFINITION/g,
                `${duckdbNativeSchemName}.COHORT_DEFINITION`
            );
            sql = sql.replace(
                /\$\$SCHEMA\$\$.COHORT/g,
                `${duckdbNativeSchemName}.COHORT`
            );
        }*/
        const replacement = schemaName === "" ? "" : `${schemaName}.`;
        return sql.replace(/\$\$SCHEMA\$\$./g, replacement);
    }
}
