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
    dialect;
    translatefn;

     constructor(
       // conn,
       // database,
        conn,
        writeConn,
        schemaName,
        vocabSchemaName,
        dialect,
        translatefn, 
    ) {
        this.connection = conn
        this.writeConn = writeConn
        this.schemaName = schemaName;
        this.vocabSchemaName = vocabSchemaName;
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
        callback
    ) {
        try {
            const result = await this.connection.atlas_query(
                atlas
            );
            callback(null, result);
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
                }
                callback(null, result);
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
