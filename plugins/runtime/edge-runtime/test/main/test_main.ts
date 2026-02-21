import { assertEquals } from "jsr:@std/assert";
import { expect } from "jsr:@std/expect";

function runtest(name:string, fn:any, delay:number) {
    setTimeout(async () => {
        console.log(`TEST ${name}`);
        await fn();
        console.log(`TEST ${name} done`)
    }, delay)
}

async function test_installPlugin() {
    const plugin = new Trex.PluginManager("./test/_tmpplugin");
    plugin.install("express");
}


async function  test_dbquery() {
        const conn = new Trex.TrexDB("demo_database");
        try {
            const res = await conn.execute("select count (1) from demo_cdm.person where person_id < ?", [10]);
            console.log(res)
        } catch(e) {
            console.error(e)
        }
}



async function  test_dbquery2() {
 
        const conn = new Trex.TrexDB("demo_database");
        try {
        const res = await conn.execute("select count (1) from demo_cdm.person where birth_datetime < ?", ["2000-01-01"]);
        console.log(res)
    } catch(e) {
        console.error(e)
    }
    
}

async function  test_dbquery3() {

        const conn = new Trex.TrexDB("demo_database");
        try {
        const res = await conn.execute("select count (1) from demo_cdm.person where race_source_value = ?", ["white"]);
        console.log(res)
    } catch(e) {
        console.error(e)
    }
    
}

async function  test_dbquery4() {
   
        const conn = new Trex.TrexDB("demo_database");
        try {
        let res = await conn.execute("insert into demo_cdm.person (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [31337, 0, 0, 0, 0, "1990-01-01", 0, 0, 0, 0, 0, '', '', 0, '', 0, '', 0]);
       //let res = await conn.execute( "insert into demo_cdm.person (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id) VALUES (31339, 0, 0, 0, 0, '1990-01-01', 0, 0, 0, 0, 0, '', '', 0, '', 0, '', 0)", []);
       
       console.log(res);
        res = await conn.execute("delete from demo_cdm.person where person_id > ?", [10000]);
        console.log(res)
    } catch(e) {
        console.error(e) 
    }

    
}


async function  test_dbquery5() {
   
        const dbm = Trex.userDatabaseManager();
        const conn = dbm.getConnection('demo_database', 'demo_cdm', "demo_cdm", "demo_cdm", {"duckdb": (n:any) => n})

        const res = conn.execute("select count(1) from $$SCHEMA$$.person where person_id < ?",[{value:4000}], ((err:any,res:any) => {
            console.log(res);
            console.log(err);
            //assertEquals(res[0]["count(1)"], 2048);

        }));

    
}

async function  test_dbquery6() {
 
    const conn = new Trex.TrexDB("demo_database");
    try {
    const res = await conn.execute("select current_date", []);
    console.log(res)
} catch(e) {
    console.error(e)
}

}

const init_tests = {
    "install plugin": test_installPlugin,
    "init credentials": () => {
        const dbm = Trex.DatabaseManager.getDatabaseManager();
        const c = [
            {
                "id": "demo_database",
                "host": "localhost",
                "port": 65432,
                "code": "demo_database",
                "name": "postgres",
                "dialect": "postgres",
                "credentials": [
                    {
                        "username": "postgres",
                        "userScope": "Admin",
                        "serviceScope": "Internal",
                        "password": "mypass"
                    },
                    {
                        "username": "postgres",
                        "userScope": "Read",
                        "serviceScope": "Internal",
                        "password": "mypass"
                    }
                ],
                "extra": [
                    {
                        "value": {
                            "max": 50,
                            "queryTimeout": 60000,
                            "statementTimeout": 60000,
                            "idleTimeoutMillis": 300000,
                            "idleInTransactionSessionTimeout": 300000
                        },
                        "serviceScope": "Internal"
                    }
                ],
                "vocab_schemas": [
                    "demo_cdm"
                ]
            }
        ];
        dbm.setCredentials(c);
    }
}

const tests = {
    "dbquery #1": test_dbquery,
    "dbquery #2": test_dbquery2,
    "dbquery #3": test_dbquery3,
    "dbquery #4": test_dbquery4,
    "dbquery #5 (analytics interface)": test_dbquery5,
    "dbquery #6 (pg conn)": async () => {
        const dbm = Trex.userDatabaseManager();
        const conn = dbm.getConnection('demo_database', 'demo_cdm', "demo_cdm", "demo_cdm", {"duckdb": (n:any) => n})
        const res = conn.execute_write("select count(1) from $$SCHEMA$$.person where person_id < ?",[{value:4000}], ((err:any,res:any) => {

   // const res = conn.executeUpdate("insert into demo_cdm.person (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id) VALUES (31337, 0, 0, 0, 0, '1990-01-01', 0, 0, 0, 0, 0, '', '', 0, '', 0, '', 0)", [], ((err:any,res:any) => {
        console.log(res);
        console.log(err);
        assertEquals(res[0]["count(1)"], 2048);

    }));
    

    },
    "dbquery #7 (pg conn insert)": async () => {
        try {
            const connx = new Trex.TrexDB("demo_database_trexpg");

            let resx = await connx.execute("delete from demo_cdm.person where person_id > ?", [10000]);
            console.log(resx);
            const dbm = Trex.userDatabaseManager();
            const conn = dbm.getConnection('demo_database', 'demo_cdm', "demo_cdm", "demo_cdm", {"duckdb": (n:any) => n})
            //const res = conn.execute_write("select count(1) from $$SCHEMA$$.person where person_id < ?",[{value:4000}], ((err:any,res:any) => {

            const res = conn.executeUpdate(
                "insert into demo_cdm.person (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id, person_source_value, gender_source_value, gender_source_concept_id, race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id) VALUES (31339, 0, 0, 0, 0, '1990-01-01', 0, 0, 0, 0, 0, '', '', 0, '', 0, '', 0)", 
                //"select count(1) from $$SCHEMA$$.person where person_id > 10000",
                [],
                async (err:any,res:any) => {
                    
                    console.log(res);
                    console.log(err);
                    resx = await connx.execute("delete from demo_cdm.person where person_id > ?", [10000]);
                    console.log(resx);
            // assertEquals(res[0]["count(1)"], 2048);

                }
            );
        } catch (e) {
            console.error(e);
        }
    },
    "ask": async () => {

       /* const stream = await Trex.ask(`write a python program to get the repos from github`);
        const reader = stream.getReader();


        let res = ""
        while (true) {
            const { done, value } = await reader.read();
            if (done) {
                break;
            }
   
            Deno.stdout.write(new TextEncoder().encode(value));
            res += value;
            
        } 
        console.log("Answer:"+res)*/
    },
    "dbquery #6": test_dbquery6,
    "dbquery json": async () => {
        const dbm = Trex.userDatabaseManager();
        const conn = dbm.getConnection('demo_database', 'demo_cdm', "demo_cdm", "demo_cdm", {"duckdb": (n:any) => n})
        const res1 = conn.execute("create table if not exists test (id number primary key, test json)",[], ((err:any,res:any) => {
            console.log(err);
            if(!err)
            conn.execute("insert into test values (0, '{\"x\": {\"id\": \"a\"}}')",[], ((err:any,res:any) => {
                console.log(err);
                if(!err)
                conn.execute("select now(), test->'$.x' from test",[], ((err:any,res:any) => {
                    console.log(res);
                    console.log(err);
                      }));
            }));
        }));
    },
    "dbquery stream": async () => {
        const dbm = Trex.userDatabaseManager();
        const conn = dbm.getConnection('demo_database', 'demo_cdm', "demo_cdm", "demo_cdm", {"duckdb": (n:any) => n})
        
        conn.executeStreamQuery(
            "select person_id, gender_concept_id, year_of_birth from $$SCHEMA$$.person where person_id < ? order by person_id",
            [{value: 10}],
            async (err: any, stream: ReadableStream) => {
                if (err) {
                    console.error("Stream query error:", err);
                    return;
                }
                
                console.log("Stream query started successfully");
                
                try {
                    const reader = stream.getReader();
                    let chunkCount = 0;
                    let totalData = "";
                    
                    while (true) {
                        const { done, value } = await reader.read();
                        if (done) {
                            console.log(`Stream completed. Received ${chunkCount} chunks`);
                            break;
                        }
                        
                        chunkCount++;
                        totalData += value;
                        console.log(`Chunk ${chunkCount}: ${value.substring(0, 100)}...`);
                    }
                    
                    // Try to parse the complete data as JSON
                    try {
                        const jsonData = JSON.parse(totalData);
                        console.log(`Successfully parsed JSON with ${jsonData.length} records`);
                        if (jsonData.length > 0) {
                            console.log("First record:", jsonData[0]);
                        }
                    } catch (parseErr) {
                        console.log("Could not parse as single JSON, likely chunked data");
                        console.log("Total data length:", totalData.length);
                    }
                    
                } catch (streamErr) {
                    console.error("Error reading stream:", streamErr);
                }
            }
        );
    },

}

export function test() {
    console.log("TEST main");

    for (const [key, value] of Object.entries(init_tests)) {
        runtest(key, value, 0);
    }
    for (const [key, value] of Object.entries(tests)) {
        runtest(key, value, 3000);
    }
}