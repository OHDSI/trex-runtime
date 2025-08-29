


import { writeFileSync, readFileSync, existsSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';


function  test_dbquery5() {
        console.log("USER WORKER Example");
        const dbm = Trex.databaseManager();
        console.log(dbm.getDatabases())
        console.log(dbm.getDatabaseCredentials())
        const conn = dbm.getConnection('demo_database', 'demo_cdm', "demo_cdm", {"duckdb": 
        (sql:string,
            schemaName:string,
            vocabSchemaName:string,
            parameters:any) => {
                //translate hana sql to duckdb sql
            return sql;
        }})

        const res = conn.execute("select count(1) from $$SCHEMA$$.person where person_id < ?",[{value:4000}], ((err:any,res:any) => {
            console.log(res);
            console.log(err);

        }));
        //res.then((r) => console.log(r)).catch((e) => console.error(e));
    
}

test_dbquery5()

function  test_atlas() {
        console.log("ATLAS USER WORKER Example");
        const dbm = Trex.databaseManager();
        console.log(dbm.getDatabases())
        console.log(dbm.getDatabaseCredentials())
        const conn = dbm.getConnection('demo_database', 'demo_cdm', "demo_cdm", {"duckdb": 
        (sql:string,
            schemaName:string,
            vocabSchemaName:string,
            parameters:any) => {
                //translate hana sql to duckdb sql
            return sql;
        }})

        // Use the exact JSON structure from CIRCE Rust tests - clean format
        const cohortJson = {
            "title": "Complete Test Cohort",
            "primaryCriteria": {
                "criteriaList": [{
                    "ConditionOccurrence": {
                        "CodesetId": 1,
                        "First": true,
                        "OccurrenceStartDate": {
                            "Value": "2020-01-01",
                            "Op": "gte"
                        }
                    }
                }],
                "observationWindow": {
                    "priorDays": 365,
                    "postDays": 0
                },
                "primaryLimit": {
                    "type": "First"
                }
            },
            "conceptSets": [{
                "id": 1,
                "name": "Diabetes Condition Set",
                "expression": {
                    "items": [{
                        "concept": {
                            "conceptId": 201826,
                            "conceptName": "Type 2 diabetes mellitus",
                            "standardConcept": "S",
                            "invalidReason": "V",
                            "conceptCode": "E11",
                            "domainId": "Condition",
                            "vocabularyId": "ICD10CM",
                            "conceptClassId": "3-char billing code"
                        },
                        "isExcluded": false,
                        "includeDescendants": true,
                        "includeMapped": false
                    }]
                }
            }],
            "qualifiedLimit": {"type": "First"},
            "expressionLimit": {"type": "First"},
            "inclusionRules": [],
            "collapseSettings": {"collapseType": "ERA", "eraPad": 0}
        };
        
        const res = conn.atlas(JSON.stringify(cohortJson), ((err:any,res:any) => {
            console.log("Result:", res);
            console.log("Error:", err);
            
            // If there's an error, it might be a JSON parsing issue
            if (err && err.message && err.message.includes("JSON")) {
                console.log("JSON parsing error detected. This suggests the CIRCE result contains invalid JSON or control characters.");
            }
        })); 
        //res.then((r) => console.log(r)).catch((e) => console.error(e));
    
}

test_atlas()

function test_writeFileSync() {
    console.log("Testing Node.js writeFileSync functionality");
    
    try {
        const testData = "Hello World from Node.js writeFileSync test!\nTimestamp: " + new Date().toISOString();
        const testFilePath = join(".", "trex_writefilesync_test.txt");
        
        // Test writing a file using Node.js writeFileSync
        writeFileSync(testFilePath, testData, 'utf8');
        console.log(`Successfully wrote test data to ${testFilePath}`);
        
        // Test reading it back to verify it was written correctly
        const readData = readFileSync(testFilePath, 'utf8');
        console.log("Read back data:", readData);
        
        // Verify the data matches
        if (readData === testData) {
            console.log("✅ writeFileSync test PASSED - data matches!");
        } else {
            console.log("❌ writeFileSync test FAILED - data mismatch!");
            console.log("Expected:", testData);
            console.log("Got:", readData);
        }
        
        // Clean up test file
        try {
            unlinkSync(testFilePath);
            console.log("✅ Cleaned up test file");
        } catch (cleanupErr) {
            console.log("Note: Could not clean up test file:", cleanupErr);
        }
        
    } catch (error) {
        console.error("❌ writeFileSync test FAILED with error:", error);
    }
}

// Call the test function
test_writeFileSync();
