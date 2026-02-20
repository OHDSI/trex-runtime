import { Client } from "https://deno.land/x/postgres@v0.17.0/mod.ts";
const client = new Client({
  user: "user",
  password: "pencil",
  database: "test",
  hostname: "127.0.0.1",
  port: 5432,
});
await client.connect();
let result
try {
result = await client.queryArray("CREATE TABLE Persons (PersonID int,City varchar(255))");
} catch (e) {}
//console.log(result.rows); // [[1, 'Carlos'], [2, 'John'], ...]
result = await client.queryArray("insert into persons values (12,'asd')");
console.log(result.rows); // [[1, 'Carlos'], [2, 'John'], ...]
result = await client.queryArray("select count(1) from demo_cdm.person");
console.log(result.rows); // [[1, 'Carlos'], [2, 'John'], ...]
result = await client.queryArray("select * from information_schema.tables");
console.log(result.rows); // [[1, 'Carlos'], [2, 'John'], ...]
await client.end();