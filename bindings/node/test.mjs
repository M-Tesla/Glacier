import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";

const require = createRequire(import.meta.url);
const glacier = require("./index.js");
const here = dirname(fileURLToPath(import.meta.url));
const sales = join(here, "../../tests/formats/sales.parquet");

assert.equal(glacier.version(), "0.2.1");
assert.equal(glacier.apiVersion(), 1);

const empty = glacier.connect();
assert.deepEqual(empty.execute("select 1"), [[1]]);
empty.close();

const con = glacier.connect(sales);
assert.deepEqual(con.execute("SELECT COUNT(*)"), [[10]]);
const buf = readFileSync(sales);
assert.equal(con.readParquet(buf).length, 10);

let threw = false;
try {
  glacier.connect().execute("SELECT * FROM a JOIN b");
} catch (e) {
  threw = String(e).includes("JOIN is not supported");
}
assert.equal(threw, true);
con.close();
console.log("node napi ok");
