import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Glacier } from "./glacier.js";

const here = dirname(fileURLToPath(import.meta.url));
const wasmPath = join(here, "../../zig-out/bin/glacier.wasm");
const parquetPath = join(here, "../../tests/formats/sales.parquet");

const wasm = readFileSync(wasmPath);
const g = await Glacier.instantiate(wasm);
if (g.version() !== "0.6.0") throw new Error(`version ${g.version()}`);

const empty = g.openEmpty();
const r1 = g.query(empty, "select 1");
g.destroyResult(r1);
g.close(empty);

const parquet = readFileSync(parquetPath);
const db = g.openBuffer(parquet);
const r2 = g.query(db, "SELECT COUNT(*)");
g.destroyResult(r2);
g.close(db);

console.log("wasm ok", g.version(), `${wasm.length} bytes`);
