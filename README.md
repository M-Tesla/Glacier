![Logo](img/GlacierLogo.png)

# <div align="center">Glacier</div>

<div align="center">

[![Zig Version](https://img.shields.io/badge/zig-0.16.0-orange.svg)](https://ziglang.org/)
[![CI](https://github.com/M-Tesla/Glacier/actions/workflows/ci.yml/badge.svg)](https://github.com/M-Tesla/Glacier/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-0.7-blue.svg)]()
[![Version](https://img.shields.io/badge/version-0.7.0-blue.svg)]()

</div>

Glacier is an OLAP engine. A connection is a session of named catalogs (a Parquet/Avro file, an Iceberg Hadoop directory, a native Glacier warehouse, or an Iceberg REST Catalog). SQL talks `catalog.namespace.table`. You can run locally, over `https://`, on `s3://`, on `gs://`, or on `az://`. `glacier serve` is the process: Iceberg REST for other engines, Arrow Flight SQL for query. Results stream as record batches (Arrow C Data on the public ABI; Flight `DoGet` sends one FlightData per batch).

The query engine is Zig 0.16. Parquet, Avro, and in-memory Arrow are small C libraries (carquet, libavro, nanoarrow), not a from-scratch codec stack.

`glacier.version()` is **0.7.0**. That is the product number. `glacier.api_version()` is **2**: a query string may hold several statements, and the call returns the last result. A file path remains a one-catalog shortcut. 0.3 catalog session, 0.4 writes through the catalog, 0.5 `glacier serve`, 0.6 streaming, and 0.7 edge identity are this release. Linux `glacier-ui` is the same number. It was built and tested on **Linux**. The commands in this README are bash (`export`, `$ZIG`, `./zig-out/bin/glacier`, `libglacier.so`). They are not a Windows playbook; cmd/PowerShell, `.exe` / `.dll`, and paths will differ, and that path was not used here.

CI in this repo: `zig build test` on Linux and macOS; on Windows only `zig build` (compile, no test suite). Python tests and manylinux wheels (`manylinux_2_28` x86_64 and aarch64) run on Ubuntu. WASM on Ubuntu.

---

## Versions

Shipped rows are what `glacier.version()` has already printed. 0.7.0 is this release.

| Number | Name | What is true |
| --- | --- | --- |
| **0.2.1** | Engine | Path to SQL to Arrow. Analysis SQL on Iceberg, Parquet, Avro. Linux wheel and Kof. |
| **0.3.x** | Catalog session | Named catalogs. `ATTACH` / `SHOW` / `USE` / `DETACH`. `catalog.namespace.table`. JOIN across catalogs. Iceberg is the table format, not the catalog. |
| **0.4.x** | Writes through the catalog | `CREATE` / `INSERT` / `CTAS` / `COPY FROM` / `DELETE` / `UPDATE` / `MERGE` / `ALTER ADD COLUMN` / `DROP`. Iceberg snapshots. Native `glacier` registry, Hadoop, REST `commitTable`. |
| **0.5.x** | Process | `glacier serve`: Iceberg REST (catalog port) and Arrow Flight SQL (query port). Two clients, same warehouse. Not in WASM or the wheel. |
| **0.6.x** | Streaming | Record batches on Flight `DoGet` and `Result.nextBatch`. `GLACIER_BATCH_ROWS` (default 65536). `glacier_result_arrow` still exports every row. `CREATE TEMP TABLE` lives in the session. A running statement stops on `glacier_cancel`, Ctrl+C in the CLI, or `GLACIER_QUERY_MS`. A query string may hold several statements; the call returns the last result. Local files of one table scan together (`GLACIER_THREADS`). An `INSERT` stores column bounds, and the next snapshot keeps them. `COMPACT TABLE` rewrites live rows into new data files; previous snapshots stay. `EXPIRE SNAPSHOTS` lists snapshots other than the current one, and `REMOVE ORPHAN FILES` lists files those snapshots no longer name. Both are a dry run until `EXECUTE`. Parquet Glacier writes store row-group min and max, and a `WHERE` skips groups that cannot match. `EXPLAIN` prints that plan and how many files the `WHERE` skips, without reading the rows. `s3://`, `gs://`, and `az://` keep several ranges in flight, and the last range goes out in the first wave (`GLACIER_HTTP_CHUNK`, default 4 MiB; `GLACIER_HTTP_WINDOW`, default 4). A subquery may name the outer alias and runs once per outer row. `JOIN … USING` keeps each shared column once. `GROUP BY GROUPING SETS`, `ROLLUP`, and `CUBE` aggregate once per set. `GROUPING(col)` is 0 when the column is in the set and 1 when it was aggregated away. `SHOW NAMESPACES` and `SHOW TABLES` list one level of an `s3://`, `gs://`, or `az://` warehouse. Writes to `s3://` and `gs://` are multipart uploads. `az://` writes are block blobs. `EXPIRE SNAPSHOTS … EXECUTE` and `REMOVE ORPHAN FILES … EXECUTE` publish that metadata and delete the object. `PARTITIONED BY` writes `year`, `month`, `day`, `hour`, `bucket`, and `truncate`, and `COMPACT` keeps that spec. `NATURAL JOIN`, a comma join, `LATERAL`, and `WITH RECURSIVE` are in this SQL. `HAVING` may name the outer alias. `WINDOW name AS` and a `GROUPS` frame are too. A public `--listen` takes `--tls-cert` and `--tls-key`. The one `glacier.wasm` decodes ZSTD. `glacier.api_version()` is **2**. `glacier.version()` prints **0.6.3**. 0.6.2 reads a dictionary page that starts a column when the footer leaves its offset empty. One local Parquet file splits its row groups across `GLACIER_THREADS`. A file with a single flat row group splits that group by row. 0.6.3 keeps integer dictionary keys on `SUM`, a filter, and `GROUP BY`, and links the SSE4.2 and AVX2 decode paths on x86_64. `glacier serve --token` requires `Authorization: Bearer` on the REST port. `GRANT SELECT` or `GRANT ALL ON TABLE t TO bearer` records that privilege on the native catalog. The local session stays the owner. Linux `glacier-ui` is this number. |
| **0.7** | Edge identity | A client that is not the local session has a name, and the Iceberg REST port and the Flight SQL port treat that name the same way. A public `--flight` takes TLS, and either `--token` or one RS256 JWKS (`--jwt-jwks`, or `--oidc-issuer`). The shared token is the principal `bearer`. A JWT `sub` is a different principal. A token whose `sub` is `bearer` is refused. `GRANT SELECT ON TABLE t TO name FILTER (pred) MASK (col, 'lit')` stores that policy on the native catalog. Client SQL runs on that scan, so `WHERE 1 = 1`, a subquery, `SELECT *`, and an aggregate do not see the raw row or the raw masked column. `FOR SNAPSHOT` still names that snapshot under the policy. A REST load of a table with that policy is HTTP 403, so the data files stay on the query port. `EXPLAIN` of that table prints `scan policy`, without the file count or the policy text. `COMPACT`, `EXPIRE SNAPSHOTS`, `REMOVE ORPHAN FILES`, `DROP`, `ALTER`, `INSERT`, `UPDATE`, `DELETE`, and `MERGE` on that table are refused. File and snapshot lists of a hidden table, or of a table with that policy, are refused. The local session stays the owner. `glacier.version()` prints **0.7.0**. `GRANT` stays on the native Glacier catalog. `glacier.api_version()` stays 2. |

---

## Update v0.2

What 0.2 adds on top of 0.1 (the SQL surface that 0.1 froze and refused):

- **JOIN.** `LEFT` / `RIGHT` / `FULL JOIN … ON` (equalities, including `AND` of equalities). Null keys do not match. LEFT keeps every left row.
- **Predicates and scalars.** `LIKE`, `IN` (literals or uncorrelated `SELECT`), `BETWEEN`, `CASE WHEN … THEN … ELSE … END`, `SELECT NULL`.
- **Set and subquery.** `UNION` / `UNION ALL` (same schema), `FROM (SELECT …)`, `WITH t AS (SELECT …)`, scalar subquery in `WHERE` or in the select list, uncorrelated `EXISTS` / `NOT EXISTS`.
- **Window.** `ROWS` / `RANGE` frames (`UNBOUNDED PRECEDING` / `FOLLOWING`, `CURRENT ROW`, `N PRECEDING` / `FOLLOWING`), `LAG` / `LEAD`, window after `GROUP BY`.
- **Iceberg scan.** Position and equality deletes. Partition prune for `bucket[N]` / `year` / `month` / `day` / `hour` / `truncate[W]` / `void`, not only file bounds. Snapshot `schema-id` may differ from `current-schema-id` (promote int32→int64 / float32→float64; incompatible types error; new optional columns are null).
- **Parquet nested.** A LIST of primitives is utf8 (`[1, 2, 3]`). Flat STRUCT leaves are columns. Maps and nested lists stay rejected.
- **Python wheel.** `pip install glacier-olap` (`import glacier`). The wheel ships `libglacier.so` with zlib and zstd compiled in (no host `libz` / `libzstd`). CI builds `manylinux_2_28` for x86_64 and aarch64.
- **Kof JVM.** [`bindings/kof`](bindings/kof): `.kf` API plus Java/JNI on `glacier.h`, tested with Kof 0.4.9-beta. `SELECT 1` and parquet `COUNT(*)` on Linux.
- **SQL scalars and VALUES.** `lower` / `upper` (ASCII), `length` / `char_length`, `trim` / `ltrim` / `rtrim`, `replace`, `substr` / `substring`, `concat`, `left` / `right`, `starts_with` / `ends_with` / `contains`, `strpos`, `date_trunc`, `extract` / `year` / `month` / `day` / `hour` / `minute` / `second` (timestamp as epoch microseconds), `ceil` / `floor` / `sign`, `greatest` / `least`, `COUNT` / `SUM` / `AVG` / `MIN` / `MAX(DISTINCT col)`, `VALUES (…), (…)`, `GROUP BY` expressions.
- **Iceberg REST client.** `--catalog` loads tables through `GET /v1/config` and `loadTable`. Auth: none, bearer, OAuth2 client credentials, catalog SigV4. Vended S3/GCS keys from the table `config` map. `gs://` is HTTPS + Bearer.

Linux is still the tested path. `glacier.api_version()` is `2`.

---

## Update v0.3

A connection is a session of named catalogs, not one path.

- **Names.** SQL talks `catalog.namespace.table`. Two levels when there is a default catalog (`USE`). One level is the file-path shortcut.
- **Adapters.** `files` (one Parquet/Avro/`.glacier` or one Iceberg table), `hadoop` (warehouse root), `glacier` (empty dir or `_glacier_catalog.json`), `iceberg_rest` (`http(s)://` that is not a data file).
- **SQL.** `SHOW CATALOGS` / `SHOW NAMESPACES` / `SHOW TABLES [FROM cat.ns]`, `DESCRIBE`, `USE cat[.ns]`, `ATTACH` / `DETACH`. `FROM glacier.catalogs` / `glacier.tables` / `glacier.snapshots` / `glacier.files` (Iceberg), also `t.snapshots` / `t.files`.
- **JOIN** across catalogs. Time travel stays `FROM t FOR SNAPSHOT …`. Remote `ATTACH` is refused in WASM.

---

## Update v0.4

Writes go through the catalog adapter. Iceberg is the default table format when Glacier creates a table. Iceberg is not the catalog.

- **SQL.** `CREATE NAMESPACE`, `CREATE TABLE` (columns or `AS SELECT`, optional `PARTITIONED BY` (`identity`, `year`, `month`, `day`, `hour`, `bucket`, `truncate`)), `INSERT INTO … VALUES` / `SELECT`, `COPY t FROM 'file.parquet'` (also `.avro`, `.glacier`, `s3://`, `gs://`), `DELETE FROM t [WHERE …]` (equality deletes), `UPDATE` / `MERGE`, `ALTER TABLE t ADD COLUMN` (optional), `DROP TABLE`.
- **Where.** Native `glacier` and Hadoop commit `metadata.json` on the warehouse. REST writes data files to the table location (local, or a multipart upload to `s3://` / `gs://`) and `commitTable` (assert snapshot; HTTP 409 if someone wrote first). The `files` catalog stays read-only.
- **WASM.** Remote `CREATE` / `INSERT` are refused.

---

## Update v0.5

`glacier serve` is the process, not a BI plugin.

- **Iceberg REST** on `http://127.0.0.1:8181`: `GET /v1/config`, namespaces, tables, `loadTable`, `commitTable`. Other engines point `type=rest` here.
- **Arrow Flight SQL** on `grpc://127.0.0.1:8815`: Handshake, `CommandStatementQuery`, `GetFlightInfo`, `DoGet`, `GetCatalogs`, `GetTables`. Each connection opens its own `Session` on the warehouse. Two Flight clients share snapshots: an `INSERT` is visible on the next connection.
- **Bind.** Localhost without TLS. A public `--listen` needs `--tls-cert` and `--tls-key` (PEM). `--flight` stays on localhost. WASM and the Python wheel do not include `serve`. Remote Python over Flight is `adbc_driver_flightsql` later, not this wheel.

## Update v0.6

Flight `DoGet` and `Session` results stream more than one record batch.

- **Chunks.** Default 65536 rows (`GLACIER_BATCH_ROWS`). `Result.nextBatch` / `glacier_result_next_arrow` / Python `next_batch()` yield each chunk. Values across chunks match the full scan.
- **Temp tables.** `CREATE TEMP TABLE` / `CREATE TEMPORARY TABLE` (columns or `AS SELECT`) and `INSERT INTO` live in the session. `DROP TABLE` removes one. They show up in `SHOW TABLES` and `SHOW TABLES FROM temp`. A temp name shadows an unqualified catalog table. Another connection does not see them. `PARTITIONED BY` is refused.
- **Cancel.** `glacier_cancel(conn)` (and `Connection.cancel()`) sets a flag the scan checks before each file and each streamed batch. Ctrl+C in the CLI does the same while a statement is running; at the prompt it still exits. `GLACIER_QUERY_MS` is a per-statement budget in milliseconds (0 means already due). `glacier serve` uses that budget on `DoGet`. The connection stays open. A file already opened for `ORDER BY` or a global aggregate finishes that file.
- **Script.** `execute` / `glacier_query` runs every statement in the string, in order, on the same session, and returns the last result. A failure stops the rest. Earlier statements keep their effect. The CLI prints every statement (`-c` with `;`, `--file script.sql`, or a script on stdin). `glacier-ui` lists every result under the editor. `glacier.api_version()` is 2. `glacier_open(path)` stays the one-catalog shortcut.
- **Parallel scan.** Local Parquet and `.glacier` files of one table are read together, one file per core. `GLACIER_THREADS` sets the width (default: the machine). Rows stay in file order. `LIMIT` / `OFFSET`, `ORDER BY`, `GROUP BY`, and `s3://` / `gs://` / `az://` / `http(s)://` stay one file at a time. A file already started runs until its next batch check.
- **Column bounds.** An `INSERT` writes the lower and upper bound of each comparable column (integers, floats, strings, timestamps) into the Iceberg manifest. The next snapshot keeps every bound already stored. A `WHERE` on those columns skips files that cannot match. All-null columns are omitted. Boolean, decimal, and uuid are omitted.
- **Compaction.** `COMPACT TABLE t` reads the current snapshot, applies position and equality deletes, and writes the live rows into new data files: one file, or one file per partition value of the table spec. The new snapshot lists only those files. Previous snapshots stay, so `FOR SNAPSHOT` still sees the earlier files. The old Parquet files stay on disk.
- **Snapshots.** `EXPIRE SNAPSHOTS t` lists every snapshot except the current one and does not rewrite metadata. `EXPIRE SNAPSHOTS t EXECUTE` publishes a metadata file that keeps only the current snapshot. `REMOVE ORPHAN FILES t` lists local `data/` and `metadata/` files the remaining snapshots do not name. `REMOVE ORPHAN FILES t EXECUTE` unlinks those files. On `s3://`, `gs://`, and `az://` the same statements publish the metadata and delete the object. The current snapshot's rows stay.
- **Row groups.** A Parquet file Glacier writes stores min and max per row group (one group, or a new group every 65536 rows). A `WHERE` on a comparable column skips groups that cannot match. The comparison still runs on the groups that remain.
- **Plan.** `EXPLAIN` prints the scan the engine would run and how many files the `WHERE` skips. It does not read the rows. Row-group skips stay inside the file, so the plan names the filter and does not count groups.
- **Remote read.** `s3://`, `gs://`, and `az://` objects (Parquet, `.glacier`, Avro, and Iceberg metadata) are fetched in ranges. Several ranges stay in flight (`GLACIER_HTTP_WINDOW`, default 4, at most 8). The last range goes out in the first wave. `GLACIER_HTTP_CHUNK` sets the range size in bytes (default 4 MiB). The reader opens the cached file when those ranges are written. `GCS_ENDPOINT_URL` points `gs://` at another host, the same role `AWS_ENDPOINT_URL` has for `s3://`.
- **Warehouse list.** `SHOW NAMESPACES` and `SHOW TABLES` on an `s3://`, `gs://`, or `az://` warehouse root list one level of prefixes. `EXPIRE SNAPSHOTS` and `REMOVE ORPHAN FILES` use that listing and delete the object.
- **Multipart upload.** `INSERT` and `COMPACT` write an `s3://` or `gs://` object as initiate, one or more parts, then complete. `GLACIER_PUT_PART` is the part size in bytes (default 8 MiB, at least 5 MiB). The last part is the remainder. An object that fits in one part still uses that sequence.
- **Partition writes.** `PARTITIONED BY` accepts a bare column (`identity`) and `year(col)`, `month(col)`, `day(col)`, `hour(col)`, `bucket(N, col)`, `truncate(W, col)`. `INSERT` and `COMPACT` write one file per transformed value (`ts_day=…`, `id_bucket=…`). A `WHERE` on the source column skips the other files. `COMPACT` keeps that spec.
- **Remote maintenance.** `EXPIRE SNAPSHOTS` and `REMOVE ORPHAN FILES` on an `s3://`, `gs://`, or `az://` table publish metadata with the same multipart upload and delete the object. Dry run is still the default. `EXECUTE` commits.
- **Azure Blob.** `az://container/key` lists, reads, and writes like `s3://`. `AZURE_ENDPOINT_URL` is the account host. `AZURE_STORAGE_TOKEN` is the bearer. Writes are block blobs. The last block is the remainder.
- **Recursive CTE.** `WITH RECURSIVE name AS (anchor UNION ALL step)` repeats the step on the rows added by the previous round, up to 64 rounds.
- **WASM ZSTD.** The one `glacier.wasm` decodes ZSTD Parquet. Snappy, GZIP, and LZ4 stay in that same binary.
- **Correlated subquery.** `EXISTS`, `IN (SELECT …)`, a scalar comparison, a scalar subquery in the select list, and `HAVING` may name the outer alias (`a.id`). The inner query runs once per outer row.
- **JOIN USING.** `JOIN … USING (cols)` equates those columns and returns each one once. `NATURAL JOIN` uses the shared names the same way. A comma between two tables is an inner join with no keys, filtered by `WHERE`. `LATERAL` runs the right subquery once per left row.
- **Grouping sets.** `GROUP BY GROUPING SETS ((cols), …, ())` aggregates once per set. A column that set does not name is null. `()` is one total over the input rows. `ROLLUP (a, b)` is the prefixes `(a, b)`, `(a)`, `()`. `CUBE (a, b)` is every subset. `GROUPING(col)` is 0 when the column is in the current set and 1 when that set aggregated it away. A window may follow the sets. `WINDOW name AS (…)` and `OVER name` name a spec. `GROUPS` counts peer groups the way `ROWS` counts rows.
- **ABI v2.** `glacier_result_arrow` / `fetchall()` / `.arrow()` still export every row. `glacier.version()` is 0.6.3. `glacier_cancel` is additive. The query string is a script.

---

## Update v0.6.3

Integer aggregates and the catalog door.

- **Dictionary keys.** An integer column stored as a dictionary stays as keys for `SUM`, a comparison filter, and `GROUP BY`. A column whose footer lists a dictionary encoding and does not list plain values keeps that dictionary even when the dictionary offset is empty.
- **x86_64 decode.** The SSE4.2 and AVX2 paths in the Parquet reader are linked on that architecture.
- **Serve token.** `glacier serve --token TOKEN` requires `Authorization: Bearer TOKEN` on the Iceberg REST port. Without `--token`, that port stays open. `--flight` stays on localhost.
- **GRANT.** `GRANT SELECT ON TABLE t TO bearer` and `GRANT ALL ON TABLE t TO bearer`, and `REVOKE ... FROM bearer`, record a privilege on the native `glacier` catalog in `_glacier_grants.json`. The local session is the owner and still lists every table. With `--token`, the bearer sees only granted tables. A load without a grant is HTTP 403. `SELECT` allows read. `ALL` also allows commit and `DROP`. `GRANT` on `iceberg_rest`, Hadoop, or `files` is refused. A 403 from a foreign REST catalog stays an error.

`glacier.api_version()` stays 2. `glacier.version()` is 0.6.3.

---

## Update v0.7.0

A client that is not the local session has a name.

- **Flight.** A public `--flight` takes TLS, and either `--token` or one RS256 JWKS (`--jwt-jwks`, or `--oidc-issuer`). With neither a token nor a JWKS, localhost stays open. The shared token is the principal `bearer`. A JWT `sub` is a different principal. A token whose `sub` is `bearer` is refused.
- **Policy.** `GRANT SELECT ON TABLE t TO name FILTER (pred) MASK (col, 'lit')` stores that policy on the native catalog. Client SQL runs on that scan. `WHERE 1 = 1`, a subquery, `SELECT *`, and an aggregate do not see the raw row or the raw masked column. `FOR SNAPSHOT` still names that snapshot. `EXPLAIN` prints `scan policy`, without the file count or the policy text.
- **Files.** A REST load of a table with that policy is HTTP 403. File and snapshot lists of a hidden table, or of a table with that policy, are refused. `COMPACT`, `EXPIRE SNAPSHOTS`, `REMOVE ORPHAN FILES`, `DROP`, `ALTER`, `INSERT`, `UPDATE`, `DELETE`, and `MERGE` on that table are refused. The local session stays the owner.

`glacier.api_version()` stays 2. `glacier.version()` is 0.7.0.

---

## What this tree does

- **SELECT.** `SELECT` (including `SELECT 1` / `SELECT NULL` with no table), `DISTINCT`, `GROUP BY` / `HAVING`, `ORDER BY` (several columns; nulls last on `ASC`), `LIMIT` / `OFFSET`. `GROUP BY` accepts expressions; a select expression that is not a group key uses the first row in the group. `GROUP BY GROUPING SETS ((a), (a, b), ())` runs one aggregation per set. A column left out of a set is null, and `()` is the total. `ROLLUP (a, b)` expands to `(a, b)`, `(a)`, `()`. `CUBE (a, b)` expands to every subset. `GROUPING(col)` is 0 in a set that names the column and 1 in a set that does not. A window may follow the sets. `WINDOW name AS (…)` names a spec for `OVER name`. `GROUPS` counts peer groups. `EXPLAIN` prints the plan of the statement that follows (the scan mode and how many files a `WHERE` would skip) and does not run it.
- **Predicates.** `WHERE` with `AND` / `OR`, comparisons, `IS [NOT] NULL`, `LIKE`, `IN` (literals or `SELECT`), `BETWEEN`, scalar subquery, `EXISTS` / `NOT EXISTS`. A subquery may name the outer alias (`FROM t a WHERE EXISTS (SELECT 1 FROM t b WHERE b.id = a.id)`). The same alias works in a scalar subquery in the select list and in `HAVING`. `LATERAL` runs the right subquery once per left row.
- **Set and subquery.** `UNION` / `UNION ALL` (same schema), `FROM (SELECT …)`, `WITH t AS (SELECT …)`, `WITH RECURSIVE` (the step repeats on the previous round, up to 64 rounds), `VALUES (…), (…)` as a query or `FROM (VALUES …)`, scalar subquery in the select list (`SELECT (SELECT 1)`; zero rows is null, more than one row is an error). A scalar subquery there may also name the outer alias.
- **JOIN.** `INNER` / `LEFT` / `RIGHT` / `FULL JOIN … ON` (equalities, including `AND` of equalities), `USING (cols)`, and `NATURAL JOIN`. A comma between two tables is an inner join with no keys, filtered by `WHERE`. `USING` and `NATURAL` keep each shared column once: the shared names come first, then the other columns of the left input, then the other columns of the right input. A row that matches on one side only takes that side's value. Null keys do not match. LEFT keeps every left row. `COUNT(*)` after a JOIN counts matches from key frequencies; it does not build the joined rows. `WHERE`, `GROUP BY`, `COUNT(col)`, and other aggregates still join first.
- **Aggregates.** `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` (`COUNT(*)` counts every row; the others skip nulls), `COUNT` / `SUM` / `AVG` / `MIN` / `MAX(DISTINCT col)`.
- **Window.** `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` / `ROW_NUMBER` / `RANK` / `DENSE_RANK` / `LAG` / `LEAD` with `OVER ([PARTITION BY …] [ORDER BY …] [ROWS|RANGE …])`. Frames: `UNBOUNDED PRECEDING` / `FOLLOWING`, `CURRENT ROW`, `N PRECEDING` / `FOLLOWING`. `GROUPS` counts peer groups. `RANGE` offsets need a single numeric `ORDER BY`. `WINDOW name AS (…)` names a spec for `OVER name`. Window after `GROUP BY` is allowed.
- **Scalars.** `abs`, `round`, `cast`, `coalesce`, `lower` / `upper` (ASCII), `length` / `char_length` (byte length), `trim` / `ltrim` / `rtrim` (ASCII space / tab / CR / LF), `replace` (all occurrences; empty search leaves the string unchanged), `substr` / `substring` (1-based), `concat`, `left` / `right`, `starts_with` / `ends_with` / `contains`, `strpos` / `instr` (1-based, 0 if missing), `date_trunc` (timestamp as epoch microseconds; units `year` / `month` / `day` / `hour` / `minute` / `second`), `extract` / `year` / `month` / `day` / `hour` / `minute` / `second` (`EXTRACT(HOUR FROM ts)` or `hour(ts)`), `ceil` / `ceiling` / `floor` / `sign`, `greatest` / `least` (skip nulls; all-null is null), `CASE WHEN … THEN … ELSE … END`.
- **COPY TO / FROM.** `COPY TO` writes a native `.glacier` file you can open again. `COPY t FROM 'file.parquet'` (also `.avro`, `.glacier`, `s3://`, `gs://`, `az://`) scans the source and commits Iceberg data files through the catalog. It does not register the external file.
- **Formats.** Parquet (Snappy, GZIP, LZ4, ZSTD), Avro (null / deflate / snappy), Iceberg (`metadata.json` + Avro manifests). Optional Parquet columns are real SQL nulls. A LIST of primitives is utf8 (`[1, 2, 3]`, `["a"]`). Flat STRUCT leaves are ordinary columns. Maps and nested lists (`max_rep_level > 1`) are rejected.
- **Iceberg scan.** Prune uses column bounds and partition values (`identity`, `bucket[N]`, `year` / `month` / `day` / `hour`, `truncate[W]`, `void`; unknown transform is still rejected). Position and equality deletes (`content` 1 / 2) are applied on scan. `DELETE FROM t WHERE …` writes equality deletes through the catalog. `UPDATE` and `MERGE` commit one snapshot with equality deletes and new data files. Snapshot `schema-id` may differ from `current-schema-id` (promote int32 to int64 and float32 to float64; incompatible types error; new optional columns are null). Time travel: `FROM t FOR SNAPSHOT id` / `FOR SNAPSHOT AS OF id` / `FOR TIMESTAMP AS OF epoch_ms` (latest snapshot with `timestamp-ms <=` that value).
- **Access.** Local path, `http(s)://` Range GET, `s3://` (SigV4; `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or `~/.aws/credentials`; `AWS_ENDPOINT_URL` for path-style), `gs://` (HTTPS + Bearer; `GOOGLE_OAUTH_ACCESS_TOKEN` / `GCS_OAUTH_TOKEN`, or a vended `gcs.oauth2.token`; `GCS_ENDPOINT_URL` for another host), `az://` (HTTPS + Bearer; `AZURE_STORAGE_TOKEN`; `AZURE_ENDPOINT_URL` for the account host; writes are block blobs). A remote object is fetched in ranges. Several stay in flight, and the last range is in the first wave (`GLACIER_HTTP_WINDOW`, default 4; `GLACIER_HTTP_CHUNK`, default 4 MiB). The reader opens the cached file when the ranges are written. Iceberg Hadoop-style directories open as a path. A warehouse root (`namespace/table/metadata`) is the `iceberg_hadoop` catalog (`SHOW NAMESPACES` / `FROM cat.ns.table`). If the table was copied, file URIs that still start with `metadata.location` are rewritten to the opened directory.
- **Catalog session.** A connection holds named catalogs. `open(path)` / `glacier_open(path)` registers `files` (one file or one Iceberg table), `hadoop` (a warehouse root), or `glacier` (an empty directory or `_glacier_catalog.json`). An `http(s)://` URI that is not a data file is an Iceberg REST Catalog (`rest`). `--catalog` / `glacier_open_catalog` / `ATTACH 'http://…' AS name` also register `iceberg_rest` (token in the C call, or `ICEBERG_TOKEN`). `ATTACH '/warehouse' AS name` registers `iceberg_hadoop` or, if the directory is empty or missing, the native `glacier` catalog. `SHOW CATALOGS`, `SHOW NAMESPACES`, `SHOW TABLES [FROM cat.ns]`, `DESCRIBE [TABLE] t`, `USE cat[.ns]`, `DETACH [CATALOG] name`. Writes on `glacier`, `iceberg_hadoop`, and `iceberg_rest`: `CREATE NAMESPACE`, `CREATE TABLE` (columns or `AS SELECT`, optional `PARTITIONED BY` (`identity`, `year`, `month`, `day`, `hour`, `bucket`, `truncate`)), `INSERT INTO … VALUES` / `SELECT`, `COPY t FROM 'file.parquet'`, `DELETE FROM t [WHERE …]` (Iceberg equality deletes), `UPDATE t SET … [WHERE …]`, `MERGE INTO t USING u ON … WHEN MATCHED THEN DELETE | UPDATE SET … WHEN NOT MATCHED THEN INSERT`, `COMPACT TABLE t`, `ALTER TABLE t ADD COLUMN` (optional), `DROP TABLE`. Native and Hadoop commit Iceberg on the warehouse (`metadata.json`). REST writes data files to the table location (local path, a multipart upload to `s3://` / `gs://`, or block blobs on `az://`) and commits with `commitTable` (assert snapshot; HTTP 409 if someone wrote first). The `files` catalog stays read-only. `GRANT SELECT` or `GRANT ALL ON TABLE t TO name`, with an optional `FILTER (pred)` and `MASK (col, 'lit')`, and the matching `REVOKE ... FROM name`, record a privilege on the native `glacier` catalog. The shared token is the principal `bearer`. A JWT `sub` is a different principal. The local session is the owner and still lists every table. `GRANT` on `iceberg_rest`, Hadoop, or `files` is refused. A 403 from a foreign REST catalog stays an error. `FROM catalog.namespace.table` and `JOIN` across catalogs. System tables: `glacier.catalogs`, `glacier.tables`, and (Iceberg only) `glacier.snapshots` / `glacier.files`, also as `t.snapshots` / `t.files`. Iceberg is the table format, not the catalog. Remote `ATTACH` and `CREATE` / `INSERT` are refused in WASM. `SHOW NAMESPACES` and `SHOW TABLES` on an `s3://`, `gs://`, or `az://` warehouse list one level of prefixes. `EXPIRE SNAPSHOTS … EXECUTE` and `REMOVE ORPHAN FILES … EXECUTE` publish the metadata and delete the object on that warehouse. Dry run stays the default.
- **Memory.** Large scans stream. `ORDER BY` / `GROUP BY` / `DISTINCT` can spill under `GLACIER_MEM` (default 256 MiB) into `$GLACIER_TEMP`. `COUNT(*)` of a JOIN does not allocate the match rows. Query results split into record batches of `GLACIER_BATCH_ROWS` (default 65536).
- **How you call it.** CLI `glacier` (TUI on a Linux terminal, or `-c SQL`). `glacier-ui` is a Linux session workspace (catalog tree, SQL editor, result grid; `$ZIG build -Dui`; not in WASM or the wheel). `glacier serve [warehouse] [--listen 127.0.0.1:8181] [--flight 127.0.0.1:8815]` is the process: Iceberg REST Catalog on HTTP, Arrow Flight SQL on `grpc://` (native binary; not in WASM or the wheel). Handshake, `CommandStatementQuery`, `GetFlightInfo`, `DoGet` (one FlightData record batch after the schema, repeated), `GetCatalogs`, and `GetTables` talk to a `Session` in that process. Bind is localhost without TLS. A public `--listen` needs `--tls-cert` and `--tls-key`. A public `--flight` needs those and either `--token` or one RS256 JWKS (`--jwt-jwks`, or `--oidc-issuer`). `glacier serve --token TOKEN` requires `Authorization: Bearer TOKEN` on the REST port and on Flight. That secret is the principal `bearer`. A JWT `sub` is a different principal, with the same `GRANT`, including `FILTER` and `MASK`. A REST load of a table with that policy is HTTP 403. The query port still runs the scan. Without `--token` and without a JWKS, localhost stays open. A REST load without a grant is HTTP 403. A Flight query without a grant is permission denied. The local session stays the owner. Shared library `libglacier.so` on Linux + [`include/glacier.h`](include/glacier.h): `glacier_open` (file, warehouse, or REST URI), `glacier_open_catalog` (REST URI + warehouse + token), `glacier_query` (SQL, including `SHOW CATALOGS` / `ATTACH`), `glacier_result_arrow` (every row), `glacier_result_next_arrow` (next batch). Bindings on that header: Python (`pip install glacier-olap`, or `bindings/python` after a local `$ZIG build`), Kof JVM (`bindings/kof`), WASM (`$ZIG build wasm`), Node, Go, Rust. Remote Python over Flight is `adbc_driver_flightsql` later, not this wheel. `glacier.api_version()` is `2`: a query string may hold several statements and the call returns the last result. A file path remains the one-catalog shortcut.

---

## What this tree does not do

A tested Windows (or native cmd) flow. Flight SQL prepared statements or JDBC metadata. A public `--flight` without TLS, and without `--token` or a JWKS, is refused.

---

## Build (Linux)

Zig **0.16.0**. These are Linux shell commands.

```bash
export ZIG="$HOME/opt/zig-0.16/zig"   # or wherever 0.16 lives
$ZIG build
$ZIG build test
$ZIG build fixtures                  # tests/formats + tests/iceberg_prune
$ZIG build wasm                      # zig-out/bin/glacier.wasm
$ZIG build -Dui                      # glacier-ui (Linux session workspace; dvui + SDL3)
```

Output on Linux: `zig-out/bin/glacier`, `zig-out/lib/libglacier.so`, `zig-out/bin/glacier.wasm`. With `-Dui`: `zig-out/bin/glacier-ui`.

```bash
./zig-out/bin/glacier                              # TUI: pick a source, then SQL
./zig-out/bin/glacier tests/formats/sales.parquet  # SQL on that file
./zig-out/bin/glacier tests/formats/sales.parquet -c 'SELECT category, COUNT(*) GROUP BY category'
./zig-out/bin/glacier tests/formats/sales.parquet -c "SELECT lower(category), substr(category, 1, 2), COUNT(DISTINCT category) GROUP BY category"
./zig-out/bin/glacier tests/formats/sales.parquet -c "SELECT upper(category), length(category), SUM(DISTINCT price) WHERE EXISTS (SELECT 1)"
./zig-out/bin/glacier -c 'SELECT 1'
./zig-out/bin/glacier -c "SELECT (SELECT 1), year(0), replace('aba', 'a', 'z')"
./zig-out/bin/glacier -c "SELECT left('fruit', 2), ceil(1.2), greatest(1, 3, 2), hour(3661000000)"
./zig-out/bin/glacier -c 'SELECT * FROM (VALUES (1), (2))'
./zig-out/bin/glacier --catalog http://127.0.0.1:8181 --warehouse lake --token "$ICEBERG_TOKEN" \
  -c 'SELECT COUNT(*) FROM default.sales'
./zig-out/bin/glacier http://127.0.0.1:8181 -c 'SHOW CATALOGS'
./zig-out/bin/glacier -c "ATTACH 'tests/formats/sales.parquet' AS extra" \
  -c 'SHOW TABLES FROM extra' -c 'DESCRIBE extra' -c 'USE extra' -c 'SHOW TABLES'
./zig-out/bin/glacier /path/to/warehouse -c 'SHOW NAMESPACES' -c 'SELECT COUNT(*) FROM hadoop.sales.prune'
./zig-out/bin/glacier tests/iceberg_prune -c 'SELECT COUNT(*) FROM iceberg_prune FOR SNAPSHOT 1'
./zig-out/bin/glacier tests/iceberg_prune -c 'SELECT snapshot_id FROM glacier.snapshots'
./zig-out/bin/glacier -c "ATTACH '/tmp/mylake' AS lake" \
  -c "CREATE NAMESPACE lake.sales" \
  -c "CREATE TABLE lake.sales.orders (id BIGINT, category STRING)" \
  -c "INSERT INTO lake.sales.orders VALUES (1, 'fruit'), (2, 'veg')" \
  -c "DELETE FROM lake.sales.orders WHERE category = 'fruit'" \
  -c "UPDATE lake.sales.orders SET category = 'x' WHERE id = 1" \
  -c "ALTER TABLE lake.sales.orders ADD COLUMN note STRING" \
  -c "SELECT COUNT(*) FROM lake.sales.orders"
./zig-out/bin/glacier-ui                               # catalog tree + SQL + results
./zig-out/bin/glacier-ui tests/formats/sales.parquet   # open that file, SELECT * LIMIT 50
./zig-out/bin/glacier serve /tmp/mylake --listen 127.0.0.1:8181 --flight 127.0.0.1:8815
# catalog: --catalog http://127.0.0.1:8181
# query:   grpc://127.0.0.1:8815  (Flight SQL; two clients share the warehouse)
./zig-out/bin/glacier -c "ATTACH '/tmp/copylake' AS lake" \
  -c "CREATE NAMESPACE lake.sales" \
  -c "CREATE TABLE lake.sales.copied (id BIGINT, price BIGINT, category STRING) PARTITIONED BY (category)" \
  -c "COPY lake.sales.copied FROM 'tests/formats/sales.parquet'" \
  -c "SELECT COUNT(*) FROM lake.sales.copied WHERE category = 'fruit'"
```

---

## Python (Linux)

```bash
pip install glacier-olap
```

The PyPI name is `glacier-olap` because `glacier` is taken. `import glacier` is unchanged. The wheel includes `libglacier.so`; you do not need Zig to run queries. CI builds `manylinux_2_28` wheels; a GitHub Release publishes them to PyPI. Until that upload exists, install a CI artifact or the source checkout below.

From a source checkout:

```bash
$ZIG build -Doptimize=ReleaseFast
python3 -m pip install bindings/python
```

```python
import glacier

con = glacier.connect("tests/formats/sales.parquet")
con.execute("SELECT category, COUNT(*) AS n GROUP BY category").fetchall()
con.execute("SELECT * FROM glacier.catalogs").fetchall()  # [('files', 'files')]

# Iceberg REST Catalog (token also via ICEBERG_TOKEN)
# con = glacier.connect("http://127.0.0.1:8181")
# con = glacier.connect_catalog("http://127.0.0.1:8181", warehouse="lake", token="…")

n = glacier.connect("tests/formats/nulls.parquet")  # $ZIG build fixtures
n.execute("SELECT id WHERE qty IS NULL").fetchall()
```

`connect()` with no path is an empty session (`SELECT 1`, then `ATTACH`). `.arrow()` / `.df()` need pyarrow (and pandas for `.df()`). The connection is a catalog session; a parquet path is the one-table shortcut.

---

## Kof (Linux)

Tested on **Kof 0.4.9-beta**. The binding is still JVM JNI on `libglacier.so`: Kof `extern` binds scalar C symbols, not Arrow C Data. `Conn.kf` is what the compiler sees; at run time that class is Java JNI. Full notes: [`bindings/kof`](bindings/kof).

```kof
var c = Conn.connect()
assert(c.firstInt("SELECT 1") == 1)
c.close()

var sales = Conn.connect("tests/formats/sales.parquet")
assert(sales.firstInt("SELECT COUNT(*)") == 10)
sales.close()
```

```bash
export ZIG="$HOME/opt/zig-0.16/zig"
export KOF_HOME=/path/to/kof-0.4.9-beta-linux-x86_64
bindings/kof/run_tests.sh
```

`connect()` / `connect(path)` are overloads. `connectEmpty` / `connectPath` remain. Do not use `kof run` / `kof test` on this tree: they recompile the stub and drop the JNI class.

---

## WASM, Node, Go, Rust (Linux)

Same `glacier.h`. Build the shared library first (`$ZIG build -Doptimize=ReleaseFast`). WASM takes parquet bytes in memory (`bindings/wasm/example.html`); there is no filesystem in that build. The commands below are Linux.

```bash
$ZIG build wasm && node bindings/wasm/smoke.mjs
cd bindings/node && node-gyp rebuild && node test.mjs
cd bindings/go && go test
cd bindings/rust && cargo test
```

---

## What to expect next

**1.0:** a Windows test suite in CI, or this release says it is a Linux base. Windows CI today compiles and does not run the suite. `pg_glacier` and `ha_glacier` keep their own number.

`glacier.version()` is `0.7.0`. `glacier.api_version()` stays 2. A file path remains the one-catalog shortcut. Issues and CI live in this repo.

---

## License

MIT
