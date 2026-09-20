//! Session is the only entry the REPL / C ABI should call.

const std = @import("std");
const sql = @import("sql/parser.zig");
const physical = @import("execution/physical.zig");
const batch_mod = @import("execution/batch.zig");
const parquet = @import("formats/parquet_wrap.zig");
const avro = @import("formats/avro_wrap.zig");
const iceberg = @import("table/iceberg.zig");
const catalog = @import("table/catalog.zig");
const rest_catalog = @import("table/rest_catalog.zig");
const hadoop_catalog = @import("table/hadoop_catalog.zig");
const glacier_catalog = @import("table/glacier_catalog.zig");
const errmod = @import("error.zig");
const vfs = @import("vfs/source.zig");
const cache = @import("vfs/cache.zig");
const spill = @import("vfs/spill.zig");
const native = @import("formats/native.zig");
const aws = @import("kernel/aws.zig");

pub const Batch = batch_mod.Batch;
pub const Column = batch_mod.Column;
pub const DataType = batch_mod.DataType;
pub const ScanStats = physical.ScanStats;
pub const GlacierError = errmod.GlacierError;
pub const RestOptions = rest_catalog.Options;

var last_open_buf: [512]u8 = undefined;
var last_open_err: ?GlacierError = null;

pub fn lastOpenError() ?GlacierError {
    return last_open_err;
}

fn captureError(buf: []u8, err: anyerror) GlacierError {
    const base = errmod.staticMessage(err);
    const detail = switch (err) {
        error.ParquetOpenFailed => parquet.lastDetail(),
        error.AvroOpenFailed, error.ManifestsNeedAvro => avro.lastDetail(),
        else => "",
    };
    if (detail.len == 0) return .{ .code = errmod.codeOf(err), .message = base };
    const msg = std.fmt.bufPrint(buf, "{s}: {s}", .{ base, detail }) catch base;
    return .{ .code = errmod.codeOf(err), .message = msg };
}

fn openInner(gpa: std.mem.Allocator, io: std.Io, http: *std.http.Client, path: []const u8) !Session {
    var catalog_arena = std.heap.ArenaAllocator.init(gpa);
    errdefer catalog_arena.deinit();
    const a = catalog_arena.allocator();

    const path_owned = try a.dupe(u8, path);
    const trimmed = std.mem.trimEnd(u8, path_owned, "/");
    const table_name = try a.dupe(u8, std.fs.path.stem(trimmed));
    const t = vfs.Transport{ .allocator = gpa, .io = io, .http = http };

    if (isParquetPath(path_owned) or isAvroPath(path_owned) or isGlacierPath(path_owned)) {
        const files = try a.alloc(iceberg.DataFile, 1);
        files[0] = .{
            .path = path_owned,
            .format = if (isAvroPath(path_owned)) .avro else if (isGlacierPath(path_owned)) .glacier else .parquet,
        };
        var session: Session = .{
            .gpa = gpa,
            .io = io,
            .http = undefined,
            .catalog_arena = catalog_arena,
            .path = path_owned,
            .table_name = table_name,
            .files = files,
            .schema_fields = &.{},
        };
        try session.registerCatalog("files", .files, table_name, files, null, &.{}, true, "");
        return session;
    }

    if (glacier_catalog.isCatalog(a, t, path_owned) or glacier_catalog.isEmptyDir(a, t, path_owned)) {
        glacier_catalog.ensure(a, io, path_owned) catch {};
        var session: Session = .{
            .gpa = gpa,
            .io = io,
            .http = undefined,
            .catalog_arena = catalog_arena,
            .path = path_owned,
            .table_name = table_name,
            .files = &.{},
            .schema_fields = &.{},
        };
        const cat_name: []const u8 = if (std.ascii.eqlIgnoreCase(table_name, "glacier")) "lake" else table_name;
        try session.registerCatalog(cat_name, .glacier, "", &.{}, null, &.{}, true, path_owned);
        return session;
    }

    if (!iceberg.isTableDir(a, t, path_owned) and hadoop_catalog.isWarehouse(a, t, path_owned)) {
        var session: Session = .{
            .gpa = gpa,
            .io = io,
            .http = undefined,
            .catalog_arena = catalog_arena,
            .path = path_owned,
            .table_name = table_name,
            .files = &.{},
            .schema_fields = &.{},
        };
        try session.registerCatalog("hadoop", .iceberg_hadoop, "", &.{}, null, &.{}, true, path_owned);
        return session;
    }

    const table = try iceberg.openTable(a, t, path_owned);
    const schema_fields = if (table.metadata.currentSchema()) |s| s.fields else &.{};
    var session: Session = .{
        .gpa = gpa,
        .io = io,
        .http = undefined,
        .catalog_arena = catalog_arena,
        .path = path_owned,
        .table_name = table_name,
        .files = table.files,
        .schema_fields = schema_fields,
    };
    try session.registerCatalog("files", .files, table_name, table.files, null, schema_fields, true, path_owned);
    return session;
}

pub const Session = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    http: std.http.Client,
    catalog_arena: std.heap.ArenaAllocator,
    path: []const u8,
    table_name: []const u8,
    files: []const iceberg.DataFile,
    schema_fields: []const iceberg.SchemaField,
    rest: ?*rest_catalog.Client = null,
    table_cache: std.StringHashMapUnmanaged([]const iceberg.DataFile) = .empty,
    catalogs: []catalog.Entry = &.{},
    default_catalog: []const u8 = "",
    s3_creds: ?aws.Credentials = null,
    s3_endpoint: ?[]const u8 = null,
    s3_region: ?[]const u8 = null,
    gcs_token: ?[]const u8 = null,
    gcs_user_project: ?[]const u8 = null,
    default_namespace: []const u8 = "default",
    error_buf: [512]u8 = undefined,
    last_error: ?GlacierError = null,

    pub fn lastError(self: *const Session) ?GlacierError {
        return self.last_error;
    }

    fn transport(self: *Session) vfs.Transport {
        return .{
            .allocator = self.gpa,
            .io = self.io,
            .http = &self.http,
            .s3_creds = self.s3_creds,
            .s3_endpoint = self.s3_endpoint,
            .s3_region = self.s3_region,
            .gcs_token = self.gcs_token,
            .gcs_user_project = self.gcs_user_project,
        };
    }

    pub fn open(gpa: std.mem.Allocator, io: std.Io, path: []const u8) !Session {
        last_open_err = null;
        if (isRestCatalogUri(path)) {
            return openRest(gpa, io, .{ .endpoint = path });
        }
        var http: std.http.Client = .{ .allocator = gpa, .io = io };
        errdefer http.deinit();
        var session = openInner(gpa, io, &http, path) catch |err| {
            last_open_err = captureError(&last_open_buf, err);
            return err;
        };
        session.http = http;
        return session;
    }

    pub fn openEmpty(gpa: std.mem.Allocator, io: std.Io) !Session {
        last_open_err = null;
        var catalog_arena = std.heap.ArenaAllocator.init(gpa);
        errdefer catalog_arena.deinit();
        const a = catalog_arena.allocator();
        var session: Session = .{
            .gpa = gpa,
            .io = io,
            .http = .{ .allocator = gpa, .io = io },
            .catalog_arena = catalog_arena,
            .path = try a.dupe(u8, ""),
            .table_name = try a.dupe(u8, ""),
            .files = &.{},
            .schema_fields = &.{},
        };
        try session.registerCatalog("files", .files, "", &.{}, null, &.{}, true, "");
        return session;
    }

    pub fn openRest(gpa: std.mem.Allocator, io: std.Io, opts: rest_catalog.Options) !Session {
        last_open_err = null;
        var http: std.http.Client = .{ .allocator = gpa, .io = io };
        errdefer http.deinit();
        var catalog_arena = std.heap.ArenaAllocator.init(gpa);
        errdefer catalog_arena.deinit();
        const a = catalog_arena.allocator();

        const t = vfs.Transport{ .allocator = gpa, .io = io, .http = &http };
        const resolved = rest_catalog.resolveOptions(opts);
        const client_val = rest_catalog.Client.connect(a, t, resolved) catch |err| {
            last_open_err = captureError(&last_open_buf, err);
            return err;
        };
        const client = try a.create(rest_catalog.Client);
        client.* = client_val;

        var session = Session{
            .gpa = gpa,
            .io = io,
            .http = http,
            .catalog_arena = catalog_arena,
            .path = try a.dupe(u8, client.endpoint),
            .table_name = try a.dupe(u8, resolved.default_table orelse "rest"),
            .files = &.{},
            .schema_fields = &.{},
            .rest = client,
            .default_namespace = client.default_namespace,
        };
        try session.applyCatalogConfig(client.config);
        try session.registerCatalog("rest", .iceberg_rest, session.table_name, session.files, client, &.{}, true, "");
        if (resolved.default_table) |name| {
            session.files = session.filesFor(a, name, null) catch |err| {
                last_open_err = captureError(&last_open_buf, err);
                return err;
            };
        }
        return session;
    }

    fn applyCatalogConfig(self: *Session, cfg: rest_catalog.Config) !void {
        const a = self.catalog_arena.allocator();
        if (cfg.s3_access_key_id) |ak| {
            if (cfg.s3_secret_access_key) |sk| {
                self.s3_creds = .{
                    .access_key = try a.dupe(u8, ak),
                    .secret_key = try a.dupe(u8, sk),
                    .session_token = if (cfg.s3_session_token) |tok| try a.dupe(u8, tok) else null,
                    .region = try a.dupe(u8, cfg.s3_region orelse "us-east-1"),
                };
            }
        }
        if (cfg.s3_endpoint) |e| self.s3_endpoint = try a.dupe(u8, e);
        if (cfg.s3_region) |r| self.s3_region = try a.dupe(u8, r);
        if (cfg.gcs_token) |tok| self.gcs_token = try a.dupe(u8, tok);
        if (cfg.gcs_project_id) |p| self.gcs_user_project = try a.dupe(u8, p);
    }

    /// `bytes` are copied. Parquet (or Avro if `format` is .avro).
    pub fn openMemory(gpa: std.mem.Allocator, io: std.Io, bytes: []const u8, format: iceberg.FileFormat) !Session {
        last_open_err = null;
        var catalog_arena = std.heap.ArenaAllocator.init(gpa);
        errdefer catalog_arena.deinit();
        const a = catalog_arena.allocator();
        const owned = try a.dupe(u8, bytes);
        const files = try a.alloc(iceberg.DataFile, 1);
        files[0] = .{
            .path = try a.dupe(u8, ":memory:"),
            .format = format,
            .bytes = owned,
        };
        var session: Session = .{
            .gpa = gpa,
            .io = io,
            .http = .{ .allocator = gpa, .io = io },
            .catalog_arena = catalog_arena,
            .path = files[0].path,
            .table_name = try a.dupe(u8, "memory"),
            .files = files,
            .schema_fields = &.{},
        };
        try session.registerCatalog("files", .files, session.table_name, session.files, null, &.{}, true, "");
        return session;
    }

    pub fn close(self: *Session) void {
        self.http.deinit();
        self.catalog_arena.deinit();
        self.* = undefined;
    }

    pub fn execute(self: *Session, query_sql: []const u8) !Result {
        self.last_error = null;
        return self.executeInner(query_sql) catch |err| {
            self.last_error = captureError(&self.error_buf, err);
            return err;
        };
    }

    fn executeInner(self: *Session, query_sql: []const u8) !Result {
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        errdefer arena.deinit();
        const a = arena.allocator();

        const owned_sql = try a.dupe(u8, query_sql);
        const stmt = try sql.parseStmt(a, owned_sql);
        switch (stmt) {
            .copy => |c| {
                const batch = try self.executeCopy(a, c);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .query => |query| {
                var stats: ScanStats = .{};
                const batch = try self.runQuery(a, query, &stats);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = stats,
                };
            },
            .show => |s| {
                const batch = try self.executeShow(a, s);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .attach => |att| {
                const batch = try self.executeAttach(a, att);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .describe => |d| {
                const batch = try self.executeDescribe(a, d);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .use => |u| {
                const batch = try self.executeUse(a, u);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .detach => |d| {
                const batch = try self.executeDetach(a, d);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .create_namespace => |n| {
                const batch = try self.executeCreateNamespace(a, n);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .create_table => |ct| {
                const batch = try self.executeCreateTable(a, ct);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .insert => |ins| {
                const batch = try self.executeInsert(a, ins);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .delete => |d| {
                const batch = try self.executeDelete(a, d);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .update => |u| {
                const batch = try self.executeUpdate(a, u);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .merge => |m| {
                const batch = try self.executeMerge(a, m);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .alter_table => |alt| {
                const batch = try self.executeAlterTable(a, alt);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
            .drop_table => |d| {
                const batch = try self.executeDropTable(a, d);
                return .{
                    .arena = arena,
                    .batch = batch,
                    .yielded = false,
                    .scan_stats = .{},
                };
            },
        }
    }

    fn executeCopy(self: *Session, a: std.mem.Allocator, c: sql.Copy) !Batch {
        if (c.from_table) |ident| return self.executeCopyFrom(a, ident, c.path);

        const query: sql.Query = c.query orelse .{
            .items = try a.dupe(sql.SelectItem, &.{.star}),
            .from = "",
            .where = null,
            .group_by = &.{},
            .having = null,
            .order_by = &.{},
            .distinct = false,
            .limit = null,
            .offset = null,
        };
        const dest = try native.resolveDest(self.gpa, c.path);
        defer self.gpa.free(dest);
        const dest_owned = try a.dupe(u8, dest);
        var stats: ScanStats = .{};
        const n = if (queryNeedsSession(query)) blk: {
            const batch = try self.runQuery(a, query, &stats);
            break :blk try physical.writeGlacier(a, self.transport(), dest_owned, batch);
        } else blk: {
            const files = try self.filesFor(a, query.from, toIcebergAsOf(query.as_of));
            const right_files: ?[]const iceberg.DataFile = if (query.join) |j| try self.filesFor(a, j.table, null) else null;
            const fallback: ?[]const iceberg.SchemaField = if (self.schema_fields.len == 0) null else self.schema_fields;
            break :blk try physical.copyTo(a, self.transport(), files, query, dest_owned, fallback, &stats, right_files);
        };

        const i64s = try a.alloc(i64, 1);
        i64s[0] = @intCast(n);
        const path_bytes = try a.dupe(u8, dest_owned);
        const offs = try a.alloc(u32, 2);
        offs[0] = 0;
        offs[1] = @intCast(path_bytes.len);
        const columns = try a.alloc(Column, 2);
        columns[0] = .{ .name = "copied", .data_type = .int64, .len = 1, .i64s = i64s };
        columns[1] = .{
            .name = "path",
            .data_type = .utf8,
            .len = 1,
            .utf8 = .{ .offsets = offs, .bytes = path_bytes },
        };
        return .{ .columns = columns, .len = 1 };
    }

    fn executeCopyFrom(self: *Session, a: std.mem.Allocator, ident: []const u8, path: []const u8) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, ident);
        var stats: ScanStats = .{};
        const q: sql.Query = .{
            .items = try a.dupe(sql.SelectItem, &.{.star}),
            .from = path,
            .where = null,
            .group_by = &.{},
            .having = null,
            .order_by = &.{},
            .distinct = false,
            .limit = null,
            .offset = null,
        };
        const input = try self.runQuery(a, q, &stats);
        const copied = try self.appendAligned(a, target, input);
        const n = try a.alloc(i64, 1);
        n[0] = copied;
        const columns = try a.alloc(Column, 1);
        columns[0] = try catalog.i64Col(a, "copied", n);
        return .{ .columns = columns, .len = 1 };
    }

    const NamedBatch = struct {
        name: []const u8,
        batch: Batch,
    };

    fn stripUnion(q: sql.Query) sql.Query {
        var c = q;
        c.union_all = false;
        c.union_right = null;
        c.order_by = &.{};
        c.limit = null;
        c.offset = null;
        return c;
    }

    fn findNamed(env: []const NamedBatch, name: []const u8) ?Batch {
        for (env) |e| {
            if (std.ascii.eqlIgnoreCase(e.name, name)) return e.batch;
        }
        return null;
    }

    fn runQuery(self: *Session, a: std.mem.Allocator, query: sql.Query, stats: *ScanStats) !Batch {
        return self.runQueryEnv(a, query, stats, &.{});
    }

    fn runQueryEnv(
        self: *Session,
        a: std.mem.Allocator,
        query: sql.Query,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) anyerror!Batch {
        if (query.ctes.len > 0) {
            var next: std.ArrayList(NamedBatch) = .empty;
            try next.appendSlice(a, env);
            for (query.ctes) |cte| {
                if (sql.usesTableName(cte.query, cte.name)) return error.UnsupportedSql;
                var body = cte.query.*;
                body.ctes = &.{};
                const b = try self.runQueryEnv(a, body, stats, next.items);
                try next.append(a, .{ .name = cte.name, .batch = b });
            }
            var main = query;
            main.ctes = &.{};
            return self.runQueryEnv(a, main, stats, next.items);
        }
        if (query.union_right == null) return self.runLeafEnv(a, query, stats, env);
        var acc = try self.runLeafEnv(a, stripUnion(query), stats, env);
        var cur = query;
        while (cur.union_right) |right| {
            const rb = try self.runLeafEnv(a, stripUnion(right.*), stats, env);
            acc = try physical.concatUnion(a, acc, rb);
            if (!cur.union_all) acc = try physical.distinctRows(a, acc);
            cur = right.*;
        }
        return physical.finishUnion(a, acc, query);
    }

    fn runLeafEnv(
        self: *Session,
        a: std.mem.Allocator,
        query: sql.Query,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) !Batch {
        var q = try self.rewritePreds(a, query, stats, env);
        if (q.isNoFrom()) {
            return physical.execute(a, self.transport(), &.{}, q, null, stats, null);
        }
        if (q.join) |j| {
            const left = try self.resolveSource(a, q.from, q.from_sub, q.as_of, stats, env);
            const right = try self.resolveSource(a, j.table, j.sub, null, stats, env);
            return physical.joinAndTail(a, left, right, q);
        }
        if (try parseGlacierSystem(q.from)) |sys| {
            const input = try self.executeSystem(a, sys, toIcebergAsOf(q.as_of));
            return physical.executeOnBatch(a, input, q);
        }
        if (try self.parseSystemSuffix(a, q.from)) |sys| {
            const input = try self.executeSystem(a, sys, toIcebergAsOf(q.as_of));
            return physical.executeOnBatch(a, input, q);
        }
        if (q.from_sub != null or (q.from.len > 0 and findNamed(env, q.from) != null)) {
            const input = try self.resolveSource(a, q.from, q.from_sub, q.as_of, stats, env);
            return physical.executeOnBatch(a, input, q);
        }
        const as_of = toIcebergAsOf(q.as_of);
        const files = try self.filesFor(a, q.from, as_of);
        const fallback = self.schemaFallback(a, q.from, as_of);
        return physical.execute(a, self.transport(), files, q, fallback, stats, null);
    }

    fn resolveSource(
        self: *Session,
        a: std.mem.Allocator,
        name: []const u8,
        sub: ?*sql.Query,
        as_of: ?sql.AsOf,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) !Batch {
        if (sub) |q| {
            var inner = q.*;
            inner.ctes = &.{};
            return self.runQueryEnv(a, inner, stats, env);
        }
        if (name.len > 0) {
            if (findNamed(env, name)) |b| return b;
        }
        if (try parseGlacierSystem(name)) |sys| {
            return self.executeSystem(a, sys, toIcebergAsOf(as_of));
        }
        if (try self.parseSystemSuffix(a, name)) |sys| {
            return self.executeSystem(a, sys, toIcebergAsOf(as_of));
        }
        const iceberg_as_of = toIcebergAsOf(as_of);
        const files = try self.filesFor(a, name, iceberg_as_of);
        const fallback = self.schemaFallback(a, name, iceberg_as_of);
        return physical.execute(a, self.transport(), files, scanAll(name), fallback, stats, null);
    }

    fn scanAll(from: []const u8) sql.Query {
        return .{
            .items = &.{.star},
            .from = from,
            .where = null,
            .group_by = &.{},
            .having = null,
            .order_by = &.{},
            .distinct = false,
            .limit = null,
            .offset = null,
        };
    }

    fn rewritePreds(
        self: *Session,
        a: std.mem.Allocator,
        query: sql.Query,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) !sql.Query {
        var q = query;
        if (query.where) |e| q.where = try self.rewriteBool(a, e, &query, stats, env);
        if (query.having) |e| q.having = try self.rewriteBool(a, e, &query, stats, env);
        q.items = try self.rewriteItems(a, query.items, &query, stats, env);
        return q;
    }

    fn rewriteItems(
        self: *Session,
        a: std.mem.Allocator,
        items: []const sql.SelectItem,
        outer: *const sql.Query,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) ![]const sql.SelectItem {
        var need = false;
        for (items) |item| {
            switch (item) {
                .expr => |ex| if (valueHasSub(ex.expr)) {
                    need = true;
                },
                .case => |cs| {
                    for (cs.arms) |arm| {
                        if (predHasSub(arm.when)) need = true;
                    }
                },
                else => {},
            }
        }
        if (!need) return items;
        const out = try a.dupe(sql.SelectItem, items);
        for (out) |*item| {
            switch (item.*) {
                .expr => |ex| {
                    item.expr.expr = try self.rewriteValue(a, ex.expr, outer, stats, env);
                },
                .case => {
                    const arms = try a.dupe(sql.CaseArm, item.case.arms);
                    for (arms) |*arm| {
                        arm.when = try self.rewriteBool(a, arm.when, outer, stats, env);
                    }
                    item.case.arms = arms;
                },
                else => {},
            }
        }
        return out;
    }

    fn rewriteValue(
        self: *Session,
        a: std.mem.Allocator,
        expr: *sql.Expr,
        outer: *const sql.Query,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) !*sql.Expr {
        switch (expr.*) {
            .subquery => |q| {
                if (sql.isCorrelated(q, outer)) return error.UnsupportedSql;
                const batch = try self.runQueryEnv(a, q.*, stats, env);
                if (batch.columns.len != 1) return error.TypeMismatch;
                if (batch.len > 1) return error.SubqueryCardinality;
                const lit: sql.Literal = if (batch.len == 0) .null else try cellToLiteral(a, batch.columns[0], 0);
                expr.* = .{ .literal = lit };
                return expr;
            },
            .binary => |b| {
                expr.binary.left = try self.rewriteValue(a, b.left, outer, stats, env);
                expr.binary.right = try self.rewriteValue(a, b.right, outer, stats, env);
                return expr;
            },
            .unary_minus => |arg| {
                expr.unary_minus = try self.rewriteValue(a, arg, outer, stats, env);
                return expr;
            },
            .call => |c| {
                for (c.args) |arg| {
                    _ = try self.rewriteValue(a, arg, outer, stats, env);
                }
                return expr;
            },
            else => return expr,
        }
    }

    fn rewriteBool(
        self: *Session,
        a: std.mem.Allocator,
        expr: *sql.BoolExpr,
        outer: *const sql.Query,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) !*sql.BoolExpr {
        switch (expr.*) {
            .@"and" => |b| {
                expr.@"and".left = try self.rewriteBool(a, b.left, outer, stats, env);
                expr.@"and".right = try self.rewriteBool(a, b.right, outer, stats, env);
                return expr;
            },
            .@"or" => |b| {
                expr.@"or".left = try self.rewriteBool(a, b.left, outer, stats, env);
                expr.@"or".right = try self.rewriteBool(a, b.right, outer, stats, env);
                return expr;
            },
            .cmp => |c| {
                expr.cmp.left = try self.rewriteValue(a, c.left, outer, stats, env);
                expr.cmp.right = try self.rewriteValue(a, c.right, outer, stats, env);
                return expr;
            },
            .in_query => |p| {
                if (sql.isCorrelated(p.query, outer)) return error.UnsupportedSql;
                const batch = try self.runQueryEnv(a, p.query.*, stats, env);
                const values = try batchToLiterals(a, batch);
                const node = try a.create(sql.BoolExpr);
                node.* = .{ .in_list = .{ .left = p.left, .values = values, .negated = p.negated } };
                return node;
            },
            .cmp_query => |p| {
                if (sql.isCorrelated(p.query, outer)) return error.UnsupportedSql;
                const batch = try self.runQueryEnv(a, p.query.*, stats, env);
                if (batch.columns.len != 1) return error.TypeMismatch;
                if (batch.len > 1) return error.SubqueryCardinality;
                const lit: sql.Literal = if (batch.len == 0) .null else try cellToLiteral(a, batch.columns[0], 0);
                const lit_e = try a.create(sql.Expr);
                lit_e.* = .{ .literal = lit };
                const node = try a.create(sql.BoolExpr);
                node.* = .{ .cmp = .{ .op = p.op, .left = p.left, .right = lit_e } };
                return node;
            },
            .exists => |p| {
                if (sql.isCorrelated(p.query, outer)) return error.UnsupportedSql;
                const batch = try self.runQueryEnv(a, p.query.*, stats, env);
                const present = batch.len > 0;
                const ok = if (p.negated) !present else present;
                const left = try a.create(sql.Expr);
                left.* = .{ .literal = .{ .int = 1 } };
                const right = try a.create(sql.Expr);
                right.* = .{ .literal = .{ .int = if (ok) 1 else 0 } };
                const node = try a.create(sql.BoolExpr);
                node.* = .{ .cmp = .{ .op = .eq, .left = left, .right = right } };
                return node;
            },
            else => return expr,
        }
    }

    fn registerCatalog(
        self: *Session,
        name: []const u8,
        kind: catalog.Kind,
        table_name: []const u8,
        files: []const iceberg.DataFile,
        rest: ?*rest_catalog.Client,
        schema_fields: []const iceberg.SchemaField,
        set_default: bool,
        warehouse: []const u8,
    ) !void {
        if (std.ascii.eqlIgnoreCase(name, "glacier")) return error.InvalidSyntax;
        if (self.findCatalog(name) != null) return error.InvalidSyntax;
        const a = self.catalog_arena.allocator();
        const owned = try a.dupe(u8, name);
        const tbl = try a.dupe(u8, table_name);
        const wh = try a.dupe(u8, warehouse);
        const n = self.catalogs.len;
        const next = try a.alloc(catalog.Entry, n + 1);
        if (n > 0) @memcpy(next[0..n], self.catalogs);
        next[n] = .{
            .name = owned,
            .kind = kind,
            .table_name = tbl,
            .files = files,
            .rest = rest,
            .schema_fields = schema_fields,
            .warehouse = wh,
        };
        self.catalogs = next;
        if (set_default or self.default_catalog.len == 0) self.default_catalog = owned;
    }

    fn findCatalog(self: *const Session, name: []const u8) ?catalog.Entry {
        for (self.catalogs) |c| {
            if (std.ascii.eqlIgnoreCase(c.name, name)) return c;
        }
        return null;
    }

    fn executeShow(self: *Session, a: std.mem.Allocator, s: sql.Show) !Batch {
        switch (s) {
            .catalogs => {
                const names = try a.alloc([]const u8, self.catalogs.len);
                const types = try a.alloc([]const u8, self.catalogs.len);
                for (self.catalogs, 0..) |c, i| {
                    names[i] = c.name;
                    types[i] = c.kind.label();
                }
                return catalog.utf8Cols(a, &.{ "name", "type" }, &.{ names, types });
            },
            .namespaces => |n| {
                const cat_name = n.catalog orelse self.default_catalog;
                const entry = self.findCatalog(cat_name) orelse return error.TableNotFound;
                return self.showNamespaces(a, entry);
            },
            .tables => |t| {
                const cat_name = t.catalog orelse self.default_catalog;
                const entry = self.findCatalog(cat_name) orelse return error.TableNotFound;
                return self.showTables(a, entry, t.namespace);
            },
        }
    }

    fn showNamespaces(self: *Session, a: std.mem.Allocator, entry: catalog.Entry) !Batch {
        var names: std.ArrayList([]const u8) = .empty;
        var cats: std.ArrayList([]const u8) = .empty;
        switch (entry.kind) {
            .files => {},
            .iceberg_rest => {
                const client = entry.rest orelse self.rest;
                if (client) |c| {
                    if (c.listNamespaces(a, self.transport())) |nss| {
                        for (nss) |ns| {
                            try names.append(a, ns);
                            try cats.append(a, entry.name);
                        }
                    } else |_| {}
                }
            },
            .iceberg_hadoop => {
                if (hadoop_catalog.listNamespaces(a, self.transport(), entry.warehouse)) |nss| {
                    for (nss) |ns| {
                        try names.append(a, ns);
                        try cats.append(a, entry.name);
                    }
                } else |_| {}
            },
            .glacier => {
                if (glacier_catalog.listNamespaces(a, self.transport(), entry.warehouse)) |nss| {
                    for (nss) |ns| {
                        try names.append(a, ns);
                        try cats.append(a, entry.name);
                    }
                } else |_| {}
            },
        }
        return catalog.utf8Cols(a, &.{ "name", "catalog" }, &.{ names.items, cats.items });
    }

    fn showTables(self: *Session, a: std.mem.Allocator, entry: catalog.Entry, namespace: ?[]const u8) !Batch {
        var names: std.ArrayList([]const u8) = .empty;
        var cats: std.ArrayList([]const u8) = .empty;
        var nss: std.ArrayList([]const u8) = .empty;
        switch (entry.kind) {
            .files => {
                if (entry.table_name.len > 0) {
                    try names.append(a, entry.table_name);
                    try cats.append(a, entry.name);
                    try nss.append(a, "");
                }
            },
            .iceberg_rest => {
                const ns = namespace orelse self.default_namespace;
                var listed = false;
                const client = entry.rest orelse self.rest;
                if (client) |c| {
                    if (c.listTables(a, self.transport(), ns)) |idents| {
                        listed = true;
                        for (idents) |id| {
                            try names.append(a, id.name);
                            try cats.append(a, entry.name);
                            try nss.append(a, id.namespace);
                        }
                    } else |_| {}
                }
                if (!listed) {
                    var it = self.table_cache.iterator();
                    while (it.next()) |kv| {
                        const ident = rest_catalog.splitIdent(kv.key_ptr.*, self.default_namespace);
                        try names.append(a, ident.name);
                        try cats.append(a, entry.name);
                        try nss.append(a, ident.ns);
                    }
                    if (self.files.len > 0 and self.table_name.len > 0) {
                        var seen = false;
                        for (names.items) |n| {
                            if (std.ascii.eqlIgnoreCase(n, self.table_name)) {
                                seen = true;
                                break;
                            }
                        }
                        if (!seen) {
                            try names.append(a, self.table_name);
                            try cats.append(a, entry.name);
                            try nss.append(a, self.default_namespace);
                        }
                    }
                }
            },
            .iceberg_hadoop => {
                const ns = namespace orelse self.default_namespace;
                if (hadoop_catalog.listTables(a, self.transport(), entry.warehouse, ns)) |idents| {
                    for (idents) |id| {
                        try names.append(a, id.name);
                        try cats.append(a, entry.name);
                        try nss.append(a, id.namespace);
                    }
                } else |_| {}
            },
            .glacier => {
                const ns = namespace orelse self.default_namespace;
                if (glacier_catalog.listTables(a, self.transport(), entry.warehouse, ns)) |idents| {
                    for (idents) |id| {
                        try names.append(a, id.name);
                        try cats.append(a, entry.name);
                        try nss.append(a, id.namespace);
                    }
                } else |_| {}
            },
        }
        return catalog.utf8Cols(a, &.{ "name", "catalog", "namespace" }, &.{
            names.items, cats.items, nss.items,
        });
    }

    fn executeAttach(self: *Session, a: std.mem.Allocator, att: sql.Attach) !Batch {
        if (isRestUri(att.source) and !isParquetPath(att.source) and !isAvroPath(att.source) and !isGlacierPath(att.source)) {
            const cat_a = self.catalog_arena.allocator();
            const client_val = try rest_catalog.Client.connect(cat_a, self.transport(), .{ .endpoint = att.source });
            const client = try cat_a.create(rest_catalog.Client);
            client.* = client_val;
            try self.applyCatalogConfig(client.config);
            try self.registerCatalog(att.name, .iceberg_rest, "", &.{}, client, &.{}, false, "");
            if (self.rest == null) self.rest = client;
            return catalog.utf8Cols(a, &.{ "name", "type" }, &.{
                &.{att.name}, &.{catalog.Kind.iceberg_rest.label()},
            });
        }
        const cat_a = self.catalog_arena.allocator();
        const src = try cat_a.dupe(u8, std.mem.trimEnd(u8, att.source, "/"));
        if (!isParquetPath(src) and !isAvroPath(src) and !isGlacierPath(src) and
            !iceberg.isTableDir(cat_a, self.transport(), src))
        {
            const missing = isMissingLocal(self.io, src);
            if (glacier_catalog.isCatalog(cat_a, self.transport(), src) or
                glacier_catalog.isEmptyDir(cat_a, self.transport(), src) or
                missing)
            {
                try glacier_catalog.ensure(cat_a, self.io, src);
                try self.registerCatalog(att.name, .glacier, "", &.{}, null, &.{}, false, src);
                return catalog.utf8Cols(a, &.{ "name", "type" }, &.{
                    &.{att.name}, &.{catalog.Kind.glacier.label()},
                });
            }
            if (hadoop_catalog.isWarehouse(cat_a, self.transport(), src)) {
                try self.registerCatalog(att.name, .iceberg_hadoop, "", &.{}, null, &.{}, false, src);
                return catalog.utf8Cols(a, &.{ "name", "type" }, &.{
                    &.{att.name}, &.{catalog.Kind.iceberg_hadoop.label()},
                });
            }
        }
        const loaded = try self.loadFilesPath(att.source);
        try self.registerCatalog(att.name, .files, loaded.table_name, loaded.files, null, loaded.schema_fields, false, loaded.root);
        return catalog.utf8Cols(a, &.{ "name", "type" }, &.{
            &.{att.name}, &.{catalog.Kind.files.label()},
        });
    }

    fn executeDescribe(self: *Session, a: std.mem.Allocator, d: sql.Describe) !Batch {
        const ident = d.ident;
        if (ident.len == 0) return error.InvalidSyntax;
        if (try parseGlacierSystem(ident)) |sys| return describeSystem(a, sys.kind);
        if (try self.parseSystemSuffix(a, ident)) |sys| return describeSystem(a, sys.kind);

        if (std.mem.indexOfScalar(u8, ident, '.')) |dot| {
            const head = ident[0..dot];
            const rest = ident[dot + 1 ..];
            if (self.findCatalog(head)) |c| {
                if (c.kind == .iceberg_rest) return self.describeRest(a, c, rest);
                if (c.kind == .iceberg_hadoop or c.kind == .glacier) return self.describeHadoop(a, c, rest);
                if (c.kind == .files) {
                    if (!filesIdentMatches(c, rest)) return error.TableNotFound;
                    return self.describeEntry(a, c);
                }
            }
        }

        if (self.findCatalog(ident)) |c| {
            if (c.kind == .iceberg_rest or c.kind == .iceberg_hadoop or c.kind == .glacier) return error.TableNotFound;
            return self.describeEntry(a, c);
        }

        const def = self.findCatalog(self.default_catalog) orelse return error.TableNotFound;
        if (def.kind == .iceberg_rest) return self.describeRest(a, def, ident);
        if (def.kind == .iceberg_hadoop or def.kind == .glacier) return self.describeHadoop(a, def, ident);
        if (def.kind == .files and filesIdentMatches(def, ident)) return self.describeEntry(a, def);
        for (self.catalogs) |c| {
            if (c.kind == .files and filesIdentMatches(c, ident)) return self.describeEntry(a, c);
        }
        return error.TableNotFound;
    }

    fn describeEntry(self: *Session, a: std.mem.Allocator, entry: catalog.Entry) !Batch {
        if (entry.schema_fields.len > 0) return describeFields(a, entry.schema_fields);
        if (entry.files.len > 0) return self.describeDataFiles(a, entry.files);
        return error.TableNotFound;
    }

    fn describeRest(self: *Session, a: std.mem.Allocator, entry: catalog.Entry, ident: []const u8) !Batch {
        const client = entry.rest orelse self.rest orelse return error.TableNotFound;
        if (ident.len == 0) return error.TableNotFound;
        const parts = rest_catalog.splitIdent(ident, client.default_namespace);
        const loaded = try client.loadTable(a, self.transport(), parts.ns, parts.name);
        const schema = loaded.metadata.currentSchema() orelse return error.SchemaNotFound;
        return describeFields(a, schema.fields);
    }

    fn describeHadoop(self: *Session, a: std.mem.Allocator, entry: catalog.Entry, ident: []const u8) !Batch {
        if (ident.len == 0) return error.TableNotFound;
        const parts = rest_catalog.splitIdent(ident, self.default_namespace);
        const dir = try hadoop_catalog.tablePath(a, entry.warehouse, parts.ns, parts.name);
        const table = try iceberg.openTable(a, self.transport(), dir);
        const schema = table.metadata.currentSchema() orelse return error.SchemaNotFound;
        return describeFields(a, schema.fields);
    }

    fn describeDataFiles(self: *Session, a: std.mem.Allocator, files: []const iceberg.DataFile) !Batch {
        if (files.len == 0) return error.TableNotFound;
        if (files[0].format != .parquet) return error.UnsupportedType;
        var reader = try parquet.openLocation(self.gpa, self.transport(), files[0].path);
        defer reader.close();
        const n: usize = @intCast(@max(reader.numColumns(), 0));
        const names = try a.alloc([]const u8, n);
        const types = try a.alloc([]const u8, n);
        const required = try a.alloc([]const u8, n);
        var i: i32 = 0;
        while (i < reader.numColumns()) : (i += 1) {
            const col = reader.column(i) orelse return error.ParquetOpenFailed;
            const idx: usize = @intCast(i);
            names[idx] = try a.dupe(u8, col.name);
            types[idx] = try parquetTypeLabel(a, reader, i);
            required[idx] = "true";
        }
        return catalog.utf8Cols(a, &.{ "name", "type", "required" }, &.{ names, types, required });
    }

    fn executeUse(self: *Session, a: std.mem.Allocator, u: sql.Use) !Batch {
        const entry = self.findCatalog(u.catalog) orelse return error.TableNotFound;
        self.default_catalog = entry.name;
        if (u.namespace) |ns| {
            const cat_a = self.catalog_arena.allocator();
            self.default_namespace = try cat_a.dupe(u8, ns);
        }
        const ns_out = u.namespace orelse "";
        return catalog.utf8Cols(a, &.{ "catalog", "namespace" }, &.{
            &.{entry.name}, &.{ns_out},
        });
    }

    fn executeDetach(self: *Session, a: std.mem.Allocator, d: sql.Detach) !Batch {
        const idx = self.findCatalogIndex(d.name) orelse return error.TableNotFound;
        const entry = self.catalogs[idx];
        const kind_label = entry.kind.label();
        const detached_name = try a.dupe(u8, entry.name);

        if (self.rest != null and entry.rest != null and self.rest == entry.rest) {
            self.rest = null;
            for (self.catalogs, 0..) |c, i| {
                if (i != idx and c.kind == .iceberg_rest) {
                    self.rest = c.rest;
                    break;
                }
            }
        }
        if (entry.files.ptr == self.files.ptr) {
            self.files = &.{};
            self.schema_fields = &.{};
        }

        var drop: std.ArrayList([]const u8) = .empty;
        var it = self.table_cache.iterator();
        while (it.next()) |kv| {
            if (cacheKeyBelongsTo(kv.key_ptr.*, detached_name)) {
                try drop.append(a, kv.key_ptr.*);
            }
        }
        for (drop.items) |key| _ = self.table_cache.remove(key);

        const cat_a = self.catalog_arena.allocator();
        const next = try cat_a.alloc(catalog.Entry, self.catalogs.len - 1);
        var w: usize = 0;
        for (self.catalogs, 0..) |c, i| {
            if (i == idx) continue;
            next[w] = c;
            w += 1;
        }
        self.catalogs = next;
        if (std.ascii.eqlIgnoreCase(self.default_catalog, detached_name)) {
            self.default_catalog = if (self.catalogs.len > 0) self.catalogs[0].name else "";
        }

        return catalog.utf8Cols(a, &.{ "name", "type" }, &.{
            &.{detached_name}, &.{kind_label},
        });
    }

    fn refuseWrite() !void {
        if (comptime @import("builtin").cpu.arch == .wasm32) return error.WriteUnsupported;
    }

    fn clearTableCache(self: *Session) void {
        self.table_cache = .{};
    }

    const WriteTarget = struct {
        entry: catalog.Entry,
        ns: []const u8,
        name: []const u8,
        dir: []const u8,
    };

    fn resolveWrite(self: *Session, a: std.mem.Allocator, ident: []const u8) !WriteTarget {
        if (ident.len == 0) return error.InvalidSyntax;
        var cat_name = self.default_catalog;
        var rest = ident;
        if (std.mem.indexOfScalar(u8, ident, '.')) |dot| {
            if (self.findCatalog(ident[0..dot])) |c| {
                cat_name = c.name;
                rest = ident[dot + 1 ..];
            }
        }
        const entry = self.findCatalog(cat_name) orelse return error.TableNotFound;
        switch (entry.kind) {
            .iceberg_hadoop, .glacier => {
                if (entry.warehouse.len == 0) return error.WriteUnsupported;
                if (rest.len == 0) return error.InvalidSyntax;
                const parts = rest_catalog.splitIdent(rest, self.default_namespace);
                const dir = try hadoop_catalog.tablePath(a, entry.warehouse, parts.ns, parts.name);
                return .{ .entry = entry, .ns = parts.ns, .name = parts.name, .dir = dir };
            },
            .iceberg_rest => {
                if (entry.rest == null) return error.WriteUnsupported;
                if (rest.len == 0) return error.InvalidSyntax;
                const parts = rest_catalog.splitIdent(rest, self.default_namespace);
                return .{ .entry = entry, .ns = parts.ns, .name = parts.name, .dir = "" };
            },
            .files => return error.WriteUnsupported,
        }
    }

    fn resolveNamespaceIdent(self: *Session, ident: []const u8) !struct { entry: catalog.Entry, ns: []const u8 } {
        if (ident.len == 0) return error.InvalidSyntax;
        var cat_name = self.default_catalog;
        var ns = ident;
        if (std.mem.indexOfScalar(u8, ident, '.')) |dot| {
            if (self.findCatalog(ident[0..dot])) |_| {
                cat_name = ident[0..dot];
                ns = ident[dot + 1 ..];
            }
        }
        if (ns.len == 0) return error.InvalidSyntax;
        const entry = self.findCatalog(cat_name) orelse return error.TableNotFound;
        switch (entry.kind) {
            .iceberg_hadoop, .glacier => {
                if (entry.warehouse.len == 0) return error.WriteUnsupported;
            },
            .iceberg_rest => {
                if (entry.rest == null) return error.WriteUnsupported;
            },
            .files => return error.WriteUnsupported,
        }
        return .{ .entry = entry, .ns = ns };
    }

    fn executeCreateNamespace(self: *Session, a: std.mem.Allocator, n: sql.CreateNamespace) !Batch {
        try refuseWrite();
        const target = try self.resolveNamespaceIdent(n.ident);
        if (target.entry.kind == .iceberg_rest) {
            const client = target.entry.rest orelse return error.WriteUnsupported;
            try client.createNamespace(a, self.transport(), target.ns);
        } else {
            const ns_dir = try vfs.joinLocation(a, target.entry.warehouse, target.ns);
            std.Io.Dir.cwd().createDirPath(self.io, ns_dir) catch {};
            if (target.entry.kind == .glacier) {
                try glacier_catalog.addNamespace(a, self.io, target.entry.warehouse, target.ns);
            }
        }
        return catalog.utf8Cols(a, &.{ "namespace", "catalog" }, &.{
            &.{target.ns}, &.{target.entry.name},
        });
    }

    fn executeCreateTable(self: *Session, a: std.mem.Allocator, ct: sql.CreateTable) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, ct.ident);
        const fields = if (ct.as_select) |q| blk: {
            var stats: ScanStats = .{};
            const input = try self.runQuery(a, q, &stats);
            if (input.columns.len == 0) return error.InvalidSyntax;
            const inferred = try fieldsFromBatch(a, input);
            const part = try iceberg.specFromNames(a, inferred, ct.partition_by);
            if (target.entry.kind == .iceberg_rest) {
                try self.restCreateAndMaybeInsert(a, target, inferred, input, part);
            } else {
                try iceberg.createTable(a, self.io, target.dir, inferred, part);
                if (input.len > 0) {
                    const aligned = try alignToFields(a, input, inferred);
                    try iceberg.appendBatch(a, self.io, target.dir, aligned);
                }
            }
            break :blk inferred;
        } else blk: {
            if (ct.columns.len == 0) return error.InvalidSyntax;
            const inferred = try fieldsFromColDefs(a, ct.columns);
            const part = try iceberg.specFromNames(a, inferred, ct.partition_by);
            if (target.entry.kind == .iceberg_rest) {
                try self.restCreateAndMaybeInsert(a, target, inferred, .{ .columns = &.{}, .len = 0 }, part);
            } else {
                try iceberg.createTable(a, self.io, target.dir, inferred, part);
            }
            break :blk inferred;
        };
        _ = fields;
        if (target.entry.kind == .glacier) {
            glacier_catalog.addTable(a, self.io, target.entry.warehouse, target.ns, target.name, target.dir) catch |err| switch (err) {
                error.InvalidSyntax => {},
                else => return err,
            };
        }
        self.clearTableCache();
        return catalog.utf8Cols(a, &.{ "name", "catalog", "namespace" }, &.{
            &.{target.name}, &.{target.entry.name}, &.{target.ns},
        });
    }

    fn restCreateAndMaybeInsert(
        self: *Session,
        a: std.mem.Allocator,
        target: WriteTarget,
        fields: []const iceberg.SchemaField,
        input: Batch,
        partition_fields: []const iceberg.PartitionField,
    ) !void {
        const client = target.entry.rest orelse return error.WriteUnsupported;
        const loaded = try client.createTable(a, self.transport(), target.ns, target.name, fields, partition_fields);
        if (input.len == 0) return;
        const aligned = try alignToFields(a, input, fields);
        try self.restCommitAppend(a, client, target.ns, target.name, loaded, aligned);
    }

    fn restCommitAppend(
        self: *Session,
        a: std.mem.Allocator,
        client: *rest_catalog.Client,
        ns: []const u8,
        name: []const u8,
        loaded: rest_catalog.LoadedTable,
        aligned: Batch,
    ) !void {
        if (aligned.len == 0) return;
        try self.applyCatalogConfig(loaded.config);
        const table = try iceberg.openFromMetadata(
            a,
            self.transport(),
            loaded.metadata,
            rest_catalog.fileBase(loaded),
        );
        const appended = try iceberg.appendFiles(a, self.transport(), table, aligned);
        _ = try client.commitAppend(a, self.transport(), ns, name, table.metadata, appended);
    }

    fn executeInsert(self: *Session, a: std.mem.Allocator, ins: sql.Insert) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, ins.ident);
        var stats: ScanStats = .{};
        const input = try self.runQuery(a, ins.query, &stats);
        const inserted = try self.appendAligned(a, target, input);
        const n = try a.alloc(i64, 1);
        n[0] = inserted;
        const columns = try a.alloc(Column, 1);
        columns[0] = try catalog.i64Col(a, "inserted", n);
        return .{ .columns = columns, .len = 1 };
    }

    fn executeDelete(self: *Session, a: std.mem.Allocator, del: sql.Delete) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, del.ident);
        var stats: ScanStats = .{};
        const q: sql.Query = .{
            .items = try a.dupe(sql.SelectItem, &.{.star}),
            .from = del.ident,
            .where = del.where,
            .group_by = &.{},
            .having = null,
            .order_by = &.{},
            .distinct = false,
            .limit = null,
            .offset = null,
        };
        const input = try self.runQuery(a, q, &stats);
        if (input.len == 0) {
            const z = try a.alloc(i64, 1);
            z[0] = 0;
            const columns = try a.alloc(Column, 1);
            columns[0] = try catalog.i64Col(a, "deleted", z);
            return .{ .columns = columns, .len = 1 };
        }
        const deleted = try self.appendEqAligned(a, target, input);
        const n = try a.alloc(i64, 1);
        n[0] = deleted;
        const columns = try a.alloc(Column, 1);
        columns[0] = try catalog.i64Col(a, "deleted", n);
        return .{ .columns = columns, .len = 1 };
    }

    fn executeUpdate(self: *Session, a: std.mem.Allocator, upd: sql.Update) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, upd.ident);
        const schema = try self.writeSchema(a, target);
        var stats: ScanStats = .{};
        const q: sql.Query = .{
            .items = try a.dupe(sql.SelectItem, &.{.star}),
            .from = upd.ident,
            .where = upd.where,
            .group_by = &.{},
            .having = null,
            .order_by = &.{},
            .distinct = false,
            .limit = null,
            .offset = null,
        };
        const old_rows = try self.runQuery(a, q, &stats);
        if (old_rows.len == 0) return countCol(a, "updated", 0);
        const aligned_old = try alignToFields(a, old_rows, schema);
        const updated = try applyAssignments(a, aligned_old, aligned_old, schema, null, upd.assignments);
        const aligned_new = try alignToFields(a, updated, schema);
        _ = try self.appendMixAligned(a, target, aligned_old, aligned_new);
        return countCol(a, "updated", @intCast(aligned_new.len));
    }

    fn executeMerge(self: *Session, a: std.mem.Allocator, mer: sql.Merge) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, mer.ident);
        const schema = try self.writeSchema(a, target);
        const dest_qual = mer.alias orelse tableStem(mer.ident);
        const src_qual = mer.source_alias orelse tableStem(mer.source);
        if (std.ascii.eqlIgnoreCase(dest_qual, src_qual)) return error.InvalidSyntax;

        var deleted_n: i64 = 0;
        var updated_n: i64 = 0;
        var inserted_n: i64 = 0;
        var delete_batch: Batch = .{ .columns = &.{}, .len = 0 };
        var insert_batch: Batch = .{ .columns = &.{}, .len = 0 };

        if (mer.matched) |matched| {
            var stats: ScanStats = .{};
            const inner: sql.Query = .{
                .items = try a.dupe(sql.SelectItem, &.{.star}),
                .from = mer.ident,
                .from_alias = dest_qual,
                .join = .{
                    .kind = .inner,
                    .table = mer.source,
                    .alias = src_qual,
                    .sub = mer.source_sub,
                    .eqs = mer.eqs,
                },
                .where = null,
                .group_by = &.{},
                .having = null,
                .order_by = &.{},
                .distinct = false,
                .limit = null,
                .offset = null,
            };
            const joined = try self.runQuery(a, inner, &stats);
            if (joined.len > 0) {
                const dest_rows = try projectFields(a, joined, schema, dest_qual);
                switch (matched) {
                    .delete => {
                        delete_batch = dest_rows;
                        deleted_n = @intCast(dest_rows.len);
                    },
                    .update => |assigns| {
                        delete_batch = dest_rows;
                        const updated = try applyAssignments(a, dest_rows, joined, schema, dest_qual, assigns);
                        insert_batch = try alignToFields(a, updated, schema);
                        updated_n = @intCast(insert_batch.len);
                    },
                }
            }
        }

        if (mer.not_matched) |ins| {
            const key = mer.eqs[0];
            const dest_key = if (key.left.qualifier) |q|
                if (std.ascii.eqlIgnoreCase(q, dest_qual)) key.left.name else key.right.name
            else
                key.left.name;
            const isnull = try a.create(sql.BoolExpr);
            isnull.* = .{ .isnull = .{
                .left = .{ .column = try std.fmt.allocPrint(a, "{s}.{s}", .{ dest_qual, dest_key }) },
                .negated = false,
            } };
            var stats: ScanStats = .{};
            const anti: sql.Query = .{
                .items = try a.dupe(sql.SelectItem, &.{.star}),
                .from = mer.source,
                .from_alias = src_qual,
                .from_sub = mer.source_sub,
                .join = .{
                    .kind = .left,
                    .table = mer.ident,
                    .alias = dest_qual,
                    .eqs = mer.eqs,
                },
                .where = isnull,
                .group_by = &.{},
                .having = null,
                .order_by = &.{},
                .distinct = false,
                .limit = null,
                .offset = null,
            };
            const unmatched = try self.runQuery(a, anti, &stats);
            if (unmatched.len > 0) {
                const built = try buildMergeInsert(a, unmatched, schema, src_qual, ins);
                const extra = try alignToFieldsByName(a, built, schema);
                insert_batch = try concatBatches(a, insert_batch, extra);
                inserted_n = @intCast(extra.len);
            }
        }

        if (delete_batch.len > 0 or insert_batch.len > 0) {
            _ = try self.appendMixAligned(a, target, delete_batch, insert_batch);
        }
        const n = try a.alloc(i64, 1);
        n[0] = deleted_n;
        const u = try a.alloc(i64, 1);
        u[0] = updated_n;
        const i = try a.alloc(i64, 1);
        i[0] = inserted_n;
        const columns = try a.alloc(Column, 3);
        columns[0] = try catalog.i64Col(a, "deleted", n);
        columns[1] = try catalog.i64Col(a, "updated", u);
        columns[2] = try catalog.i64Col(a, "inserted", i);
        return .{ .columns = columns, .len = 1 };
    }

    fn executeAlterTable(self: *Session, a: std.mem.Allocator, alt: sql.AlterTable) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, alt.ident);
        if (target.entry.kind == .iceberg_rest) {
            const client = target.entry.rest orelse return error.WriteUnsupported;
            const loaded = try client.loadTable(a, self.transport(), target.ns, target.name);
            const schema = loaded.metadata.currentSchema() orelse return error.SchemaNotFound;
            for (schema.fields) |f| {
                if (std.ascii.eqlIgnoreCase(f.name, alt.column.name)) return error.InvalidSyntax;
            }
            var last_id: i32 = 0;
            for (schema.fields) |f| {
                if (f.id > last_id) last_id = f.id;
            }
            const fields = try a.alloc(iceberg.SchemaField, schema.fields.len + 1);
            @memcpy(fields[0..schema.fields.len], schema.fields);
            fields[schema.fields.len] = .{
                .id = last_id + 1,
                .name = try a.dupe(u8, alt.column.name),
                .required = false,
                .type_name = try a.dupe(u8, alt.column.type_name),
            };
            _ = try client.commitAddColumn(a, self.transport(), target.ns, target.name, loaded.metadata, fields);
        } else {
            try iceberg.addOptionalColumn(a, self.io, target.dir, alt.column.name, alt.column.type_name);
        }
        self.clearTableCache();
        return catalog.utf8Cols(a, &.{ "name", "column", "type" }, &.{
            &.{target.name}, &.{alt.column.name}, &.{alt.column.type_name},
        });
    }

    fn appendAligned(self: *Session, a: std.mem.Allocator, target: WriteTarget, input: Batch) !i64 {
        const n: i64 = if (target.entry.kind == .iceberg_rest) blk: {
            const client = target.entry.rest orelse return error.WriteUnsupported;
            const loaded = try client.loadTable(a, self.transport(), target.ns, target.name);
            const schema = loaded.metadata.currentSchema() orelse return error.SchemaNotFound;
            const aligned = try alignToFields(a, input, schema.fields);
            try self.restCommitAppend(a, client, target.ns, target.name, loaded, aligned);
            break :blk @intCast(aligned.len);
        } else blk: {
            const table = try iceberg.openTable(a, self.transport(), target.dir);
            const schema = table.metadata.currentSchema() orelse return error.SchemaNotFound;
            const aligned = try alignToFields(a, input, schema.fields);
            try iceberg.appendBatch(a, self.io, target.dir, aligned);
            break :blk @intCast(aligned.len);
        };
        self.clearTableCache();
        return n;
    }

    fn appendEqAligned(self: *Session, a: std.mem.Allocator, target: WriteTarget, input: Batch) !i64 {
        const n: i64 = if (target.entry.kind == .iceberg_rest) blk: {
            const client = target.entry.rest orelse return error.WriteUnsupported;
            const loaded = try client.loadTable(a, self.transport(), target.ns, target.name);
            const schema = loaded.metadata.currentSchema() orelse return error.SchemaNotFound;
            const aligned = try alignToFields(a, input, schema.fields);
            try self.restCommitDeletes(a, client, target.ns, target.name, loaded, aligned);
            break :blk @intCast(aligned.len);
        } else blk: {
            const table = try iceberg.openTable(a, self.transport(), target.dir);
            const schema = table.metadata.currentSchema() orelse return error.SchemaNotFound;
            const aligned = try alignToFields(a, input, schema.fields);
            try iceberg.appendEqDeleteBatch(a, self.io, target.dir, aligned);
            break :blk @intCast(aligned.len);
        };
        self.clearTableCache();
        return n;
    }

    fn restCommitDeletes(
        self: *Session,
        a: std.mem.Allocator,
        client: *rest_catalog.Client,
        ns: []const u8,
        name: []const u8,
        loaded: rest_catalog.LoadedTable,
        aligned: Batch,
    ) !void {
        if (aligned.len == 0) return;
        try self.applyCatalogConfig(loaded.config);
        const table = try iceberg.openFromMetadata(
            a,
            self.transport(),
            loaded.metadata,
            rest_catalog.fileBase(loaded),
        );
        const appended = try iceberg.appendEqDeletes(a, self.transport(), table, aligned);
        _ = try client.commitAppend(a, self.transport(), ns, name, table.metadata, appended);
    }

    fn writeSchema(self: *Session, a: std.mem.Allocator, target: WriteTarget) ![]const iceberg.SchemaField {
        if (target.entry.kind == .iceberg_rest) {
            const client = target.entry.rest orelse return error.WriteUnsupported;
            const loaded = try client.loadTable(a, self.transport(), target.ns, target.name);
            const schema = loaded.metadata.currentSchema() orelse return error.SchemaNotFound;
            return schema.fields;
        }
        const table = try iceberg.openTable(a, self.transport(), target.dir);
        const schema = table.metadata.currentSchema() orelse return error.SchemaNotFound;
        return schema.fields;
    }

    fn appendMixAligned(self: *Session, a: std.mem.Allocator, target: WriteTarget, deletes: Batch, inserts: Batch) !void {
        const empty: Batch = .{ .columns = &.{}, .len = 0 };
        const del = if (deletes.len == 0) empty else deletes;
        const ins = if (inserts.len == 0) empty else inserts;
        if (del.len == 0 and ins.len == 0) return;
        if (target.entry.kind == .iceberg_rest) {
            const client = target.entry.rest orelse return error.WriteUnsupported;
            const loaded = try client.loadTable(a, self.transport(), target.ns, target.name);
            try self.applyCatalogConfig(loaded.config);
            const table = try iceberg.openFromMetadata(
                a,
                self.transport(),
                loaded.metadata,
                rest_catalog.fileBase(loaded),
            );
            const appended = try iceberg.appendMix(a, self.transport(), table, del, ins);
            _ = try client.commitAppend(a, self.transport(), target.ns, target.name, table.metadata, appended);
        } else {
            try iceberg.appendMixBatch(a, self.io, target.dir, del, ins);
        }
        self.clearTableCache();
    }

    fn executeDropTable(self: *Session, a: std.mem.Allocator, d: sql.DropTable) !Batch {
        try refuseWrite();
        const target = try self.resolveWrite(a, d.ident);
        if (target.entry.kind == .iceberg_rest) {
            const client = target.entry.rest orelse return error.WriteUnsupported;
            try client.dropTable(a, self.transport(), target.ns, target.name);
        } else {
            if (target.entry.kind == .glacier) {
                _ = glacier_catalog.dropTable(a, self.io, target.entry.warehouse, target.ns, target.name) catch |err| switch (err) {
                    error.TableNotFound => {},
                    else => return err,
                };
            }
            iceberg.unpublishTable(self.io, target.dir);
        }
        self.clearTableCache();
        return catalog.utf8Cols(a, &.{ "name", "catalog", "namespace" }, &.{
            &.{target.name}, &.{target.entry.name}, &.{target.ns},
        });
    }

    fn findCatalogIndex(self: *const Session, name: []const u8) ?usize {
        for (self.catalogs, 0..) |c, i| {
            if (std.ascii.eqlIgnoreCase(c.name, name)) return i;
        }
        return null;
    }

    const LoadedFiles = struct {
        table_name: []const u8,
        files: []const iceberg.DataFile,
        schema_fields: []const iceberg.SchemaField = &.{},
        root: []const u8 = "",
    };

    fn loadFilesPath(self: *Session, path: []const u8) !LoadedFiles {
        const a = self.catalog_arena.allocator();
        const path_owned = try a.dupe(u8, path);
        const trimmed = std.mem.trimEnd(u8, path_owned, "/");
        const table_name = try a.dupe(u8, std.fs.path.stem(trimmed));
        if (isParquetPath(path_owned) or isAvroPath(path_owned) or isGlacierPath(path_owned)) {
            const files = try a.alloc(iceberg.DataFile, 1);
            files[0] = .{
                .path = path_owned,
                .format = if (isAvroPath(path_owned)) .avro else if (isGlacierPath(path_owned)) .glacier else .parquet,
            };
            return .{ .table_name = table_name, .files = files };
        }
        const table = try iceberg.openTable(a, self.transport(), path_owned);
        const schema_fields = if (table.metadata.currentSchema()) |s| s.fields else &.{};
        return .{ .table_name = table_name, .files = table.files, .schema_fields = schema_fields, .root = path_owned };
    }

    fn catalogFiles(self: *const Session, from: []const u8) ?[]const iceberg.DataFile {
        if (self.findCatalog(from)) |c| {
            if (c.kind == .files and c.files.len > 0) return c.files;
        }
        const dot = std.mem.indexOfScalar(u8, from, '.') orelse {
            for (self.catalogs) |c| {
                if (c.kind == .files and c.table_name.len > 0 and
                    std.ascii.eqlIgnoreCase(c.table_name, from) and c.files.len > 0)
                    return c.files;
            }
            return null;
        };
        const cat = from[0..dot];
        const rest = from[dot + 1 ..];
        const c = self.findCatalog(cat) orelse return null;
        if (c.kind != .files or c.files.len == 0) return null;
        if (c.table_name.len == 0) return c.files;
        const stem = std.fs.path.stem(rest);
        if (std.ascii.eqlIgnoreCase(rest, c.table_name) or std.ascii.eqlIgnoreCase(stem, c.table_name))
            return c.files;
        if (std.mem.lastIndexOfScalar(u8, rest, '.')) |i| {
            if (std.ascii.eqlIgnoreCase(rest[i + 1 ..], c.table_name)) return c.files;
        }
        return null;
    }

    fn filesFor(self: *Session, arena: std.mem.Allocator, from: []const u8, as_of: ?iceberg.AsOf) ![]const iceberg.DataFile {
        if (as_of != null) return self.filesForAsOf(arena, from, as_of.?);
        if (from.len == 0) return self.files;
        if (self.catalogFiles(from)) |files| return files;
        if (isParquetPath(from) or isAvroPath(from) or isGlacierPath(from)) {
            const files = try arena.alloc(iceberg.DataFile, 1);
            files[0] = .{
                .path = try arena.dupe(u8, from),
                .format = if (isAvroPath(from)) .avro else if (isGlacierPath(from)) .glacier else .parquet,
            };
            return files;
        }
        if (std.mem.indexOfScalar(u8, from, '/') != null or std.mem.indexOf(u8, from, "://") != null) {
            if (iceberg.openTable(arena, self.transport(), from)) |table| {
                return table.files;
            } else |_| {}
            const files = try arena.alloc(iceberg.DataFile, 1);
            files[0] = .{
                .path = try arena.dupe(u8, from),
                .format = .parquet,
            };
            return files;
        }
        const stem = std.fs.path.stem(from);
        if (std.ascii.eqlIgnoreCase(stem, self.table_name) and self.files.len > 0) return self.files;
        if (self.resolveHadoop(from)) |h| {
            if (h.ident.len == 0) return error.TableNotFound;
            const cat_a = self.catalog_arena.allocator();
            const cache_key = try std.fmt.allocPrint(cat_a, "{s}\x1f{s}", .{ h.cat, h.ident });
            if (self.table_cache.get(cache_key)) |cached| return cached;
            const ident = rest_catalog.splitIdent(h.ident, self.default_namespace);
            const dir = try hadoop_catalog.tablePath(cat_a, h.warehouse, ident.ns, ident.name);
            const table = iceberg.openTable(cat_a, self.transport(), dir) catch |err| switch (err) {
                error.FileNotFound, error.InvalidMetadata, error.SnapshotNotFound, error.SchemaNotFound => return error.TableNotFound,
                else => return err,
            };
            try self.table_cache.put(cat_a, cache_key, table.files);
            return table.files;
        }
        if (self.resolveRest(from)) |r| {
            if (r.ident.len == 0) return error.TableNotFound;
            const cat_a = self.catalog_arena.allocator();
            const cache_key = try std.fmt.allocPrint(cat_a, "{s}\x1f{s}", .{ r.cat, r.ident });
            if (self.table_cache.get(cache_key)) |cached| return cached;
            const ident = rest_catalog.splitIdent(r.ident, r.client.default_namespace);
            const loaded = try r.client.loadTable(cat_a, self.transport(), ident.ns, ident.name);
            try self.applyCatalogConfig(loaded.config);
            const table = try iceberg.openFromMetadata(
                cat_a,
                self.transport(),
                loaded.metadata,
                rest_catalog.fileBase(loaded),
            );
            try self.table_cache.put(cat_a, cache_key, table.files);
            if (self.schema_fields.len == 0) {
                if (table.metadata.currentSchema()) |s| self.schema_fields = s.fields;
            }
            return table.files;
        }
        return error.TableNotFound;
    }

    fn filesForAsOf(self: *Session, arena: std.mem.Allocator, from: []const u8, as_of: iceberg.AsOf) ![]const iceberg.DataFile {
        const cat_a = self.catalog_arena.allocator();
        const suffix = switch (as_of) {
            .snapshot => |id| try std.fmt.allocPrint(cat_a, "s{d}", .{id}),
            .timestamp_ms => |ts| try std.fmt.allocPrint(cat_a, "t{d}", .{ts}),
        };
        const cache_key = try std.fmt.allocPrint(cat_a, "{s}\x1f{s}", .{ from, suffix });
        if (self.table_cache.get(cache_key)) |cached| return cached;

        if (self.resolveRest(from)) |r| {
            if (r.ident.len == 0) return error.TableNotFound;
            const ident = rest_catalog.splitIdent(r.ident, r.client.default_namespace);
            const loaded = try r.client.loadTable(cat_a, self.transport(), ident.ns, ident.name);
            try self.applyCatalogConfig(loaded.config);
            const table = try iceberg.openFromMetadataAsOf(
                cat_a,
                self.transport(),
                loaded.metadata,
                rest_catalog.fileBase(loaded),
                as_of,
            );
            try self.table_cache.put(cat_a, cache_key, table.files);
            return table.files;
        }

        const dir = try self.icebergDir(arena, from);
        const table = try iceberg.openTableAsOf(cat_a, self.transport(), dir, as_of);
        try self.table_cache.put(cat_a, cache_key, table.files);
        return table.files;
    }

    fn icebergDir(self: *Session, arena: std.mem.Allocator, from: []const u8) ![]const u8 {
        if (from.len == 0) {
            if (self.findCatalog(self.default_catalog)) |c| {
                if (c.kind == .files and c.warehouse.len > 0) return c.warehouse;
                if (c.kind == .files) return error.UnsupportedSql;
            }
            if (self.path.len > 0) return self.path;
            return error.TableNotFound;
        }
        if (self.findCatalog(from)) |c| {
            if (c.kind == .files) {
                if (c.warehouse.len > 0) return c.warehouse;
                return error.UnsupportedSql;
            }
        }
        if (self.resolveHadoop(from)) |h| {
            if (h.ident.len == 0) return error.TableNotFound;
            const ident = rest_catalog.splitIdent(h.ident, self.default_namespace);
            return hadoop_catalog.tablePath(arena, h.warehouse, ident.ns, ident.name);
        }
        if (std.mem.indexOfScalar(u8, from, '.')) |dot| {
            if (self.findCatalog(from[0..dot])) |c| {
                if (c.kind == .files) {
                    if (c.warehouse.len > 0 and filesIdentMatches(c, from[dot + 1 ..])) return c.warehouse;
                    if (c.warehouse.len == 0) return error.UnsupportedSql;
                }
            }
        }
        for (self.catalogs) |c| {
            if (c.kind != .files) continue;
            const hit = filesIdentMatches(c, from) or
                (c.table_name.len > 0 and std.ascii.eqlIgnoreCase(c.table_name, from));
            if (!hit) continue;
            if (c.warehouse.len > 0) return c.warehouse;
            return error.UnsupportedSql;
        }
        if (std.mem.indexOfScalar(u8, from, '/') != null or std.mem.indexOf(u8, from, "://") != null)
            return from;
        return error.TableNotFound;
    }

    fn parseSystemSuffix(self: *Session, arena: std.mem.Allocator, from: []const u8) !?SystemTable {
        const dot = std.mem.lastIndexOfScalar(u8, from, '.') orelse return null;
        const prefix = from[0..dot];
        const tail = from[dot + 1 ..];
        if (prefix.len == 0) return null;
        const kind: SystemKind = if (std.ascii.eqlIgnoreCase(tail, "snapshots"))
            .snapshots
        else if (std.ascii.eqlIgnoreCase(tail, "files"))
            .files
        else
            return null;
        if (!try self.looksLikeIceberg(arena, prefix)) return null;
        return .{ .kind = kind, .ident = prefix };
    }

    fn looksLikeIceberg(self: *Session, arena: std.mem.Allocator, from: []const u8) !bool {
        if (self.resolveRest(from)) |r| return r.ident.len > 0;
        if (self.icebergDir(arena, from)) |_| return true else |err| switch (err) {
            error.UnsupportedSql, error.TableNotFound => return false,
            else => return err,
        }
    }

    fn executeSystem(self: *Session, a: std.mem.Allocator, sys: SystemTable, as_of: ?iceberg.AsOf) !Batch {
        return switch (sys.kind) {
            .catalogs => self.executeShow(a, .catalogs),
            .tables => self.glacierTables(a),
            .snapshots => self.systemSnapshots(a, sys.ident),
            .files => self.systemFiles(a, sys.ident, as_of),
        };
    }

    fn systemIcebergIdents(self: *Session, a: std.mem.Allocator, ident: []const u8) ![]const []const u8 {
        if (ident.len > 0) {
            const one = try a.alloc([]const u8, 1);
            one[0] = ident;
            return one;
        }
        var out: std.ArrayList([]const u8) = .empty;
        var catalog_session = false;
        for (self.catalogs) |entry| {
            switch (entry.kind) {
                .iceberg_rest => {
                    catalog_session = true;
                    const client = entry.rest orelse self.rest orelse continue;
                    const namespaces = client.listNamespaces(a, self.transport()) catch continue;
                    for (namespaces) |ns| {
                        const idents = client.listTables(a, self.transport(), ns) catch continue;
                        for (idents) |id| {
                            const ns_name = if (id.namespace.len > 0) id.namespace else ns;
                            const qn = if (ns_name.len == 0)
                                id.name
                            else
                                try std.fmt.allocPrint(a, "{s}.{s}", .{ ns_name, id.name });
                            try out.append(a, qn);
                        }
                    }
                },
                .iceberg_hadoop => {
                    if (entry.warehouse.len == 0) continue;
                    catalog_session = true;
                    const namespaces = hadoop_catalog.listNamespaces(a, self.transport(), entry.warehouse) catch continue;
                    for (namespaces) |ns| {
                        const idents = hadoop_catalog.listTables(a, self.transport(), entry.warehouse, ns) catch continue;
                        for (idents) |id| {
                            const ns_name = if (id.namespace.len > 0) id.namespace else ns;
                            const qn = if (ns_name.len == 0)
                                id.name
                            else
                                try std.fmt.allocPrint(a, "{s}.{s}", .{ ns_name, id.name });
                            try out.append(a, qn);
                        }
                    }
                },
                .glacier => {
                    if (entry.warehouse.len == 0) continue;
                    catalog_session = true;
                    const namespaces = glacier_catalog.listNamespaces(a, self.transport(), entry.warehouse) catch continue;
                    for (namespaces) |ns| {
                        const idents = glacier_catalog.listTables(a, self.transport(), entry.warehouse, ns) catch continue;
                        for (idents) |id| {
                            const ns_name = if (id.namespace.len > 0) id.namespace else ns;
                            const qn = if (ns_name.len == 0)
                                id.name
                            else
                                try std.fmt.allocPrint(a, "{s}.{s}", .{ ns_name, id.name });
                            try out.append(a, qn);
                        }
                    }
                },
                .files => {},
            }
        }
        if (out.items.len > 0) return out.items;
        if (catalog_session) return error.UnsupportedSql;
        const one = try a.alloc([]const u8, 1);
        one[0] = "";
        return one;
    }

    fn concatSystemParts(a: std.mem.Allocator, parts: []const Batch) !Batch {
        if (parts.len == 0) return error.UnsupportedSql;
        var acc = parts[0];
        for (parts[1..]) |p| acc = try concatBatches(a, acc, p);
        return acc;
    }

    fn systemSnapshots(self: *Session, a: std.mem.Allocator, ident: []const u8) !Batch {
        const idents = try self.systemIcebergIdents(a, ident);
        var parts: std.ArrayList(Batch) = .empty;
        for (idents) |id| {
            const table = self.openIcebergTable(a, id, null) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => continue,
            };
            try parts.append(a, try snapshotsBatch(a, table.metadata));
        }
        return concatSystemParts(a, parts.items);
    }

    fn systemFiles(self: *Session, a: std.mem.Allocator, ident: []const u8, as_of: ?iceberg.AsOf) !Batch {
        const idents = try self.systemIcebergIdents(a, ident);
        var parts: std.ArrayList(Batch) = .empty;
        for (idents) |id| {
            const table = self.openIcebergTable(a, id, as_of) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => continue,
            };
            try parts.append(a, try filesBatch(a, try tableFilesList(a, table)));
        }
        return concatSystemParts(a, parts.items);
    }

    fn glacierTables(self: *Session, a: std.mem.Allocator) !Batch {
        var names: std.ArrayList([]const u8) = .empty;
        var cats: std.ArrayList([]const u8) = .empty;
        var nss: std.ArrayList([]const u8) = .empty;
        for (self.catalogs) |entry| {
            switch (entry.kind) {
                .files => {
                    if (entry.table_name.len == 0) continue;
                    try names.append(a, entry.table_name);
                    try cats.append(a, entry.name);
                    try nss.append(a, "");
                },
                .iceberg_hadoop => {
                    if (hadoop_catalog.listNamespaces(a, self.transport(), entry.warehouse)) |namespaces| {
                        for (namespaces) |ns| {
                            if (hadoop_catalog.listTables(a, self.transport(), entry.warehouse, ns)) |idents| {
                                for (idents) |id| {
                                    try names.append(a, id.name);
                                    try cats.append(a, entry.name);
                                    try nss.append(a, id.namespace);
                                }
                            } else |_| {}
                        }
                    } else |_| {}
                },
                .glacier => {
                    if (glacier_catalog.listNamespaces(a, self.transport(), entry.warehouse)) |namespaces| {
                        for (namespaces) |ns| {
                            if (glacier_catalog.listTables(a, self.transport(), entry.warehouse, ns)) |idents| {
                                for (idents) |id| {
                                    try names.append(a, id.name);
                                    try cats.append(a, entry.name);
                                    try nss.append(a, id.namespace);
                                }
                            } else |_| {}
                        }
                    } else |_| {}
                },
                .iceberg_rest => {
                    var listed = false;
                    const client = entry.rest orelse self.rest;
                    if (client) |c| {
                        if (c.listNamespaces(a, self.transport())) |namespaces| {
                            listed = true;
                            for (namespaces) |ns| {
                                if (c.listTables(a, self.transport(), ns)) |idents| {
                                    for (idents) |id| {
                                        try names.append(a, id.name);
                                        try cats.append(a, entry.name);
                                        try nss.append(a, id.namespace);
                                    }
                                } else |_| {}
                            }
                        } else |_| {}
                    }
                    if (!listed) {
                        const part = try self.showTables(a, entry, null);
                        var i: usize = 0;
                        while (i < part.len) : (i += 1) {
                            try names.append(a, part.columns[0].strAt(i));
                            try cats.append(a, part.columns[1].strAt(i));
                            try nss.append(a, part.columns[2].strAt(i));
                        }
                    }
                },
            }
        }
        return catalog.utf8Cols(a, &.{ "name", "catalog", "namespace" }, &.{
            names.items, cats.items, nss.items,
        });
    }

    fn schemaFallback(
        self: *Session,
        arena: std.mem.Allocator,
        from: []const u8,
        as_of: ?iceberg.AsOf,
    ) ?[]const iceberg.SchemaField {
        if (self.openIcebergTable(arena, from, as_of)) |table| {
            if (table.metadata.currentSchema()) |s| return s.fields;
        } else |_| {}
        if (self.schema_fields.len == 0) return null;
        return self.schema_fields;
    }

    fn openIcebergTable(
        self: *Session,
        arena: std.mem.Allocator,
        from: []const u8,
        as_of: ?iceberg.AsOf,
    ) !iceberg.Table {
        if (self.resolveRest(from)) |r| {
            if (r.ident.len == 0) return error.TableNotFound;
            const ident = rest_catalog.splitIdent(r.ident, r.client.default_namespace);
            const loaded = try r.client.loadTable(arena, self.transport(), ident.ns, ident.name);
            try self.applyCatalogConfig(loaded.config);
            return iceberg.openFromMetadataAsOf(
                arena,
                self.transport(),
                loaded.metadata,
                rest_catalog.fileBase(loaded),
                as_of,
            );
        }
        const dir = try self.icebergDir(arena, from);
        return iceberg.openTableAsOf(arena, self.transport(), dir, as_of);
    }

    fn resolveHadoop(self: *Session, from: []const u8) ?struct {
        warehouse: []const u8,
        ident: []const u8,
        cat: []const u8,
    } {
        if (std.mem.indexOfScalar(u8, from, '.')) |dot| {
            if (self.findCatalog(from[0..dot])) |c| {
                if (c.kind == .iceberg_hadoop or c.kind == .glacier) {
                    return .{ .warehouse = c.warehouse, .ident = from[dot + 1 ..], .cat = c.name };
                }
            }
        }
        if (self.findCatalog(from)) |c| {
            if (c.kind == .iceberg_hadoop or c.kind == .glacier) {
                return .{ .warehouse = c.warehouse, .ident = "", .cat = c.name };
            }
        }
        if (self.findCatalog(self.default_catalog)) |c| {
            if ((c.kind == .iceberg_hadoop or c.kind == .glacier) and c.warehouse.len > 0) {
                return .{ .warehouse = c.warehouse, .ident = from, .cat = c.name };
            }
        }
        return null;
    }

    fn resolveRest(self: *Session, from: []const u8) ?struct {
        client: *rest_catalog.Client,
        ident: []const u8,
        cat: []const u8,
    } {
        if (std.mem.indexOfScalar(u8, from, '.')) |dot| {
            if (self.findCatalog(from[0..dot])) |c| {
                if (c.kind == .iceberg_rest) {
                    const client = c.rest orelse self.rest orelse return null;
                    return .{ .client = client, .ident = from[dot + 1 ..], .cat = c.name };
                }
            }
        }
        if (self.findCatalog(from)) |c| {
            if (c.kind == .iceberg_rest) {
                const client = c.rest orelse self.rest orelse return null;
                return .{ .client = client, .ident = "", .cat = c.name };
            }
        }
        if (self.rest) |client| {
            return .{ .client = client, .ident = from, .cat = self.default_catalog };
        }
        return null;
    }
};

fn queryNeedsSession(q: sql.Query) bool {
    if (q.ctes.len > 0 or q.from_sub != null or q.union_right != null) return true;
    if (q.join) |j| {
        if (j.sub != null) return true;
    }
    if (predHasSub(q.where) or predHasSub(q.having)) return true;
    for (q.items) |item| {
        switch (item) {
            .expr => |ex| if (valueHasSub(ex.expr)) return true,
            .case => |cs| {
                for (cs.arms) |arm| {
                    if (predHasSub(arm.when)) return true;
                }
            },
            else => {},
        }
    }
    return false;
}

fn valueHasSub(e: *const sql.Expr) bool {
    return switch (e.*) {
        .subquery => true,
        .binary => |b| valueHasSub(b.left) or valueHasSub(b.right),
        .unary_minus => |a| valueHasSub(a),
        .call => |c| blk: {
            for (c.args) |arg| {
                if (valueHasSub(arg)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn predHasSub(expr: ?*sql.BoolExpr) bool {
    const e = expr orelse return false;
    return switch (e.*) {
        .in_query, .cmp_query, .exists => true,
        .cmp => |c| valueHasSub(c.left) or valueHasSub(c.right),
        .@"and", .@"or" => |b| predHasSub(b.left) or predHasSub(b.right),
        else => false,
    };
}

fn batchToLiterals(a: std.mem.Allocator, batch: Batch) ![]const sql.Literal {
    if (batch.columns.len != 1) return error.TypeMismatch;
    const col = batch.columns[0];
    const out = try a.alloc(sql.Literal, batch.len);
    var i: usize = 0;
    while (i < batch.len) : (i += 1) {
        out[i] = try cellToLiteral(a, col, i);
    }
    return out;
}

fn cellToLiteral(a: std.mem.Allocator, col: Column, row: usize) !sql.Literal {
    if (col.isNull(row)) return .null;
    return switch (col.data_type) {
        .int32 => .{ .int = col.i32s[row] },
        .int64, .timestamp, .timestamptz => .{ .int = col.i64s[row] },
        .float32 => .{ .float = col.f32s[row] },
        .float64 => .{ .float = col.f64s[row] },
        .utf8 => .{ .string = try a.dupe(u8, col.strAt(row)) },
        .boolean => .{ .int = if (col.bools[row] != 0) 1 else 0 },
        else => error.TypeMismatch,
    };
}

fn toIcebergAsOf(as_of: ?sql.AsOf) ?iceberg.AsOf {
    const spec = as_of orelse return null;
    return switch (spec) {
        .snapshot => |id| .{ .snapshot = id },
        .timestamp_ms => |ts| .{ .timestamp_ms = ts },
    };
}

const SystemKind = enum { catalogs, tables, snapshots, files };

const SystemTable = struct {
    kind: SystemKind,
    ident: []const u8,
};

fn parseGlacierSystem(from: []const u8) error{TableNotFound}!?SystemTable {
    if (!std.ascii.startsWithIgnoreCase(from, "glacier.")) return null;
    const rest = from["glacier.".len..];
    if (std.ascii.eqlIgnoreCase(rest, "catalogs")) return .{ .kind = .catalogs, .ident = "" };
    if (std.ascii.eqlIgnoreCase(rest, "tables")) return .{ .kind = .tables, .ident = "" };
    if (std.ascii.eqlIgnoreCase(rest, "snapshots")) return .{ .kind = .snapshots, .ident = "" };
    if (std.ascii.eqlIgnoreCase(rest, "files")) return .{ .kind = .files, .ident = "" };
    return error.TableNotFound;
}

fn snapshotsBatch(a: std.mem.Allocator, meta: iceberg.TableMetadata) !Batch {
    const n = meta.snapshots.len;
    const ids = try a.alloc(i64, n);
    const ts = try a.alloc(i64, n);
    const seq = try a.alloc(i64, n);
    const schema_ids = try a.alloc(i64, n);
    const valid = try a.alloc(u8, n);
    const lists = try a.alloc([]const u8, n);
    for (meta.snapshots, 0..) |s, i| {
        ids[i] = s.snapshot_id;
        ts[i] = s.timestamp_ms;
        seq[i] = s.sequence_number;
        if (s.schema_id) |sid| {
            schema_ids[i] = sid;
            valid[i] = 1;
        } else {
            schema_ids[i] = 0;
            valid[i] = 0;
        }
        lists[i] = s.manifest_list;
    }
    var schema_col = try catalog.i64Col(a, "schema_id", schema_ids);
    if (n > 0) schema_col.valid = valid;
    const columns = try a.alloc(Column, 5);
    columns[0] = try catalog.i64Col(a, "snapshot_id", ids);
    columns[1] = try catalog.i64Col(a, "timestamp_ms", ts);
    columns[2] = try catalog.i64Col(a, "sequence_number", seq);
    columns[3] = schema_col;
    columns[4] = try catalog.utf8Col(a, "manifest_list", lists);
    return .{ .columns = columns, .len = n };
}

fn tableFilesList(a: std.mem.Allocator, table: iceberg.Table) ![]const iceberg.DataFile {
    const n = table.files.len + table.delete_files.len;
    if (n == table.files.len) return table.files;
    const all = try a.alloc(iceberg.DataFile, n);
    @memcpy(all[0..table.files.len], table.files);
    @memcpy(all[table.files.len..], table.delete_files);
    return all;
}

fn filesBatch(a: std.mem.Allocator, files: []const iceberg.DataFile) !Batch {
    const n = files.len;
    const paths = try a.alloc([]const u8, n);
    const formats = try a.alloc([]const u8, n);
    const counts = try a.alloc(i64, n);
    const contents = try a.alloc(i64, n);
    for (files, 0..) |f, i| {
        paths[i] = f.path;
        formats[i] = f.format.label();
        counts[i] = f.record_count;
        contents[i] = f.content;
    }
    const columns = try a.alloc(Column, 4);
    columns[0] = try catalog.utf8Col(a, "path", paths);
    columns[1] = try catalog.utf8Col(a, "format", formats);
    columns[2] = try catalog.i64Col(a, "record_count", counts);
    columns[3] = try catalog.i64Col(a, "content", contents);
    return .{ .columns = columns, .len = n };
}

fn describeSystem(a: std.mem.Allocator, kind: SystemKind) !Batch {
    const cols: []const [3][]const u8 = switch (kind) {
        .catalogs => &.{
            .{ "name", "string", "true" },
            .{ "type", "string", "true" },
        },
        .tables => &.{
            .{ "name", "string", "true" },
            .{ "catalog", "string", "true" },
            .{ "namespace", "string", "true" },
        },
        .snapshots => &.{
            .{ "snapshot_id", "long", "true" },
            .{ "timestamp_ms", "long", "true" },
            .{ "sequence_number", "long", "true" },
            .{ "schema_id", "long", "false" },
            .{ "manifest_list", "string", "true" },
        },
        .files => &.{
            .{ "path", "string", "true" },
            .{ "format", "string", "true" },
            .{ "record_count", "long", "true" },
            .{ "content", "long", "true" },
        },
    };
    const names = try a.alloc([]const u8, cols.len);
    const types = try a.alloc([]const u8, cols.len);
    const required = try a.alloc([]const u8, cols.len);
    for (cols, 0..) |c, i| {
        names[i] = c[0];
        types[i] = c[1];
        required[i] = c[2];
    }
    return catalog.utf8Cols(a, &.{ "name", "type", "required" }, &.{ names, types, required });
}

fn filesIdentMatches(entry: catalog.Entry, ident: []const u8) bool {
    if (entry.table_name.len == 0) return false;
    if (std.ascii.eqlIgnoreCase(ident, entry.table_name)) return true;
    const stem = std.fs.path.stem(ident);
    if (std.ascii.eqlIgnoreCase(stem, entry.table_name)) return true;
    if (std.mem.lastIndexOfScalar(u8, ident, '.')) |i| {
        if (std.ascii.eqlIgnoreCase(ident[i + 1 ..], entry.table_name)) return true;
    }
    return false;
}

fn cacheKeyBelongsTo(key: []const u8, cat: []const u8) bool {
    if (key.len <= cat.len) return false;
    if (!std.ascii.eqlIgnoreCase(key[0..cat.len], cat)) return false;
    return key[cat.len] == 0x1f;
}

fn describeFields(a: std.mem.Allocator, fields: []const iceberg.SchemaField) !Batch {
    const n = fields.len;
    const names = try a.alloc([]const u8, n);
    const types = try a.alloc([]const u8, n);
    const required = try a.alloc([]const u8, n);
    for (fields, 0..) |f, i| {
        names[i] = try a.dupe(u8, f.name);
        if (std.ascii.eqlIgnoreCase(f.type_name, "decimal")) {
            types[i] = try std.fmt.allocPrint(a, "decimal({d},{d})", .{ f.decimal_precision, f.decimal_scale });
        } else {
            types[i] = try a.dupe(u8, f.type_name);
        }
        required[i] = if (f.required) "true" else "false";
    }
    return catalog.utf8Cols(a, &.{ "name", "type", "required" }, &.{ names, types, required });
}

fn parquetTypeLabel(a: std.mem.Allocator, reader: parquet.Reader, index: i32) ![]u8 {
    const meta = reader.column(index) orelse return error.ParquetOpenFailed;
    const logical = parquet.columnLogical(reader, index);
    const lid: parquet.LogicalId = @enumFromInt(logical.id);
    if (lid == .decimal) {
        return std.fmt.allocPrint(a, "decimal({d},{d})", .{ logical.precision, logical.scale });
    }
    const name: []const u8 = switch (lid) {
        .string => "string",
        .date => "date",
        .timestamp => if (logical.utc != 0) "timestamptz" else "timestamp",
        .uuid => "uuid",
        else => switch (meta.physical_type) {
            .boolean => "boolean",
            .int32 => "int",
            .int64 => "long",
            .float => "float",
            .double => "double",
            .byte_array, .fixed_len_byte_array => "string",
            .int96 => "timestamp",
            else => "binary",
        },
    };
    return a.dupe(u8, name);
}

fn fieldsFromColDefs(a: std.mem.Allocator, cols: []const sql.ColDef) ![]iceberg.SchemaField {
    const fields = try a.alloc(iceberg.SchemaField, cols.len);
    for (cols, 0..) |c, i| {
        fields[i] = .{
            .id = @intCast(i + 1),
            .name = try a.dupe(u8, c.name),
            .required = true,
            .type_name = try a.dupe(u8, c.type_name),
        };
    }
    return fields;
}

fn fieldsFromBatch(a: std.mem.Allocator, input: Batch) ![]iceberg.SchemaField {
    const fields = try a.alloc(iceberg.SchemaField, input.columns.len);
    for (input.columns, 0..) |col, i| {
        fields[i] = .{
            .id = @intCast(i + 1),
            .name = try a.dupe(u8, col.name),
            .required = true,
            .type_name = try a.dupe(u8, dataTypeIcebergName(col.data_type)),
        };
    }
    return fields;
}

fn dataTypeIcebergName(dt: DataType) []const u8 {
    return switch (dt) {
        .boolean => "boolean",
        .int32 => "int",
        .int64 => "long",
        .float32 => "float",
        .float64 => "double",
        .utf8 => "string",
        .timestamp => "timestamp",
        .timestamptz => "timestamptz",
        .uuid => "uuid",
        .decimal128 => "decimal",
    };
}

fn icebergNameDataType(name: []const u8) !DataType {
    if (std.ascii.eqlIgnoreCase(name, "boolean")) return .boolean;
    if (std.ascii.eqlIgnoreCase(name, "int") or std.ascii.eqlIgnoreCase(name, "date")) return .int32;
    if (std.ascii.eqlIgnoreCase(name, "long")) return .int64;
    if (std.ascii.eqlIgnoreCase(name, "float")) return .float32;
    if (std.ascii.eqlIgnoreCase(name, "double")) return .float64;
    if (std.ascii.eqlIgnoreCase(name, "string") or std.ascii.eqlIgnoreCase(name, "binary") or
        std.ascii.eqlIgnoreCase(name, "list")) return .utf8;
    if (std.ascii.eqlIgnoreCase(name, "timestamp") or std.ascii.eqlIgnoreCase(name, "timestamp_ntz")) return .timestamp;
    if (std.ascii.eqlIgnoreCase(name, "timestamptz") or std.ascii.eqlIgnoreCase(name, "timestamp_tz")) return .timestamptz;
    if (std.ascii.eqlIgnoreCase(name, "uuid")) return .uuid;
    if (std.ascii.eqlIgnoreCase(name, "decimal")) return .decimal128;
    return error.UnsupportedType;
}

fn alignToFields(a: std.mem.Allocator, input: Batch, fields: []const iceberg.SchemaField) !Batch {
    if (input.columns.len > fields.len) return error.SchemaMismatch;
    if (inputNamesMatchFields(input, fields)) return alignToFieldsByNamePadded(a, input, fields);
    if (input.columns.len < fields.len) {
        for (fields[input.columns.len..]) |f| {
            if (f.required) return error.SchemaMismatch;
        }
    }
    const cols = try a.alloc(Column, fields.len);
    for (fields, 0..) |f, i| {
        const want = try icebergNameDataType(f.type_name);
        var col = if (i < input.columns.len) input.columns[i] else try nullFieldColumn(a, f, input.len);
        if (col.isNull(0) and input.len > 0) {
            var row: usize = 0;
            while (row < input.len) : (row += 1) {
                if (col.isNull(row) and f.required) return error.TypeMismatch;
            }
        }
        if (col.data_type != want) col = try coerceColumn(a, col, want, input.len);
        col.name = f.name;
        cols[i] = col;
    }
    return .{ .columns = cols, .len = input.len };
}

fn inputNamesMatchFields(input: Batch, fields: []const iceberg.SchemaField) bool {
    if (input.columns.len == 0) return false;
    for (input.columns) |col| {
        var found = false;
        for (fields) |f| {
            if (std.ascii.eqlIgnoreCase(col.name, f.name)) {
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

fn nullFieldColumn(a: std.mem.Allocator, f: iceberg.SchemaField, len: usize) !Column {
    const want = try icebergNameDataType(f.type_name);
    var col = try fillLiteralColumn(a, f.name, want, len, .null);
    col.decimal_precision = f.decimal_precision;
    col.decimal_scale = f.decimal_scale;
    return col;
}

fn countCol(a: std.mem.Allocator, name: []const u8, value: i64) !Batch {
    const n = try a.alloc(i64, 1);
    n[0] = value;
    const columns = try a.alloc(Column, 1);
    columns[0] = try catalog.i64Col(a, name, n);
    return .{ .columns = columns, .len = 1 };
}

fn tableStem(ident: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, ident, '.')) |dot| return ident[dot + 1 ..];
    return ident;
}

fn qualNameStr(a: std.mem.Allocator, q: sql.QualName) ![]const u8 {
    if (q.qualifier) |qual| return std.fmt.allocPrint(a, "{s}.{s}", .{ qual, q.name });
    return q.name;
}

fn projectFields(
    a: std.mem.Allocator,
    input: Batch,
    fields: []const iceberg.SchemaField,
    qual: []const u8,
) !Batch {
    const cols = try a.alloc(Column, fields.len);
    for (fields, 0..) |f, i| {
        const qname = try std.fmt.allocPrint(a, "{s}.{s}", .{ qual, f.name });
        const idx = input.lookup(qname) catch try input.lookup(f.name);
        var col = input.columns[idx];
        col.name = f.name;
        cols[i] = col;
    }
    return .{ .columns = cols, .len = input.len };
}

fn fillLiteralColumn(a: std.mem.Allocator, name: []const u8, want: DataType, len: usize, lit: sql.Literal) !Column {
    var col: Column = .{ .name = name, .data_type = want, .len = len };
    switch (lit) {
        .null => {
            const valid = try a.alloc(u8, len);
            @memset(valid, 0);
            col.valid = valid;
            switch (want) {
                .int64, .timestamp, .timestamptz => col.i64s = try a.alloc(i64, len),
                .int32 => col.i32s = try a.alloc(i32, len),
                .float64 => col.f64s = try a.alloc(f64, len),
                .float32 => col.f32s = try a.alloc(f32, len),
                .boolean => col.bools = try a.alloc(u8, len),
                .utf8 => {
                    const offs = try a.alloc(u32, len + 1);
                    @memset(offs, 0);
                    col.utf8 = .{ .offsets = offs, .bytes = &.{} };
                },
                .uuid => col.uuids = try a.alloc([16]u8, len),
                .decimal128 => col.i128s = try a.alloc(i128, len),
            }
        },
        .int => |v| switch (want) {
            .int64, .timestamp, .timestamptz => {
                const i64s = try a.alloc(i64, len);
                @memset(i64s, v);
                col.i64s = i64s;
            },
            .int32 => {
                const i32s = try a.alloc(i32, len);
                @memset(i32s, @intCast(v));
                col.i32s = i32s;
            },
            else => return error.TypeMismatch,
        },
        .float => |v| switch (want) {
            .float64 => {
                const f64s = try a.alloc(f64, len);
                @memset(f64s, v);
                col.f64s = f64s;
            },
            .float32 => {
                const f32s = try a.alloc(f32, len);
                @memset(f32s, @floatCast(v));
                col.f32s = f32s;
            },
            else => return error.TypeMismatch,
        },
        .string => |s| {
            if (want != .utf8) return error.TypeMismatch;
            const offs = try a.alloc(u32, len + 1);
            const bytes = try a.alloc(u8, s.len * len);
            var off: u32 = 0;
            var row: usize = 0;
            while (row < len) : (row += 1) {
                offs[row] = off;
                if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
                off += @intCast(s.len);
            }
            offs[len] = off;
            col.utf8 = .{ .offsets = offs, .bytes = bytes };
        },
    }
    return col;
}

fn applyAssignments(
    a: std.mem.Allocator,
    dest_rows: Batch,
    lookup_rows: Batch,
    fields: []const iceberg.SchemaField,
    dest_qual: ?[]const u8,
    assignments: []const sql.Assignment,
) !Batch {
    const cols = try a.alloc(Column, dest_rows.columns.len);
    @memcpy(cols, dest_rows.columns);
    for (assignments) |asg| {
        const fi = blk: {
            for (fields, 0..) |f, i| {
                if (std.ascii.eqlIgnoreCase(f.name, asg.column)) break :blk i;
            }
            return error.ColumnNotFound;
        };
        const want = try icebergNameDataType(fields[fi].type_name);
        cols[fi] = switch (asg.value) {
            .literal => |lit| try fillLiteralColumn(a, fields[fi].name, want, dest_rows.len, lit),
            .column => |q| blk: {
                const n = try qualNameStr(a, q);
                const idx = lookup_rows.lookup(n) catch return error.ColumnNotFound;
                var col = lookup_rows.columns[idx];
                col.name = fields[fi].name;
                if (col.data_type != want) col = try coerceColumn(a, col, want, dest_rows.len);
                break :blk col;
            },
        };
        _ = dest_qual;
    }
    return .{ .columns = cols, .len = dest_rows.len };
}

fn buildMergeInsert(
    a: std.mem.Allocator,
    unmatched: Batch,
    fields: []const iceberg.SchemaField,
    src_qual: []const u8,
    spec: sql.MergeInsert,
) !Batch {
    if (spec.values.len == 0) return projectFields(a, unmatched, fields, src_qual);
    const cols = try a.alloc(Column, fields.len);
    for (fields, 0..) |f, i| {
        var found: ?sql.AssignValue = null;
        if (spec.columns.len == spec.values.len) {
            for (spec.columns, spec.values) |cname, val| {
                if (std.ascii.eqlIgnoreCase(cname, f.name)) {
                    found = val;
                    break;
                }
            }
        } else if (spec.columns.len == 0 and i < spec.values.len) {
            found = spec.values[i];
        }
        const want = try icebergNameDataType(f.type_name);
        cols[i] = if (found) |val| switch (val) {
            .literal => |lit| try fillLiteralColumn(a, f.name, want, unmatched.len, lit),
            .column => |q| blk: {
                const n = try qualNameStr(a, q);
                const idx = unmatched.lookup(n) catch return error.ColumnNotFound;
                var col = unmatched.columns[idx];
                col.name = f.name;
                if (col.data_type != want) col = try coerceColumn(a, col, want, unmatched.len);
                break :blk col;
            },
        } else blk: {
            const qname = try std.fmt.allocPrint(a, "{s}.{s}", .{ src_qual, f.name });
            const idx = unmatched.lookup(qname) catch unmatched.lookup(f.name) catch return error.ColumnNotFound;
            var col = unmatched.columns[idx];
            col.name = f.name;
            break :blk col;
        };
    }
    return .{ .columns = cols, .len = unmatched.len };
}

fn alignToFieldsByName(a: std.mem.Allocator, input: Batch, fields: []const iceberg.SchemaField) !Batch {
    return alignToFieldsByNamePadded(a, input, fields);
}

fn alignToFieldsByNamePadded(a: std.mem.Allocator, input: Batch, fields: []const iceberg.SchemaField) !Batch {
    const cols = try a.alloc(Column, fields.len);
    for (fields, 0..) |f, i| {
        const want = try icebergNameDataType(f.type_name);
        var col = if (input.lookup(f.name)) |idx|
            input.columns[idx]
        else |_| blk: {
            if (f.required) return error.SchemaMismatch;
            break :blk try nullFieldColumn(a, f, input.len);
        };
        if (col.data_type != want) col = try coerceColumn(a, col, want, input.len);
        col.name = f.name;
        cols[i] = col;
    }
    return .{ .columns = cols, .len = input.len };
}

fn concatBatches(a: std.mem.Allocator, left: Batch, right: Batch) !Batch {
    if (left.len == 0) return right;
    if (right.len == 0) return left;
    return physical.concatUnion(a, left, right);
}

fn coerceColumn(a: std.mem.Allocator, col: Column, want: DataType, len: usize) !Column {
    var out = col;
    out.data_type = want;
    if (col.data_type == want) return out;
    if ((col.data_type == .int64 or col.data_type.storesI64()) and want == .int32) {
        const i32s = try a.alloc(i32, len);
        for (0..len) |row| i32s[row] = @intCast(col.i64s[row]);
        out.i32s = i32s;
        return out;
    }
    if (col.data_type == .int32 and (want == .int64 or want.storesI64())) {
        const i64s = try a.alloc(i64, len);
        for (0..len) |row| i64s[row] = col.i32s[row];
        out.i64s = i64s;
        return out;
    }
    if (col.data_type == .float32 and want == .float64) {
        const f64s = try a.alloc(f64, len);
        for (0..len) |row| f64s[row] = col.f32s[row];
        out.f64s = f64s;
        return out;
    }
    return error.TypeMismatch;
}

fn isMissingLocal(io: std.Io, path: []const u8) bool {
    if (std.mem.indexOf(u8, path, "://") != null) return false;
    std.Io.Dir.cwd().access(io, path, .{}) catch return true;
    return false;
}

fn isParquetPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".parquet");
}

fn isAvroPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".avro");
}

fn isGlacierPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".glacier");
}

fn isRestUri(path: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(path, "http://") or std.ascii.startsWithIgnoreCase(path, "https://");
}

fn isRestCatalogUri(path: []const u8) bool {
    return isRestUri(path) and !isParquetPath(path) and !isAvroPath(path) and !isGlacierPath(path);
}

pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    batch: Batch,
    yielded: bool,
    scan_stats: ScanStats = .{},

    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn schema(self: Result) []const Column {
        return self.batch.columns;
    }

    pub fn nextBatch(self: *Result) ?Batch {
        if (self.yielded) return null;
        self.yielded = true;
        return self.batch;
    }
};

pub fn writeSalesFixture(path: [:0]const u8) !void {
    try parquet.writeSalesFixture(path);
}

fn expectPricesAbove(batch: Batch, min_exclusive: i64) !void {
    const idx = batch.columnIndex("price") orelse return error.ColumnNotFound;
    const prices = batch.columns[idx].i64s;
    var i: usize = 0;
    while (i < batch.len) : (i += 1) {
        try std.testing.expect(prices[i] > min_exclusive);
    }
}

test "SELECT * WHERE LIMIT projection" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/sales.parquet");

    var session = try Session.open(gpa, io, "/tmp/sales.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT * FROM sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 10), b.len);
        try std.testing.expectEqual(@as(usize, 3), b.columns.len);
        try std.testing.expect(result.nextBatch() == null);
    }

    {
        var result = try session.execute("SELECT * FROM sales WHERE price > 100");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
        try expectPricesAbove(b, 100);
    }

    {
        var result = try session.execute("SELECT * FROM sales LIMIT 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT id FROM sales WHERE price > 100 LIMIT 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.columns.len);
        try std.testing.expectEqualStrings("id", b.columns[0].name);
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT id FROM sales WHERE category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
    }

    {
        var result = try session.execute("SELECT COUNT(*) FROM sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("count", b.columns[0].name);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute("SELECT SUM(price), AVG(price), MIN(price), MAX(price) FROM sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1275), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(f64, 127.5), b.columns[1].f64s[0]);
        try std.testing.expectEqual(@as(i64, 50), b.columns[2].i64s[0]);
        try std.testing.expectEqual(@as(i64, 300), b.columns[3].i64s[0]);
    }

    {
        var result = try session.execute("SELECT COUNT(*) FROM sales WHERE price > 100");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute("SELECT category, COUNT(*) AS n, SUM(price) FROM sales GROUP BY category");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        const cat = b.columnIndex("category").?;
        const n = b.columnIndex("n").?;
        const sum_i = b.columnIndex("sum_price").?;
        var seen_fruit = false;
        var seen_veg = false;
        var seen_dairy = false;
        var i: usize = 0;
        while (i < b.len) : (i += 1) {
            const name = b.columns[cat].strAt(i);
            const count = b.columns[n].i64s[i];
            const sumv = b.columns[sum_i].i64s[i];
            if (std.mem.eql(u8, name, "fruit")) {
                try std.testing.expectEqual(@as(i64, 5), count);
                try std.testing.expectEqual(@as(i64, 445), sumv);
                seen_fruit = true;
            } else if (std.mem.eql(u8, name, "veg")) {
                try std.testing.expectEqual(@as(i64, 3), count);
                try std.testing.expectEqual(@as(i64, 330), sumv);
                seen_veg = true;
            } else if (std.mem.eql(u8, name, "dairy")) {
                try std.testing.expectEqual(@as(i64, 2), count);
                try std.testing.expectEqual(@as(i64, 500), sumv);
                seen_dairy = true;
            } else return error.UnexpectedCategory;
        }
        try std.testing.expect(seen_fruit and seen_veg and seen_dairy);
    }
}

test "OR ORDER BY HAVING OFFSET" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/sales.parquet");

    var session = try Session.open(gpa, io, "/tmp/sales.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT * FROM sales WHERE price > 100 OR category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 9), b.len);
    }

    {
        var result = try session.execute("SELECT * FROM sales ORDER BY price DESC");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 300), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 9), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute("SELECT id FROM sales ORDER BY price");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT category, COUNT(*) AS n FROM sales GROUP BY category HAVING COUNT(*) > 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        const cat = b.columnIndex("category").?;
        var seen_fruit = false;
        var seen_veg = false;
        var i: usize = 0;
        while (i < b.len) : (i += 1) {
            const name = b.columns[cat].strAt(i);
            if (std.mem.eql(u8, name, "fruit")) seen_fruit = true;
            if (std.mem.eql(u8, name, "veg")) seen_veg = true;
            try std.testing.expect(!std.mem.eql(u8, name, "dairy"));
        }
        try std.testing.expect(seen_fruit and seen_veg);
    }

    {
        var result = try session.execute("SELECT category, COUNT(*) AS n FROM sales GROUP BY category HAVING n > 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
    }

    {
        var result = try session.execute("SELECT * FROM sales LIMIT 2 OFFSET 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT * FROM sales ORDER BY id LIMIT 2 OFFSET 8");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 9), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT * FROM sales OFFSET 8 LIMIT 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 9), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT * WHERE price > 100 LIMIT 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try expectPricesAbove(b, 100);
    }
}

test "stream limit offset across parquet row groups" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeRowGroupsFixture("/tmp/glacier-stream-rgs.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier-stream-rgs.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT * LIMIT 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT * OFFSET 4 LIMIT 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 6), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute("SELECT * OFFSET 3");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 6), b.columns[0].i64s[2]);
    }

    {
        var result = try session.execute("SELECT * WHERE id > 4 LIMIT 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute("SELECT * OFFSET 10");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 0), b.len);
    }
}

test "iceberg prune does not open files outside bounds" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/iceberg_prune");

    var session = try Session.open(gpa, io, "/tmp/iceberg_prune");
    defer session.close();
    try std.testing.expectEqual(@as(usize, 2), session.files.len);

    {
        var result = try session.execute("SELECT * FROM iceberg_prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 10), b.len);
        try std.testing.expectEqual(@as(usize, 2), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 0), result.scan_stats.files_pruned);
    }

    {
        var result = try session.execute("SELECT * WHERE price > 180");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_pruned);
        try expectPricesAbove(b, 180);
    }

    {
        var result = try session.execute("SELECT * FROM iceberg_prune WHERE price < 60");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i64, 50), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_pruned);
    }

    {
        var result = try session.execute("SELECT category, COUNT(*) AS n FROM iceberg_prune GROUP BY category");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
    }
}

test "iceberg lake bucket partition deletes and schema evolution" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writeLakeFixture(gpa, io, "/tmp/glacier_iceberg_lake");
    var session = try Session.open(gpa, io, "/tmp/glacier_iceberg_lake");
    defer session.close();

    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_lake");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 8), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute("SELECT id FROM glacier_iceberg_lake ORDER BY id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 8), b.len);
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 9), b.columns[0].i64s[7]);
        var i: usize = 0;
        while (i < b.len) : (i += 1) {
            try std.testing.expect(b.columns[0].i64s[i] != 1);
            try std.testing.expect(b.columns[0].i64s[i] != 10);
        }
    }

    {
        var result = try session.execute("SELECT note FROM glacier_iceberg_lake LIMIT 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(DataType.utf8, b.columns[0].data_type);
        try std.testing.expect(b.columns[0].isNull(0));
    }

    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_lake WHERE id = 2");
        defer result.deinit();
        _ = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expect(result.scan_stats.files_pruned >= 1);
    }
}

test "SELECT WHERE on avro data file" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const prices = [_]i64{ 50, 80, 100, 120, 150, 200, 90, 110, 300, 75 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg", "veg", "fruit", "dairy", "fruit", "veg", "dairy", "fruit" };
    try avro.writeSalesRows("/tmp/sales.avro", &ids, &prices, &cats);

    var session = try Session.open(gpa, io, "/tmp/sales.avro");
    defer session.close();

    {
        var result = try session.execute("SELECT * FROM sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 10), b.len);
        try std.testing.expectEqual(@as(usize, 3), b.columns.len);
    }

    {
        var result = try session.execute("SELECT * FROM sales WHERE price > 100");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
        try expectPricesAbove(b, 100);
    }

    {
        var result = try session.execute("SELECT category, COUNT(*) AS n FROM sales GROUP BY category");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
    }

    {
        var result = try session.execute("SELECT * WHERE price > 100 ORDER BY price LIMIT 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 110), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 120), b.columns[1].i64s[1]);
    }
}

test "SELECT WHERE on deflate and snappy avro" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const prices = [_]i64{ 50, 80, 100, 120, 150, 200, 90, 110, 300, 75 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg", "veg", "fruit", "dairy", "fruit", "veg", "dairy", "fruit" };
    const cases = [_]struct { path: [:0]const u8, codec: avro.Codec }{
        .{ .path = "/tmp/sales-deflate.avro", .codec = .deflate },
        .{ .path = "/tmp/sales-snappy.avro", .codec = .snappy },
    };
    for (cases) |cse| {
        try avro.writeSalesRowsCodec(cse.path, &ids, &prices, &cats, cse.codec);
        var session = try Session.open(gpa, io, cse.path);
        defer session.close();
        var result = try session.execute("SELECT COUNT(*) WHERE price > 100");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
}

test "query without FROM uses the connected parquet avro and iceberg" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/sales.parquet");
    {
        var session = try Session.open(gpa, io, "/tmp/sales.parquet");
        defer session.close();
        var result = try session.execute("SELECT * WHERE price > 100");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
        try expectPricesAbove(b, 100);
    }

    const ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const prices = [_]i64{ 50, 80, 100, 120, 150, 200, 90, 110, 300, 75 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg", "veg", "fruit", "dairy", "fruit", "veg", "dairy", "fruit" };
    try avro.writeSalesRows("/tmp/sales.avro", &ids, &prices, &cats);
    {
        var session = try Session.open(gpa, io, "/tmp/sales.avro");
        defer session.close();
        var result = try session.execute("SELECT COUNT(*)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }

    try iceberg.writePruneFixture(gpa, io, "/tmp/iceberg_prune_nofrom");
    {
        var session = try Session.open(gpa, io, "/tmp/iceberg_prune_nofrom");
        defer session.close();
        var result = try session.execute("SELECT * WHERE price > 180");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_pruned);
        try expectPricesAbove(b, 180);
    }
}

test "GlacierError carries a message not the error name" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/sales.parquet");
    var session = try Session.open(gpa, io, "/tmp/sales.parquet");
    defer session.close();

    try std.testing.expectError(error.UnsupportedJoin, session.execute("SELECT * FROM a JOIN b"));
    const ge = session.lastError() orelse return error.MissingGlacierError;
    try std.testing.expectEqualStrings("JOIN is not supported", ge.message);
    try std.testing.expectEqual(errmod.Code.unsupported_join, ge.code);

    try std.testing.expectError(error.TableNotFound, session.execute("SELECT * FROM nope"));
    const missing = session.lastError() orelse return error.MissingGlacierError;
    try std.testing.expectEqualStrings("table not found", missing.message);

    {
        var sub = try session.execute("SELECT (SELECT 1) FROM sales");
        defer sub.deinit();
        const b = sub.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 10), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    try std.testing.expectError(
        error.UnsupportedSql,
        session.execute("SELECT * FROM sales a WHERE a.id IN (SELECT b.id FROM sales b WHERE a.id > 0)"),
    );
}

test "distinct order by scalars" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/sales.parquet");
    var session = try Session.open(gpa, io, "/tmp/sales.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT DISTINCT category FROM sales ORDER BY category");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(2));
    }

    {
        var result = try session.execute("SELECT * FROM sales ORDER BY category, price DESC");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        const cat = b.columnIndex("category").?;
        const price = b.columnIndex("price").?;
        try std.testing.expectEqualStrings("dairy", b.columns[cat].strAt(0));
        try std.testing.expectEqual(@as(i64, 300), b.columns[price].i64s[0]);
        try std.testing.expectEqualStrings("dairy", b.columns[cat].strAt(1));
        try std.testing.expectEqual(@as(i64, 200), b.columns[price].i64s[1]);
    }

    {
        var result = try session.execute("SELECT abs(price) AS p, coalesce(category, 'x') AS c FROM sales WHERE id = 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i64, 50), b.columns[0].i64s[0]);
        try std.testing.expectEqualStrings("fruit", b.columns[1].strAt(0));
    }

    {
        var result = try session.execute("SELECT CAST(id AS DOUBLE) AS d FROM sales WHERE id = 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(f64, 1.0), b.columns[0].f64s[0]);
    }
}

test "round and abs on types fixture" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeTypesFixture("/tmp/glacier-types-sql.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier-types-sql.parquet");
    defer session.close();
    var result = try session.execute("SELECT round(f32) AS r, abs(i32) AS a");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqual(@as(f32, 2.0), b.columns[0].f32s[0]);
    try std.testing.expectEqual(@as(f32, 3.0), b.columns[0].f32s[1]);
    try std.testing.expectEqual(@as(f32, 4.0), b.columns[0].f32s[2]);
    try std.testing.expectEqual(@as(i32, 1), b.columns[1].i32s[0]);
}

test "parquet types fixture projection row groups and nested" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeTypesFixture("/tmp/glacier-types.parquet");
    {
        var session = try Session.open(gpa, io, "/tmp/glacier-types.parquet");
        defer session.close();

        {
            var result = try session.execute("SELECT *");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 3), b.len);
            try std.testing.expectEqual(@as(usize, 6), b.columns.len);
            try std.testing.expectEqual(DataType.boolean, b.columns[0].data_type);
            try std.testing.expectEqual(DataType.int32, b.columns[1].data_type);
            try std.testing.expectEqual(DataType.int64, b.columns[2].data_type);
            try std.testing.expectEqual(DataType.float32, b.columns[3].data_type);
            try std.testing.expectEqual(DataType.float64, b.columns[4].data_type);
            try std.testing.expectEqual(DataType.utf8, b.columns[5].data_type);
            try std.testing.expectEqualSlices(u8, &.{ 1, 0, 1 }, b.columns[0].bools[0..3]);
            try std.testing.expectEqualSlices(i32, &.{ 1, 2, 3 }, b.columns[1].i32s[0..3]);
            try std.testing.expectEqualSlices(i64, &.{ 10, 20, 30 }, b.columns[2].i64s[0..3]);
            try std.testing.expectEqual(@as(f32, 1.5), b.columns[3].f32s[0]);
            try std.testing.expectEqual(@as(f32, 2.5), b.columns[3].f32s[1]);
            try std.testing.expectEqual(@as(f32, 3.5), b.columns[3].f32s[2]);
            try std.testing.expectEqual(@as(f64, 1.25), b.columns[4].f64s[0]);
            try std.testing.expectEqualStrings("a", b.columns[5].strAt(0));
            try std.testing.expectEqualStrings("c", b.columns[5].strAt(2));
        }

        {
            var result = try session.execute("SELECT name WHERE i32 > 1");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 1), b.columns.len);
            try std.testing.expectEqualStrings("name", b.columns[0].name);
            try std.testing.expectEqual(@as(usize, 2), b.len);
            try std.testing.expectEqualStrings("b", b.columns[0].strAt(0));
            try std.testing.expectEqualStrings("c", b.columns[0].strAt(1));
        }
    }

    try parquet.writeRowGroupsFixture("/tmp/glacier-rowgroups.parquet");
    {
        var session = try Session.open(gpa, io, "/tmp/glacier-rowgroups.parquet");
        defer session.close();
        var result = try session.execute("SELECT id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 6), b.len);
        try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5, 6 }, b.columns[0].i64s[0..6]);
    }

    try parquet.writeNestedFixture("/tmp/glacier-nested.parquet");
    {
        var session = try Session.open(gpa, io, "/tmp/glacier-nested.parquet");
        defer session.close();
        var result = try session.execute("SELECT *");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(DataType.utf8, b.columns[0].data_type);
        try std.testing.expectEqualStrings("[1, 2, 3]", b.columns[0].strAt(0));
    }

    try parquet.writeStructFixture("/tmp/glacier-struct.parquet");
    {
        var session = try Session.open(gpa, io, "/tmp/glacier-struct.parquet");
        defer session.close();
        var result = try session.execute("SELECT *");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(usize, 2), b.columns.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        const city_i: usize = if (std.ascii.eqlIgnoreCase(b.columns[1].name, "city") or
            std.mem.endsWith(u8, b.columns[1].name, "city")) 1 else 0;
        try std.testing.expectEqualStrings("oslo", b.columns[city_i].strAt(0));
        try std.testing.expectEqualStrings("bergen", b.columns[city_i].strAt(1));
    }
}

test "SELECT 1 without a table" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    var result = try session.execute("select 1");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(usize, 1), b.len);
    try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    try std.testing.expectEqualStrings("1", b.columns[0].name);

    try std.testing.expectError(error.TableNotFound, session.execute("SELECT *"));
}

test "SHOW CATALOGS and SHOW TABLES on files session" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var empty = try Session.openEmpty(gpa, io);
    defer empty.close();
    {
        var result = try empty.execute("SHOW CATALOGS");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("files", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("files", b.columns[1].strAt(0));
    }
    {
        var result = try empty.execute("SHOW TABLES");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 0), b.len);
    }

    try parquet.writeSalesFixture("/tmp/sales-show.parquet");
    var session = try Session.open(gpa, io, "/tmp/sales-show.parquet");
    defer session.close();
    {
        var result = try session.execute("SHOW TABLES FROM files");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("sales-show", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("files", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("", b.columns[2].strAt(0));
    }
    {
        var result = try session.execute("SHOW TABLES");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("sales-show", b.columns[0].strAt(0));
    }
    try std.testing.expectError(error.TableNotFound, session.execute("SHOW TABLES FROM missing"));
}

test "ATTACH parquet then SHOW and SELECT from named catalog" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/attached.parquet");
    var session = try Session.openEmpty(gpa, io);
    defer session.close();

    {
        var result = try session.execute("ATTACH '/tmp/attached.parquet' AS extra");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("extra", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("files", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SHOW CATALOGS");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqualStrings("files", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("extra", b.columns[0].strAt(1));
    }
    {
        var result = try session.execute("SHOW TABLES FROM extra");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("attached", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("extra", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM extra");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM extra.attached");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    try std.testing.expectError(error.InvalidSyntax, session.execute("ATTACH '/tmp/attached.parquet' AS extra"));
}

test "DESCRIBE USE DETACH named catalog" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_describe.parquet");
    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_describe.parquet' AS extra");
        defer result.deinit();
    }
    {
        var result = try session.execute("DESCRIBE extra");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("name", b.columns[0].name);
        try std.testing.expectEqualStrings("type", b.columns[1].name);
        try std.testing.expectEqualStrings("required", b.columns[2].name);
        try std.testing.expectEqualStrings("id", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("long", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("true", b.columns[2].strAt(0));
        try std.testing.expectEqualStrings("price", b.columns[0].strAt(1));
        try std.testing.expectEqualStrings("long", b.columns[1].strAt(1));
        try std.testing.expectEqualStrings("category", b.columns[0].strAt(2));
        try std.testing.expectEqualStrings("string", b.columns[1].strAt(2));
    }
    {
        var result = try session.execute("USE extra");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("extra", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute("SHOW TABLES");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("glacier_describe", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("extra", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("DETACH CATALOG extra");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("extra", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("files", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SHOW CATALOGS");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("files", b.columns[0].strAt(0));
    }
    try std.testing.expectError(error.TableNotFound, session.execute("SELECT COUNT(*) FROM extra"));
    try std.testing.expectError(error.TableNotFound, session.execute("DETACH extra"));
    try std.testing.expectError(error.TableNotFound, session.execute("USE missing"));
}

test "DESCRIBE Iceberg Hadoop table schema" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_describe_iceberg");
    var session = try Session.open(gpa, io, "/tmp/glacier_describe_iceberg");
    defer session.close();
    var result = try session.execute("DESCRIBE glacier_describe_iceberg");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqualStrings("id", b.columns[0].strAt(0));
    try std.testing.expectEqualStrings("long", b.columns[1].strAt(0));
    try std.testing.expectEqualStrings("true", b.columns[2].strAt(0));
    try std.testing.expectEqualStrings("price", b.columns[0].strAt(1));
    try std.testing.expectEqualStrings("category", b.columns[0].strAt(2));
    try std.testing.expectEqualStrings("string", b.columns[1].strAt(2));
}

test "relocated Iceberg location prefix remaps to opened directory" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writeRelocatedFixture(gpa, io, "/tmp/glacier_reloc_iceberg");
    var session = try Session.open(gpa, io, "/tmp/glacier_reloc_iceberg");
    defer session.close();
    var result = try session.execute("SELECT COUNT(*) FROM glacier_reloc_iceberg");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
}

test "Iceberg FOR SNAPSHOT and FOR TIMESTAMP AS OF" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writeTravelFixture(gpa, io, "/tmp/glacier_iceberg_travel");
    var session = try Session.open(gpa, io, "/tmp/glacier_iceberg_travel");
    defer session.close();
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel FOR SNAPSHOT 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel FOR SNAPSHOT AS OF 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel FOR TIMESTAMP AS OF 1700000000000");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel FOR TIMESTAMP AS OF 1700000000500");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel FOR TIMESTAMP AS OF 1700000001000");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    try std.testing.expectError(error.SnapshotNotFound, session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel FOR SNAPSHOT 9"));
    try std.testing.expectError(error.SnapshotNotFound, session.execute("SELECT COUNT(*) FROM glacier_iceberg_travel FOR TIMESTAMP AS OF 1"));
    try parquet.writeSalesFixture("/tmp/glacier_travel_sales.parquet");
    {
        var parquet_session = try Session.open(gpa, io, "/tmp/glacier_travel_sales.parquet");
        defer parquet_session.close();
        try std.testing.expectError(
            error.UnsupportedSql,
            parquet_session.execute("SELECT COUNT(*) FROM glacier_travel_sales FOR SNAPSHOT 1"),
        );
    }
    {
        var attached = try Session.openEmpty(gpa, io);
        defer attached.close();
        {
            var result = try attached.execute("ATTACH '/tmp/glacier_iceberg_travel' AS hist");
            defer result.deinit();
        }
        var result = try attached.execute("SELECT COUNT(*) FROM hist FOR SNAPSHOT 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
}

test "glacier catalogs tables snapshots and files" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    {
        var empty = try Session.openEmpty(gpa, io);
        defer empty.close();
        {
            var result = try empty.execute("SELECT * FROM glacier.catalogs");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 1), b.len);
            try std.testing.expectEqualStrings("files", b.columns[0].strAt(0));
            try std.testing.expectEqualStrings("files", b.columns[1].strAt(0));
        }
        {
            var result = try empty.execute("SELECT COUNT(*) FROM glacier.tables");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(i64, 0), b.columns[0].i64s[0]);
        }
        try std.testing.expectError(error.UnsupportedSql, empty.execute("SELECT * FROM glacier.snapshots"));
        try std.testing.expectError(error.TableNotFound, empty.execute("SELECT * FROM glacier.missing"));
        try std.testing.expectError(error.InvalidSyntax, empty.execute("ATTACH '/tmp/x.parquet' AS glacier"));
        {
            var result = try empty.execute("DESCRIBE glacier.catalogs");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 2), b.len);
            try std.testing.expectEqualStrings("name", b.columns[0].strAt(0));
            try std.testing.expectEqualStrings("type", b.columns[0].strAt(1));
        }
    }

    try parquet.writeSalesFixture("/tmp/glacier_sys_sales.parquet");
    {
        var parquet_session = try Session.open(gpa, io, "/tmp/glacier_sys_sales.parquet");
        defer parquet_session.close();
        {
            var result = try parquet_session.execute("SELECT name, catalog FROM glacier.tables");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 1), b.len);
            try std.testing.expectEqualStrings("glacier_sys_sales", b.columns[0].strAt(0));
            try std.testing.expectEqualStrings("files", b.columns[1].strAt(0));
        }
        try std.testing.expectError(
            error.UnsupportedSql,
            parquet_session.execute("SELECT * FROM glacier.snapshots"),
        );
        try std.testing.expectError(
            error.UnsupportedSql,
            parquet_session.execute("SELECT * FROM glacier.files"),
        );
    }

    try iceberg.writeTravelFixture(gpa, io, "/tmp/glacier_sys_travel");
    var session = try Session.open(gpa, io, "/tmp/glacier_sys_travel");
    defer session.close();
    {
        var result = try session.execute("SELECT snapshot_id FROM glacier.snapshots ORDER BY snapshot_id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[1]);
        try std.testing.expect(b.columns.len >= 1);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier.files");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT path, record_count FROM glacier.files FOR SNAPSHOT 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i64, 5), b.columns[1].i64s[0]);
        try std.testing.expect(std.mem.indexOf(u8, b.columns[0].strAt(0), "part-a.parquet") != null);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_sys_travel.snapshots");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("DESCRIBE glacier.files");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 4), b.len);
        try std.testing.expectEqualStrings("path", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("record_count", b.columns[0].strAt(2));
    }

    try iceberg.writeWarehouseFixture(gpa, io, "/tmp/glacier_sys_wh");
    {
        var lake = try Session.openEmpty(gpa, io);
        defer lake.close();
        {
            var result = try lake.execute("ATTACH '/tmp/glacier_sys_wh' AS lake");
            defer result.deinit();
        }
        {
            var result = try lake.execute("SELECT name, catalog, namespace FROM glacier.tables ORDER BY namespace, name");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 2), b.len);
            try std.testing.expectEqualStrings("prune", b.columns[0].strAt(0));
            try std.testing.expectEqualStrings("lake", b.columns[1].strAt(0));
            try std.testing.expectEqualStrings("extra", b.columns[2].strAt(0));
            try std.testing.expectEqualStrings("prune", b.columns[0].strAt(1));
            try std.testing.expectEqualStrings("sales", b.columns[2].strAt(1));
        }
        {
            var result = try lake.execute("SELECT COUNT(*) FROM lake.sales.prune.snapshots");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        }
        {
            var result = try lake.execute("SELECT COUNT(*) FROM lake.sales.prune.files");
            defer result.deinit();
            const b = result.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
        }
    }

    {
        var attached = try Session.openEmpty(gpa, io);
        defer attached.close();
        {
            var result = try attached.execute("ATTACH '/tmp/glacier_sys_travel' AS hist");
            defer result.deinit();
        }
        var result = try attached.execute("SELECT COUNT(*) FROM hist.snapshots");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
}

test "ATTACH Hadoop warehouse SHOW and SELECT ns.table" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writeWarehouseFixture(gpa, io, "/tmp/glacier_hadoop_wh");
    try parquet.writeSalesFixture("/tmp/glacier_hadoop_join.parquet");

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_hadoop_wh' AS lake");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("lake", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("iceberg_hadoop", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SHOW NAMESPACES FROM lake");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
    }
    {
        var result = try session.execute("SHOW TABLES FROM lake.sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("prune", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("lake", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("sales", b.columns[2].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("DESCRIBE lake.sales.prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("id", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("category", b.columns[0].strAt(2));
    }
    {
        var result = try session.execute("USE lake.sales");
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("ATTACH '/tmp/glacier_hadoop_join.parquet' AS extra");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM lake.extra.prune a JOIN extra b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
}

test "open Hadoop warehouse registers iceberg_hadoop catalog" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writeWarehouseFixture(gpa, io, "/tmp/glacier_hadoop_open");
    var session = try Session.open(gpa, io, "/tmp/glacier_hadoop_open");
    defer session.close();
    {
        var result = try session.execute("SHOW CATALOGS");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("hadoop", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("iceberg_hadoop", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM hadoop.sales.prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
}

test "JOIN across attached catalogs" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const left_ids = [_]i64{ 1, 2, 3 };
    const left_prices = [_]i64{ 10, 20, 30 };
    const left_cats = [_][*:0]const u8{ "fruit", "veg", "dairy" };
    try parquet.writeSalesRows("/tmp/glacier_cat_left.parquet", &left_ids, &left_prices, &left_cats);

    const right_ids = [_]i64{ 1, 1, 2, 9 };
    const right_prices = [_]i64{ 100, 101, 200, 900 };
    const right_cats = [_][*:0]const u8{ "x", "y", "z", "w" };
    try parquet.writeSalesRows("/tmp/glacier_cat_right.parquet", &right_ids, &right_prices, &right_cats);

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_cat_left.parquet' AS leftcat");
        defer result.deinit();
    }
    {
        var result = try session.execute("ATTACH '/tmp/glacier_cat_right.parquet' AS rightcat");
        defer result.deinit();
    }
    {
        var result = try session.execute("SHOW CATALOGS");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("leftcat", b.columns[0].strAt(1));
        try std.testing.expectEqualStrings("rightcat", b.columns[0].strAt(2));
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM leftcat a JOIN rightcat b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT a.id, b.price FROM leftcat.glacier_cat_left a JOIN rightcat.glacier_cat_right b ON a.id = b.id ORDER BY a.id, b.price",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualSlices(i64, &.{ 1, 1, 2 }, b.columns[0].i64s[0..3]);
        try std.testing.expectEqualSlices(i64, &.{ 100, 101, 200 }, b.columns[1].i64s[0..3]);
    }
}

test "SHOW NAMESPACES on files catalog is empty" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    var result = try session.execute("SHOW NAMESPACES");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(usize, 0), b.len);
    try std.testing.expectEqualStrings("name", b.columns[0].name);
}

test "openMemory parquet then SELECT" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/sales-mem.parquet");
    const FileSource = @import("vfs/source.zig").FileSource;
    var src = try FileSource.openPath(io, "/tmp/sales-mem.parquet");
    defer src.close();
    const bytes = try src.readAll(gpa);
    defer gpa.free(bytes);

    var session = try Session.openMemory(gpa, io, bytes, .parquet);
    defer session.close();
    var result = try session.execute("SELECT COUNT(*)");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
}

test "SELECT 1 FROM connected table repeats per row" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/sales-lit.parquet");
    var session = try Session.open(gpa, io, "/tmp/sales-lit.parquet");
    defer session.close();
    var result = try session.execute("SELECT 1 FROM '/tmp/sales-lit.parquet'");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(usize, 10), b.len);
    try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[9]);
}

pub const remote_test = if (@import("builtin").is_test) struct {
    const posix_env = struct {
        extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern "c" fn unsetenv(name: [*:0]const u8) c_int;
    };

    const EnvSlot = struct {
        key: [:0]const u8,
        old: ?[:0]u8,
    };

    fn captureEnv(a: std.mem.Allocator, key: [:0]const u8) !EnvSlot {
        if (std.c.getenv(key)) |p| {
            return .{ .key = key, .old = try a.dupeZ(u8, std.mem.span(p)) };
        }
        return .{ .key = key, .old = null };
    }

    fn restoreEnv(a: std.mem.Allocator, slot: EnvSlot) void {
        if (slot.old) |v| {
            _ = posix_env.setenv(slot.key, v, 1);
            a.free(v);
        } else {
            _ = posix_env.unsetenv(slot.key);
        }
    }

    fn spawnFileServer(io: std.Io, root: []const u8, prefix: []const u8, require_auth: bool) !struct { child: std.process.Child, port: u16 } {
        const py =
            \\from http.server import BaseHTTPRequestHandler, HTTPServer
            \\from pathlib import Path
            \\import sys
            \\ROOT = Path(sys.argv[1]).resolve()
            \\PREFIX = sys.argv[2].rstrip("/")
            \\AUTH = sys.argv[3] == "1"
            \\class H(BaseHTTPRequestHandler):
            \\    def log_message(self, *args):
            \\        pass
            \\    def _ok(self):
            \\        if not AUTH:
            \\            return True
            \\        auth = self.headers.get("Authorization") or ""
            \\        return auth.startswith("AWS4-HMAC-SHA256 ")
            \\    def _file(self):
            \\        p = self.path.split("?", 1)[0]
            \\        if PREFIX and not (p == PREFIX or p.startswith(PREFIX + "/")):
            \\            return None
            \\        rel = p[len(PREFIX):].lstrip("/") if PREFIX else p.lstrip("/")
            \\        if ROOT.is_file():
            \\            return ROOT if rel == "" else None
            \\        fp = (ROOT / rel).resolve()
            \\        if fp != ROOT and ROOT not in fp.parents:
            \\            return None
            \\        if not fp.is_file():
            \\            return None
            \\        return fp
            \\    def _dest(self):
            \\        p = self.path.split("?", 1)[0]
            \\        if PREFIX and not (p == PREFIX or p.startswith(PREFIX + "/")):
            \\            return None
            \\        rel = p[len(PREFIX):].lstrip("/") if PREFIX else p.lstrip("/")
            \\        if not rel or ROOT.is_file():
            \\            return None
            \\        fp = (ROOT / rel).resolve()
            \\        if ROOT not in fp.parents:
            \\            return None
            \\        return fp
            \\    def _send_file(self, fp, body):
            \\        data = fp.read_bytes()
            \\        rng = self.headers.get("Range")
            \\        if rng and rng.startswith("bytes="):
            \\            spec = rng[6:]
            \\            start_s, _, end_s = spec.partition("-")
            \\            start = int(start_s)
            \\            last = int(end_s) if end_s else len(data) - 1
            \\            chunk = data[start:last + 1]
            \\            self.send_response(206)
            \\            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, last, len(data)))
            \\            self.send_header("Content-Length", str(len(chunk)))
            \\            self.send_header("Accept-Ranges", "bytes")
            \\            self.end_headers()
            \\            if body:
            \\                self.wfile.write(chunk)
            \\            return
            \\        self.send_response(200)
            \\        self.send_header("Content-Length", str(len(data)))
            \\        self.send_header("Accept-Ranges", "bytes")
            \\        self.end_headers()
            \\        if body:
            \\            self.wfile.write(data)
            \\    def do_HEAD(self):
            \\        if not self._ok():
            \\            self.send_response(403); self.end_headers(); return
            \\        fp = self._file()
            \\        if fp is None:
            \\            self.send_response(404); self.end_headers(); return
            \\        self._send_file(fp, False)
            \\    def do_GET(self):
            \\        if not self._ok():
            \\            self.send_response(403); self.end_headers(); return
            \\        fp = self._file()
            \\        if fp is None:
            \\            self.send_response(404); self.end_headers(); return
            \\        self._send_file(fp, True)
            \\    def do_PUT(self):
            \\        if not self._ok():
            \\            self.send_response(403); self.end_headers(); return
            \\        fp = self._dest()
            \\        if fp is None:
            \\            self.send_response(404); self.end_headers(); return
            \\        n = int(self.headers.get("Content-Length") or 0)
            \\        fp.parent.mkdir(parents=True, exist_ok=True)
            \\        fp.write_bytes(self.rfile.read(n))
            \\        self.send_response(200)
            \\        self.send_header("Content-Length", "0")
            \\        self.end_headers()
            \\httpd = HTTPServer(("127.0.0.1", 0), H)
            \\print(httpd.server_address[1], flush=True)
            \\httpd.serve_forever()
        ;
        const auth = if (require_auth) "1" else "0";
        var child = std.process.spawn(io, .{
            .argv = &.{ "python3", "-c", py, root, prefix, auth },
            .stdout = .pipe,
            .stderr = .ignore,
            .stdin = .ignore,
        }) catch return error.SkipZigTest;
        errdefer child.kill(io);

        var line_buf: [64]u8 = undefined;
        var stdout_reader = child.stdout.?.readerStreaming(io, &line_buf);
        const line = stdout_reader.interface.takeDelimiterExclusive('\n') catch {
            child.kill(io);
            return error.SkipZigTest;
        };
        const port = std.fmt.parseInt(u16, std.mem.trim(u8, line, " \r\n"), 10) catch {
            child.kill(io);
            return error.SkipZigTest;
        };
        return .{ .child = child, .port = port };
    }

    pub fn spawnRestCatalog(io: std.Io, root: []const u8, token: []const u8, mode: []const u8, header: []const u8) !struct { child: std.process.Child, port: u16 } {
        const py =
            \\from http.server import BaseHTTPRequestHandler, HTTPServer
            \\from pathlib import Path
            \\import json, sys
            \\ROOT = Path(sys.argv[1]).resolve()
            \\TOKEN = sys.argv[2]
            \\MODE = sys.argv[3]
            \\HEADER = sys.argv[4]
            \\PREFIX = "cat"
            \\class H(BaseHTTPRequestHandler):
            \\    def log_message(self, *args):
            \\        pass
            \\    def _deny(self, code):
            \\        self.send_response(code)
            \\        self.end_headers()
            \\    def _check(self):
            \\        if HEADER:
            \\            name, _, val = HEADER.partition(":")
            \\            got = self.headers.get(name.strip()) or ""
            \\            if got != val.strip():
            \\                self._deny(403)
            \\                return False
            \\        if MODE == "none":
            \\            return True
            \\        auth = self.headers.get("Authorization") or ""
            \\        if MODE in ("bearer", "oauth"):
            \\            if auth != "Bearer " + TOKEN:
            \\                self._deny(401)
            \\                return False
            \\            return True
            \\        if MODE == "sigv4":
            \\            if not auth.startswith("AWS4-HMAC-SHA256 "):
            \\                self._deny(403)
            \\                return False
            \\            return True
            \\        return True
            \\    def _send(self, obj):
            \\        data = json.dumps(obj).encode()
            \\        self.send_response(200)
            \\        self.send_header("Content-Type", "application/json")
            \\        self.send_header("Content-Length", str(len(data)))
            \\        self.end_headers()
            \\        self.wfile.write(data)
            \\    def do_POST(self):
            \\        p = self.path.split("?", 1)[0].rstrip("/")
            \\        if p in ("/v1/oauth/tokens", "/oauth/tokens"):
            \\            n = int(self.headers.get("Content-Length") or 0)
            \\            body = self.rfile.read(n).decode()
            \\            if MODE == "oauth" and "s3cret" not in body:
            \\                self._deny(401)
            \\                return
            \\            self._send({"access_token": TOKEN or "test-token", "token_type": "bearer", "expires_in": 3600})
            \\            return
            \\        self._deny(404)
            \\    def do_GET(self):
            \\        if not self._check():
            \\            return
            \\        p = self.path.split("?", 1)[0]
            \\        if p == "/v1/config":
            \\            self._send({"defaults": {"prefix": PREFIX}, "overrides": {}})
            \\            return
            \\        want = "/v1/%s/namespaces/default/tables/prune" % PREFIX
            \\        if p == want:
            \\            meta = str(ROOT / "metadata" / "v1.metadata.json")
            \\            self._send({
            \\                "metadata-location": meta,
            \\                "config": {
            \\                    "s3.access-key-id": "AKIATEST",
            \\                    "s3.secret-access-key": "secret",
            \\                    "gcs.oauth2.token": "ya29.test",
            \\                    "gcs.project-id": "proj",
            \\                },
            \\            })
            \\            return
            \\        if p == "/v1/%s/namespaces/default/tables" % PREFIX:
            \\            self._send({"identifiers":[{"namespace":["default"],"name":"prune"}]})
            \\            return
            \\        if p == "/v1/%s/namespaces" % PREFIX:
            \\            self._send({"namespaces":[["default"]]})
            \\            return
            \\        self._deny(404)
            \\httpd = HTTPServer(("127.0.0.1", 0), H)
            \\print(httpd.server_address[1], flush=True)
            \\httpd.serve_forever()
        ;
        var child = std.process.spawn(io, .{
            .argv = &.{ "python3", "-c", py, root, token, mode, header },
            .stdout = .pipe,
            .stderr = .ignore,
            .stdin = .ignore,
        }) catch return error.SkipZigTest;
        errdefer child.kill(io);

        var line_buf: [64]u8 = undefined;
        var stdout_reader = child.stdout.?.readerStreaming(io, &line_buf);
        const line = stdout_reader.interface.takeDelimiterExclusive('\n') catch {
            child.kill(io);
            return error.SkipZigTest;
        };
        const port = std.fmt.parseInt(u16, std.mem.trim(u8, line, " \r\n"), 10) catch {
            child.kill(io);
            return error.SkipZigTest;
        };
        return .{ .child = child, .port = port };
    }

    pub fn spawnRestWriteCatalog(io: std.Io, root: []const u8, location: []const u8) !struct { child: std.process.Child, port: u16 } {
        const py =
            \\from http.server import BaseHTTPRequestHandler, HTTPServer
            \\from pathlib import Path
            \\from urllib.parse import unquote
            \\import json, sys, uuid, time
            \\ROOT = Path(sys.argv[1]).resolve()
            \\ROOT.mkdir(parents=True, exist_ok=True)
            \\LOC = sys.argv[2] if len(sys.argv) > 2 else ""
            \\namespaces = set()
            \\tables = {}
            \\def decode_ns(enc):
            \\    return unquote(enc).replace("\x1f", ".")
            \\def path_parts(p):
            \\    p = p.split("?", 1)[0].rstrip("/")
            \\    return p.split("/")
            \\def ns_array(ns):
            \\    return [x for x in ns.split(".") if x]
            \\def write_meta(loc, meta, ver):
            \\    if "://" in loc:
            \\        return
            \\    md = Path(loc) / "metadata"
            \\    md.mkdir(parents=True, exist_ok=True)
            \\    (Path(loc) / "data").mkdir(parents=True, exist_ok=True)
            \\    (md / ("v%d.metadata.json" % ver)).write_text(json.dumps(meta, indent=2))
            \\    (md / "version-hint.text").write_text("%d\n" % ver)
            \\class H(BaseHTTPRequestHandler):
            \\    def log_message(self, *a):
            \\        pass
            \\    def _deny(self, code, msg="error"):
            \\        data = json.dumps({"error": {"message": msg}}).encode()
            \\        self.send_response(code)
            \\        self.send_header("Content-Type", "application/json")
            \\        self.send_header("Content-Length", str(len(data)))
            \\        self.end_headers()
            \\        self.wfile.write(data)
            \\    def _send(self, obj, code=200):
            \\        data = json.dumps(obj).encode()
            \\        self.send_response(code)
            \\        self.send_header("Content-Type", "application/json")
            \\        self.send_header("Content-Length", str(len(data)))
            \\        self.end_headers()
            \\        self.wfile.write(data)
            \\    def _body(self):
            \\        n = int(self.headers.get("Content-Length") or 0)
            \\        raw = self.rfile.read(n).decode() if n else "{}"
            \\        return json.loads(raw or "{}")
            \\    def _load(self, ns, name):
            \\        rec = tables.get((ns, name))
            \\        if not rec:
            \\            self._deny(404, "no such table")
            \\            return None
            \\        loc = rec["meta"]["location"]
            \\        ver = rec["ver"]
            \\        if "://" in loc:
            \\            md = loc.rstrip("/") + "/metadata/v%d.metadata.json" % ver
            \\        else:
            \\            md = str(Path(loc) / "metadata" / ("v%d.metadata.json" % ver))
            \\        return {
            \\            "metadata-location": md,
            \\            "metadata": rec["meta"],
            \\            "config": {},
            \\        }
            \\    def do_GET(self):
            \\        parts = path_parts(self.path)
            \\        if parts == ["", "v1", "config"]:
            \\            self._send({"defaults": {}, "overrides": {}})
            \\            return
            \\        if parts == ["", "v1", "namespaces"]:
            \\            self._send({"namespaces": [ns_array(n) for n in sorted(namespaces)]})
            \\            return
            \\        if len(parts) == 5 and parts[1] == "v1" and parts[2] == "namespaces" and parts[4] == "tables":
            \\            ns = decode_ns(parts[3])
            \\            ids = [{"namespace": ns_array(n), "name": t} for (n, t) in tables if n == ns]
            \\            self._send({"identifiers": ids})
            \\            return
            \\        if len(parts) == 6 and parts[1] == "v1" and parts[2] == "namespaces" and parts[4] == "tables":
            \\            ns = decode_ns(parts[3])
            \\            name = unquote(parts[5])
            \\            obj = self._load(ns, name)
            \\            if obj:
            \\                self._send(obj)
            \\            return
            \\        self._deny(404)
            \\    def do_POST(self):
            \\        try:
            \\            self._post()
            \\        except Exception as e:
            \\            try:
            \\                (ROOT / "handler.err").write_text(repr(e))
            \\            except Exception:
            \\                pass
            \\            self._deny(500, repr(e))
            \\    def _post(self):
            \\        parts = path_parts(self.path)
            \\        body = self._body()
            \\        if parts == ["", "v1", "namespaces"]:
            \\            ns = ".".join(body.get("namespace") or [])
            \\            if not ns:
            \\                self._deny(400)
            \\                return
            \\            namespaces.add(ns)
            \\            self._send({"namespace": ns_array(ns)})
            \\            return
            \\        if len(parts) == 5 and parts[1] == "v1" and parts[2] == "namespaces" and parts[4] == "tables":
            \\            ns = decode_ns(parts[3])
            \\            name = body.get("name") or ""
            \\            if not name:
            \\                self._deny(400)
            \\                return
            \\            if (ns, name) in tables:
            \\                self._deny(409, "already exists")
            \\                return
            \\            namespaces.add(ns)
            \\            schema = body.get("schema") or {"type": "struct", "schema-id": 0, "fields": []}
            \\            if "type" not in schema:
            \\                schema = {"type": "struct", "schema-id": schema.get("schema-id", 0), "fields": schema.get("fields", [])}
            \\            loc = (LOC.rstrip("/") + "/" + ns + "/" + name) if LOC else str(ROOT / ns / name)
            \\            fields = schema.get("fields") or []
            \\            last_id = max([f.get("id", 0) for f in fields] or [0])
            \\            ps = body.get("partition-spec") or {"spec-id": 0, "fields": []}
            \\            if isinstance(ps, list):
            \\                ps = {"spec-id": 0, "fields": ps}
            \\            if "fields" not in ps:
            \\                ps = {"spec-id": 0, "fields": []}
            \\            meta = {
            \\                "format-version": 2,
            \\                "table-uuid": str(uuid.uuid4()),
            \\                "location": loc,
            \\                "last-updated-ms": int(time.time() * 1000),
            \\                "last-column-id": last_id,
            \\                "last-sequence-number": 0,
            \\                "current-schema-id": schema.get("schema-id", 0),
            \\                "default-spec-id": ps.get("spec-id", 0),
            \\                "partition-specs": [ps],
            \\                "schemas": [schema],
            \\                "snapshots": [],
            \\            }
            \\            write_meta(loc, meta, 1)
            \\            tables[(ns, name)] = {"meta": meta, "ver": 1}
            \\            self._send(self._load(ns, name))
            \\            return
            \\        if len(parts) == 6 and parts[1] == "v1" and parts[2] == "namespaces" and parts[4] == "tables":
            \\            ns = decode_ns(parts[3])
            \\            name = unquote(parts[5])
            \\            rec = tables.get((ns, name))
            \\            if not rec:
            \\                self._deny(404)
            \\                return
            \\            meta = rec["meta"]
            \\            for req in body.get("requirements") or []:
            \\                typ = req.get("type")
            \\                if typ == "assert-table-uuid":
            \\                    if req.get("uuid") != meta.get("table-uuid"):
            \\                        self._deny(409, "uuid")
            \\                        return
            \\                elif typ == "assert-ref-snapshot-id":
            \\                    expected = req.get("snapshot-id")
            \\                    current = meta.get("current-snapshot-id")
            \\                    if expected is None:
            \\                        if current not in (None, -1):
            \\                            self._deny(409, "snapshot")
            \\                            return
            \\                    elif current != expected:
            \\                        self._deny(409, "snapshot")
            \\                        return
            \\            for upd in body.get("updates") or []:
            \\                act = upd.get("action")
            \\                if act == "add-snapshot":
            \\                    snap = upd.get("snapshot") or {}
            \\                    snaps = list(meta.get("snapshots") or [])
            \\                    snaps.append(snap)
            \\                    meta["snapshots"] = snaps
            \\                    if "sequence-number" in snap:
            \\                        meta["last-sequence-number"] = snap["sequence-number"]
            \\                    meta["last-updated-ms"] = snap.get("timestamp-ms", int(time.time() * 1000))
            \\                elif act == "set-snapshot-ref":
            \\                    meta["current-snapshot-id"] = upd.get("snapshot-id")
            \\                elif act == "add-schema":
            \\                    sch = upd.get("schema") or {}
            \\                    schemas = list(meta.get("schemas") or [])
            \\                    schemas.append(sch)
            \\                    meta["schemas"] = schemas
            \\                    fields = sch.get("fields") or []
            \\                    last_id = max([f.get("id", 0) for f in fields] or [0])
            \\                    if last_id > int(meta.get("last-column-id") or 0):
            \\                        meta["last-column-id"] = last_id
            \\                elif act == "set-current-schema":
            \\                    meta["current-schema-id"] = upd.get("schema-id")
            \\            rec["ver"] += 1
            \\            write_meta(meta["location"], meta, rec["ver"])
            \\            self._send(self._load(ns, name))
            \\            return
            \\        self._deny(404)
            \\    def do_DELETE(self):
            \\        parts = path_parts(self.path)
            \\        if len(parts) == 6 and parts[1] == "v1" and parts[2] == "namespaces" and parts[4] == "tables":
            \\            ns = decode_ns(parts[3])
            \\            name = unquote(parts[5])
            \\            if (ns, name) not in tables:
            \\                self._deny(404)
            \\                return
            \\            del tables[(ns, name)]
            \\            self.send_response(204)
            \\            self.end_headers()
            \\            return
            \\        self._deny(404)
            \\httpd = HTTPServer(("127.0.0.1", 0), H)
            \\print(httpd.server_address[1], flush=True)
            \\httpd.serve_forever()
        ;
        var child = std.process.spawn(io, .{
            .argv = &.{ "python3", "-c", py, root, location },
            .stdout = .pipe,
            .stderr = .ignore,
            .stdin = .ignore,
        }) catch return error.SkipZigTest;
        errdefer child.kill(io);

        var line_buf: [64]u8 = undefined;
        var stdout_reader = child.stdout.?.readerStreaming(io, &line_buf);
        const line = stdout_reader.interface.takeDelimiterExclusive('\n') catch {
            child.kill(io);
            return error.SkipZigTest;
        };
        const port = std.fmt.parseInt(u16, std.mem.trim(u8, line, " \r\n"), 10) catch {
            child.kill(io);
            return error.SkipZigTest;
        };
        return .{ .child = child, .port = port };
    }
} else struct {};

test "Session opens http parquet" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_http_sales.parquet");
    var srv = try remote_test.spawnFileServer(io, "/tmp/glacier_http_sales.parquet", "/glacier_http_sales.parquet", false);
    defer srv.child.kill(io);

    const url = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}/glacier_http_sales.parquet", .{srv.port});
    defer gpa.free(url);
    var session = try Session.open(gpa, io, url);
    defer session.close();
    var result = try session.execute("SELECT COUNT(*)");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);

    {
        var limited = try session.execute("SELECT * LIMIT 2");
        defer limited.deinit();
        const rows = limited.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), rows.len);
        try std.testing.expectEqual(@as(i64, 1), rows.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 2), rows.columns[0].i64s[1]);
    }

    const cached = try cache.filePath(gpa, url);
    defer gpa.free(cached);
    const st = try std.Io.Dir.cwd().statFile(io, cached, .{});
    try std.testing.expect(st.size > 0);
}

test "Session opens s3 Iceberg via path-style mock" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_s3_iceberg");
    var srv = try remote_test.spawnFileServer(io, "/tmp/glacier_s3_iceberg", "/glacier-test/iceberg_prune", true);
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrintSentinel(gpa, "http://127.0.0.1:{d}", .{srv.port}, 0);
    defer gpa.free(endpoint);

    const ak = try remote_test.captureEnv(gpa, "AWS_ACCESS_KEY_ID");
    const sk = try remote_test.captureEnv(gpa, "AWS_SECRET_ACCESS_KEY");
    const region = try remote_test.captureEnv(gpa, "AWS_REGION");
    const ep = try remote_test.captureEnv(gpa, "AWS_ENDPOINT_URL");
    const tok = try remote_test.captureEnv(gpa, "AWS_SESSION_TOKEN");
    defer {
        remote_test.restoreEnv(gpa, tok);
        remote_test.restoreEnv(gpa, ep);
        remote_test.restoreEnv(gpa, region);
        remote_test.restoreEnv(gpa, sk);
        remote_test.restoreEnv(gpa, ak);
    }
    _ = remote_test.posix_env.setenv("AWS_ACCESS_KEY_ID", "AKIATEST", 1);
    _ = remote_test.posix_env.setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", 1);
    _ = remote_test.posix_env.setenv("AWS_REGION", "us-east-1", 1);
    _ = remote_test.posix_env.setenv("AWS_ENDPOINT_URL", endpoint, 1);
    _ = remote_test.posix_env.unsetenv("AWS_SESSION_TOKEN");

    var session = try Session.open(gpa, io, "s3://glacier-test/iceberg_prune");
    defer session.close();
    try std.testing.expectEqual(@as(usize, 2), session.files.len);

    {
        var result = try session.execute("SELECT COUNT(*)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(usize, 2), result.scan_stats.files_opened);
    }

    {
        var result = try session.execute("SELECT * WHERE price > 180");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_pruned);
    }
}

test "parquet decimal uuid timestamp sql" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeLogicalFixture("/tmp/glacier-logical.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier-logical.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT *");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqual(@as(usize, 4), b.columns.len);
        try std.testing.expectEqual(DataType.decimal128, b.columns[0].data_type);
        try std.testing.expectEqual(DataType.uuid, b.columns[1].data_type);
        try std.testing.expectEqual(DataType.timestamp, b.columns[2].data_type);
        try std.testing.expectEqual(DataType.timestamptz, b.columns[3].data_type);
        try std.testing.expectEqual(@as(i32, 10), b.columns[0].decimal_precision);
        try std.testing.expectEqual(@as(i32, 2), b.columns[0].decimal_scale);
        try std.testing.expectEqual(@as(i128, 1050), b.columns[0].i128s[0]);
        try std.testing.expectEqual(@as(i128, 2000), b.columns[0].i128s[1]);
        try std.testing.expectEqual(@as(i128, 50), b.columns[0].i128s[2]);
        try std.testing.expectEqualSlices(u8, &.{ 0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00 }, &b.columns[1].uuids[0]);
        try std.testing.expectEqual(@as(i64, 1700000000000000), b.columns[2].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1700000000000000), b.columns[3].i64s[0]);
    }

    {
        var result = try session.execute("SELECT * WHERE ts > 1700000000000000");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 1700000001000000), b.columns[2].i64s[0]);
    }

    {
        var result = try session.execute("SELECT * WHERE id = '550e8400-e29b-41d4-a716-446655440000'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i128, 1050), b.columns[0].i128s[0]);
    }

    {
        var result = try session.execute("SELECT * WHERE amount > 10");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
    }

    {
        var result = try session.execute("SELECT * ORDER BY amount");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i128, 50), b.columns[0].i128s[0]);
        try std.testing.expectEqual(@as(i128, 1050), b.columns[0].i128s[1]);
        try std.testing.expectEqual(@as(i128, 2000), b.columns[0].i128s[2]);
    }

    {
        var result = try session.execute("SELECT SUM(amount), MIN(amount), MAX(amount), COUNT(*)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(DataType.decimal128, b.columns[0].data_type);
        try std.testing.expectEqual(@as(i128, 3100), b.columns[0].i128s[0]);
        try std.testing.expectEqual(@as(i128, 50), b.columns[1].i128s[0]);
        try std.testing.expectEqual(@as(i128, 2000), b.columns[2].i128s[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[3].i64s[0]);
    }
}

test "ORDER BY GROUP BY DISTINCT spill under GLACIER_MEM" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const n: usize = 400;
    const ids = try gpa.alloc(i64, n);
    defer gpa.free(ids);
    const prices = try gpa.alloc(i64, n);
    defer gpa.free(prices);
    const cats = try gpa.alloc([*:0]const u8, n);
    defer gpa.free(cats);
    const pool = [_][:0]const u8{ "fruit", "veg", "dairy" };
    for (0..n) |i| {
        ids[i] = @intCast(i + 1);
        prices[i] = @intCast((i * 7) % 300);
        cats[i] = pool[i % 3].ptr;
    }
    try parquet.writeSalesRows("/tmp/glacier_spill_sales.parquet", ids, prices, cats);

    const mem = try remote_test.captureEnv(gpa, "GLACIER_MEM");
    const tmp = try remote_test.captureEnv(gpa, "GLACIER_TEMP");
    defer {
        remote_test.restoreEnv(gpa, tmp);
        remote_test.restoreEnv(gpa, mem);
    }
    _ = remote_test.posix_env.setenv("GLACIER_MEM", "1024", 1);
    _ = remote_test.posix_env.setenv("GLACIER_TEMP", "/tmp/glacier_spill_tmp", 1);

    var session = try Session.open(gpa, io, "/tmp/glacier_spill_sales.parquet");
    defer session.close();

    spill.debug_files_written = 0;
    {
        var result = try session.execute("SELECT * FROM glacier_spill_sales ORDER BY price DESC");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 400), b.len);
        try std.testing.expectEqual(@as(i64, 299), b.columns[1].i64s[0]);
        try std.testing.expect(spill.debug_files_written > 0);
    }

    {
        var result = try session.execute("SELECT category, COUNT(*) AS n FROM glacier_spill_sales GROUP BY category");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        var total: i64 = 0;
        for (0..b.len) |i| total += b.columns[1].i64s[i];
        try std.testing.expectEqual(@as(i64, 400), total);
    }

    {
        var result = try session.execute("SELECT DISTINCT category FROM glacier_spill_sales ORDER BY category");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(2));
    }

    {
        var result = try session.execute("SELECT * FROM glacier_spill_sales ORDER BY id LIMIT 5 OFFSET 10");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
        try std.testing.expectEqual(@as(i64, 11), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 15), b.columns[0].i64s[4]);
    }

    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_spill_sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 400), b.columns[0].i64s[0]);
    }
}

test "COPY TO native glacier and query pages" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_copy_src.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier_copy_src.parquet");
    defer session.close();

    {
        var result = try session.execute("COPY TO '/tmp/glacier_copy_sales.glacier'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
        try std.testing.expectEqualStrings("/tmp/glacier_copy_sales.glacier", b.columns[1].strAt(0));
    }

    {
        var result = try session.execute("COPY (SELECT * FROM glacier_copy_src WHERE price > 100) TO '/tmp/glacier_copy_filtered.glacier'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }

    var native_session = try Session.open(gpa, io, "/tmp/glacier_copy_sales.glacier");
    defer native_session.close();
    {
        var result = try native_session.execute("SELECT COUNT(*)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try native_session.execute("SELECT * WHERE price > 100");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
    }
    {
        var result = try native_session.execute("SELECT * ORDER BY price DESC LIMIT 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 300), b.columns[1].i64s[0]);
    }
    {
        var result = try native_session.execute("COPY (SELECT * WHERE price > 100) TO '/tmp/glacier_copy_from_native.glacier'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }

    const tmp = try remote_test.captureEnv(gpa, "GLACIER_TEMP");
    defer remote_test.restoreEnv(gpa, tmp);
    _ = remote_test.posix_env.setenv("GLACIER_TEMP", "/tmp/glacier_copy_tmp", 1);
    {
        var result = try session.execute("COPY TO 'relative_sales'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
        try std.testing.expect(std.mem.endsWith(u8, b.columns[1].strAt(0), "/copy/relative_sales.glacier"));
    }
}

test "COPY FROM parquet into Iceberg catalog" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_copy_from_src.parquet");
    const root = "/tmp/glacier_copy_from_wh";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_copy_from_wh' AS lake");
        defer result.deinit();
    }
    {
        var result = try session.execute("CREATE NAMESPACE lake.sales");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.orders (id BIGINT, price BIGINT, category STRING) PARTITIONED BY (category)",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("COPY lake.sales.orders FROM '/tmp/glacier_copy_from_src.parquet'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
        try std.testing.expectEqualStrings("copied", b.columns[0].name);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders WHERE category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 2), result.scan_stats.files_pruned);
    }
    {
        var result = try session.execute("SELECT path FROM lake.sales.orders.files");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        var saw_fruit = false;
        var saw_veg = false;
        var saw_dairy = false;
        var i: usize = 0;
        while (i < b.len) : (i += 1) {
            const p = b.columns[0].strAt(i);
            if (std.mem.indexOf(u8, p, "category=fruit") != null) saw_fruit = true;
            if (std.mem.indexOf(u8, p, "category=veg") != null) saw_veg = true;
            if (std.mem.indexOf(u8, p, "category=dairy") != null) saw_dairy = true;
        }
        try std.testing.expect(saw_fruit);
        try std.testing.expect(saw_veg);
        try std.testing.expect(saw_dairy);
    }
}

test "DELETE FROM Iceberg equality deletes" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_delete_wh";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_delete_wh' AS lake");
        defer result.deinit();
    }
    {
        var result = try session.execute("CREATE NAMESPACE lake.sales");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.orders (id BIGINT, category STRING)",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "INSERT INTO lake.sales.orders VALUES (1, 'fruit'), (2, 'veg'), (3, 'fruit')",
        );
        defer result.deinit();
    }
    var snap_before: i64 = 0;
    {
        var result = try session.execute("SELECT snapshot_id FROM lake.sales.orders.snapshots ORDER BY snapshot_id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expect(b.len >= 1);
        snap_before = b.columns[0].i64s[b.len - 1];
    }
    {
        var result = try session.execute("DELETE FROM lake.sales.orders WHERE category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
        try std.testing.expectEqualStrings("deleted", b.columns[0].name);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT category FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(0));
    }
    {
        var snap_sql_buf: [128]u8 = undefined;
        const snap_sql = try std.fmt.bufPrint(
            &snap_sql_buf,
            "SELECT COUNT(*) FROM lake.sales.orders FOR SNAPSHOT {d}",
            .{snap_before},
        );
        var result = try session.execute(snap_sql);
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("INSERT INTO lake.sales.orders VALUES (4, 'dairy')");
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("DELETE FROM lake.sales.orders WHERE category = 'missing'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 0), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT content FROM lake.sales.orders.files");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        var saw_eq = false;
        var i: usize = 0;
        while (i < b.len) : (i += 1) {
            if (b.columns[0].i64s[i] == 2) saw_eq = true;
        }
        try std.testing.expect(saw_eq);
    }
}

test "UPDATE MERGE ALTER Iceberg native" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_update_wh";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_update_wh' AS lake");
        defer result.deinit();
    }
    {
        var result = try session.execute("CREATE NAMESPACE lake.sales");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.orders (id BIGINT, category STRING)",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "INSERT INTO lake.sales.orders VALUES (1, 'fruit'), (2, 'veg'), (3, 'fruit')",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("UPDATE lake.sales.orders SET category = 'x' WHERE id = 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqualStrings("updated", b.columns[0].name);
    }
    {
        var result = try session.execute("SELECT category FROM lake.sales.orders WHERE id = 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("x", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.delta (id BIGINT, category STRING)",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "INSERT INTO lake.sales.delta VALUES (2, 'dairy'), (4, 'veg')",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "MERGE INTO lake.sales.orders d USING lake.sales.delta s ON d.id = s.id WHEN MATCHED THEN UPDATE SET category = s.category WHEN NOT MATCHED THEN INSERT",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 0), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[2].i64s[0]);
        try std.testing.expectEqualStrings("deleted", b.columns[0].name);
        try std.testing.expectEqualStrings("updated", b.columns[1].name);
        try std.testing.expectEqualStrings("inserted", b.columns[2].name);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT category FROM lake.sales.orders WHERE id = 2");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.dropper (id BIGINT, category STRING)",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("INSERT INTO lake.sales.dropper VALUES (3, 'gone')");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "MERGE INTO lake.sales.orders d USING lake.sales.dropper s ON d.id = s.id WHEN MATCHED THEN DELETE",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 0), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 0), b.columns[2].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("ALTER TABLE lake.sales.orders ADD COLUMN note STRING");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("orders", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("note", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("string", b.columns[2].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders WHERE note IS NULL");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("DESCRIBE lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        var saw_note = false;
        var i: usize = 0;
        while (i < b.len) : (i += 1) {
            if (std.ascii.eqlIgnoreCase(b.columns[0].strAt(i), "note")) saw_note = true;
        }
        try std.testing.expect(saw_note);
    }
    {
        var result = try session.execute(
            "INSERT INTO lake.sales.orders SELECT id + 100, category FROM lake.sales.orders",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 6), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders WHERE id > 50");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
}

test "INNER JOIN hash 1:1 1:N utf8 i32 self-join" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const left_ids = [_]i64{ 1, 2, 3 };
    const left_prices = [_]i64{ 10, 20, 30 };
    const left_cats = [_][*:0]const u8{ "fruit", "veg", "dairy" };
    try parquet.writeSalesRows("/tmp/glacier_join_left.parquet", &left_ids, &left_prices, &left_cats);

    const right_ids = [_]i64{ 1, 1, 2, 9 };
    const right_prices = [_]i64{ 100, 101, 200, 900 };
    const right_cats = [_][*:0]const u8{ "x", "y", "z", "w" };
    try parquet.writeSalesRows("/tmp/glacier_join_right.parquet", &right_ids, &right_prices, &right_cats);

    var session = try Session.open(gpa, io, "/tmp/glacier_join_left.parquet");
    defer session.close();

    {
        var result = try session.execute(
            "SELECT a.id, b.price FROM glacier_join_left a JOIN '/tmp/glacier_join_right.parquet' b ON a.id = b.id ORDER BY a.id, b.price",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualSlices(i64, &.{ 1, 1, 2 }, b.columns[0].i64s[0..3]);
        try std.testing.expectEqualSlices(i64, &.{ 100, 101, 200 }, b.columns[1].i64s[0..3]);
    }

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_join_left a JOIN '/tmp/glacier_join_right.parquet' b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }

    const dim_ids = [_]i64{ 10, 11 };
    const dim_prices = [_]i64{ 1, 2 };
    const dim_cats = [_][*:0]const u8{ "fruit", "fruit" };
    try parquet.writeSalesRows("/tmp/glacier_join_dim.parquet", &dim_ids, &dim_prices, &dim_cats);
    {
        var result = try session.execute(
            "SELECT a.category, b.id FROM glacier_join_left a JOIN '/tmp/glacier_join_dim.parquet' b ON a.category = b.category ORDER BY b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqual(@as(i64, 10), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 11), b.columns[1].i64s[1]);
    }

    try parquet.writeSalesFixture("/tmp/glacier_join_sales.parquet");
    var sales = try Session.open(gpa, io, "/tmp/glacier_join_sales.parquet");
    defer sales.close();
    {
        var result = try sales.execute("SELECT COUNT(*) FROM glacier_join_sales a JOIN glacier_join_sales b ON a.id = b.id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try sales.execute("SELECT COUNT(*) AS n FROM glacier_join_sales a JOIN glacier_join_sales b ON a.category = b.category");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("n", b.columns[0].name);
        try std.testing.expectEqual(@as(i64, 38), b.columns[0].i64s[0]);
    }
    {
        var result = try sales.execute("SELECT * FROM glacier_join_sales a JOIN glacier_join_sales b ON a.id = b.id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 10), b.len);
        try std.testing.expect(b.columnIndex("a.id") != null);
        try std.testing.expect(b.columnIndex("b.id") != null);
        try std.testing.expectError(error.AmbiguousColumn, b.lookup("id"));
    }

    try parquet.writeTypesFixture("/tmp/glacier_join_types.parquet");
    var types = try Session.open(gpa, io, "/tmp/glacier_join_types.parquet");
    defer types.close();
    {
        var result = try types.execute(
            "SELECT a.i32, b.name FROM glacier_join_types a JOIN '/tmp/glacier_join_types.parquet' b ON a.i32 = b.i32 ORDER BY a.i32",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualSlices(i32, &.{ 1, 2, 3 }, b.columns[0].i32s[0..3]);
        try std.testing.expectEqualStrings("a", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("c", b.columns[1].strAt(2));
    }

    try std.testing.expectError(error.UnsupportedJoin, session.execute("SELECT * FROM a JOIN b"));
}

test "JOIN COUNT star many-to-many uses key frequencies" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const n: usize = 4000;
    const ids = try gpa.alloc(i64, n);
    defer gpa.free(ids);
    const prices = try gpa.alloc(i64, n);
    defer gpa.free(prices);
    const cats = try gpa.alloc([*:0]const u8, n);
    defer gpa.free(cats);
    for (0..n) |i| {
        ids[i] = @intCast(i + 1);
        prices[i] = 1;
        cats[i] = if (i % 2 == 0) "fruit" else "veg";
    }
    try parquet.writeSalesRows("/tmp/glacier_join_count_mm.parquet", ids, prices, cats);
    var session = try Session.open(gpa, io, "/tmp/glacier_join_count_mm.parquet");
    defer session.close();
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_join_count_mm a JOIN glacier_join_count_mm b ON a.category = b.category",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 8_000_000), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_join_count_mm a LEFT JOIN glacier_join_count_mm b ON a.category = b.category",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 8_000_000), b.columns[0].i64s[0]);
    }
}

test "LEFT JOIN unmatched null key 1:N" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const left_ids = [_]i64{ 1, 2, 3 };
    const left_prices = [_]i64{ 10, 20, 30 };
    const left_cats = [_][*:0]const u8{ "fruit", "veg", "dairy" };
    try parquet.writeSalesRows("/tmp/glacier_ljoin_left.parquet", &left_ids, &left_prices, &left_cats);

    const right_ids = [_]i64{ 1, 1, 2, 9 };
    const right_prices = [_]i64{ 100, 101, 200, 900 };
    const right_cats = [_][*:0]const u8{ "x", "y", "z", "w" };
    try parquet.writeSalesRows("/tmp/glacier_ljoin_right.parquet", &right_ids, &right_prices, &right_cats);

    var session = try Session.open(gpa, io, "/tmp/glacier_ljoin_left.parquet");
    defer session.close();

    {
        var result = try session.execute(
            "SELECT a.id, b.price FROM glacier_ljoin_left a LEFT JOIN '/tmp/glacier_ljoin_right.parquet' b ON a.id = b.id ORDER BY a.id, b.price",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 4), b.len);
        try std.testing.expectEqualSlices(i64, &.{ 1, 1, 2, 3 }, b.columns[0].i64s[0..4]);
        try std.testing.expectEqual(@as(i64, 100), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 101), b.columns[1].i64s[1]);
        try std.testing.expectEqual(@as(i64, 200), b.columns[1].i64s[2]);
        try std.testing.expect(b.columns[1].isNull(3));
    }

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_ljoin_left a LEFT JOIN '/tmp/glacier_ljoin_right.parquet' b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_ljoin_left a LEFT JOIN '/tmp/glacier_ljoin_right.parquet' b ON a.id = b.id WHERE b.id IS NULL",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_ljoin_left a LEFT OUTER JOIN '/tmp/glacier_ljoin_right.parquet' b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_ljoin_left a RIGHT JOIN '/tmp/glacier_ljoin_right.parquet' b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_ljoin_left a FULL JOIN '/tmp/glacier_ljoin_right.parquet' b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }

    try parquet.writeNullsFixture("/tmp/glacier_ljoin_nulls.parquet");
    var nulls = try Session.open(gpa, io, "/tmp/glacier_ljoin_nulls.parquet");
    defer nulls.close();
    {
        var result = try nulls.execute(
            "SELECT COUNT(*) FROM glacier_ljoin_nulls a LEFT JOIN glacier_ljoin_nulls b ON a.qty = b.qty",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
    }
}

test "window COUNT SUM ROW_NUMBER PARTITION BY" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_window_sales.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier_window_sales.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT COUNT(*) OVER () FROM glacier_window_sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 10), b.len);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[9]);
    }

    {
        var result = try session.execute(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM glacier_window_sales ORDER BY id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 10), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[9]);
        try std.testing.expectEqual(@as(i64, 10), b.columns[1].i64s[9]);
    }

    {
        var result = try session.execute(
            "SELECT category, COUNT(*) OVER (PARTITION BY category) AS n FROM glacier_window_sales ORDER BY id LIMIT 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(0));
        try std.testing.expectEqual(@as(i64, 5), b.columns[1].i64s[0]);
    }

    {
        var result = try session.execute(
            "SELECT SUM(price) OVER () AS s FROM glacier_window_sales LIMIT 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1275), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute(
            "SELECT RANK() OVER (ORDER BY category) AS r, DENSE_RANK() OVER (ORDER BY category) AS d FROM glacier_window_sales ORDER BY id LIMIT 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 2), b.columns[1].i64s[0]);
    }

    {
        var result = try session.execute(
            \\SELECT id, SUM(price) OVER (
            \\  PARTITION BY category ORDER BY id
            \\  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            \\) AS s
            \\FROM glacier_window_sales
            \\WHERE category = 'fruit'
            \\ORDER BY id
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
        try std.testing.expectEqual(@as(i64, 50), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 130), b.columns[1].i64s[1]);
        try std.testing.expectEqual(@as(i64, 230), b.columns[1].i64s[2]);
        try std.testing.expectEqual(@as(i64, 240), b.columns[1].i64s[3]);
        try std.testing.expectEqual(@as(i64, 165), b.columns[1].i64s[4]);
    }

    {
        var result = try session.execute(
            "SELECT SUM(price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS s FROM glacier_window_sales ORDER BY id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 50), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1275), b.columns[0].i64s[9]);
    }

    {
        var result = try session.execute(
            "SELECT LAG(price) OVER (ORDER BY id) AS p FROM glacier_window_sales ORDER BY id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expect(b.columns[0].isNull(0));
        try std.testing.expectEqual(@as(i64, 50), b.columns[0].i64s[1]);
        try std.testing.expectEqual(@as(i64, 300), b.columns[0].i64s[9]);
    }

    {
        var result = try session.execute(
            "SELECT LEAD(price, 2) OVER (ORDER BY id) AS p FROM glacier_window_sales ORDER BY id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 100), b.columns[0].i64s[0]);
        try std.testing.expect(b.columns[0].isNull(8));
        try std.testing.expect(b.columns[0].isNull(9));
    }

    {
        var result = try session.execute(
            \\SELECT category, COUNT(*) AS n, ROW_NUMBER() OVER (ORDER BY category) AS rn
            \\FROM glacier_window_sales
            \\GROUP BY category
            \\ORDER BY category
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
        try std.testing.expectEqual(@as(i64, 2), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[2].i64s[0]);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqual(@as(i64, 5), b.columns[1].i64s[1]);
        try std.testing.expectEqual(@as(i64, 2), b.columns[2].i64s[1]);
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(2));
        try std.testing.expectEqual(@as(i64, 3), b.columns[1].i64s[2]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[2].i64s[2]);
    }

    {
        var full = try session.execute(
            "SELECT SUM(price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS s FROM glacier_window_sales ORDER BY id",
        );
        defer full.deinit();
        const all = full.nextBatch() orelse return error.EmptyResult;
        var limited = try session.execute(
            "SELECT SUM(price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS s FROM glacier_window_sales ORDER BY id LIMIT 3",
        );
        defer limited.deinit();
        const lim = limited.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), lim.len);
        try std.testing.expectEqual(all.columns[0].i64s[0], lim.columns[0].i64s[0]);
        try std.testing.expectEqual(all.columns[0].i64s[1], lim.columns[0].i64s[1]);
        try std.testing.expectEqual(all.columns[0].i64s[2], lim.columns[0].i64s[2]);
        var skipped = try session.execute(
            "SELECT SUM(price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS s FROM glacier_window_sales ORDER BY id OFFSET 2 LIMIT 2",
        );
        defer skipped.deinit();
        const off = skipped.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), off.len);
        try std.testing.expectEqual(all.columns[0].i64s[2], off.columns[0].i64s[0]);
        try std.testing.expectEqual(all.columns[0].i64s[3], off.columns[0].i64s[1]);
    }
}

test "window running LIMIT does not scan the whole frame" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const n: usize = 8000;
    const ids = try gpa.alloc(i64, n);
    defer gpa.free(ids);
    const prices = try gpa.alloc(i64, n);
    defer gpa.free(prices);
    const cats = try gpa.alloc([*:0]const u8, n);
    defer gpa.free(cats);
    for (0..n) |i| {
        ids[i] = @intCast(i + 1);
        prices[i] = 1;
        cats[i] = "a";
    }
    try parquet.writeSalesRows("/tmp/glacier_window_limit.parquet", ids, prices, cats);
    var session = try Session.open(gpa, io, "/tmp/glacier_window_limit.parquet");
    defer session.close();
    {
        var result = try session.execute(
            \\SELECT id, SUM(price) OVER (
            \\  PARTITION BY category ORDER BY id
            \\  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            \\) AS running
            \\FROM glacier_window_limit
            \\ORDER BY id
            \\LIMIT 20
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 20), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 20), b.columns[0].i64s[19]);
        try std.testing.expectEqual(@as(i64, 20), b.columns[1].i64s[19]);
    }
    {
        var result = try session.execute(
            "SELECT SUM(price) OVER () AS s FROM glacier_window_limit ORDER BY id LIMIT 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 8000), b.columns[0].i64s[0]);
    }
}

test "NULL is null coalesce count skip join" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeNullsFixture("/tmp/glacier_nulls.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier_nulls.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT COUNT(*), COUNT(qty), SUM(qty) FROM glacier_nulls");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 2), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 40), b.columns[2].i64s[0]);
        try std.testing.expect(!b.columns[2].isNull(0));
    }

    {
        var result = try session.execute("SELECT id FROM glacier_nulls WHERE qty IS NULL ORDER BY id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, b.columns[0].i64s[0..2]);
    }

    {
        var result = try session.execute("SELECT id FROM glacier_nulls WHERE qty IS NOT NULL ORDER BY id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, b.columns[0].i64s[0..2]);
    }

    {
        var result = try session.execute("SELECT id FROM glacier_nulls WHERE qty = 10");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute("SELECT coalesce(qty, 0) FROM glacier_nulls ORDER BY id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 4), b.len);
        try std.testing.expectEqualSlices(i64, &.{ 10, 0, 30, 0 }, b.columns[0].i64s[0..4]);
        try std.testing.expectEqual(@as(usize, 0), b.columns[0].valid.len);
    }

    {
        var result = try session.execute("SELECT note FROM glacier_nulls WHERE note IS NULL");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expect(b.columns[0].isNull(0));
    }

    {
        var result = try session.execute("SELECT qty FROM glacier_nulls ORDER BY qty, id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 30), b.columns[0].i64s[1]);
        try std.testing.expect(b.columns[0].isNull(2));
        try std.testing.expect(b.columns[0].isNull(3));
    }

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_nulls a JOIN glacier_nulls b ON a.qty = b.qty",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }

    {
        var result = try session.execute("COPY TO '/tmp/glacier_nulls.glacier'");
        defer result.deinit();
    }
    var copied = try Session.open(gpa, io, "/tmp/glacier_nulls.glacier");
    defer copied.close();
    {
        var result = try copied.execute("SELECT COUNT(qty)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
}

test "CASE LIKE IN BETWEEN NULL UNION" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    {
        var empty = try Session.openEmpty(gpa, io);
        defer empty.close();
        var result = try empty.execute("SELECT NULL");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expect(b.columns[0].isNull(0));

        var u = try empty.execute("SELECT 1 UNION SELECT 1");
        defer u.deinit();
        const ub = u.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), ub.len);
        try std.testing.expectEqual(@as(i64, 1), ub.columns[0].i64s[0]);

        var ua = try empty.execute("SELECT 1 UNION ALL SELECT 1");
        defer ua.deinit();
        const uab = ua.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), uab.len);

        var mix = try empty.execute("SELECT 1 UNION SELECT 2 UNION ALL SELECT 2");
        defer mix.deinit();
        const mb = mix.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), mb.len);

        {
            var uo = try empty.execute("SELECT 2 AS x UNION SELECT 1 UNION ALL SELECT 3 ORDER BY x");
            defer uo.deinit();
            const ob = uo.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 3), ob.len);
            try std.testing.expectEqual(@as(i64, 1), ob.columns[0].i64s[0]);
            try std.testing.expectEqual(@as(i64, 2), ob.columns[0].i64s[1]);
            try std.testing.expectEqual(@as(i64, 3), ob.columns[0].i64s[2]);
        }
        {
            var ul = try empty.execute("SELECT 3 AS x UNION SELECT 1 UNION ALL SELECT 2 ORDER BY x LIMIT 2 OFFSET 1");
            defer ul.deinit();
            const lb = ul.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 2), lb.len);
            try std.testing.expectEqual(@as(i64, 2), lb.columns[0].i64s[0]);
            try std.testing.expectEqual(@as(i64, 3), lb.columns[0].i64s[1]);
        }

        try std.testing.expectError(error.SchemaMismatch, empty.execute("SELECT 1 UNION SELECT 'x'"));
        {
            var scal = try empty.execute("SELECT (SELECT 1)");
            defer scal.deinit();
            const sb1 = scal.nextBatch() orelse return error.EmptyResult;
            try std.testing.expectEqual(@as(usize, 1), sb1.len);
            try std.testing.expectEqual(@as(i64, 1), sb1.columns[0].i64s[0]);
        }

        var arith = try empty.execute("SELECT 1 + 2");
        defer arith.deinit();
        const ab = arith.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), ab.columns[0].i64s[0]);

        var sub = try empty.execute("SELECT * FROM (SELECT 1 AS x)");
        defer sub.deinit();
        const sb = sub.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), sb.len);
        try std.testing.expectEqual(@as(i64, 1), sb.columns[0].i64s[0]);

        var with_q = try empty.execute("WITH t AS (SELECT 1 AS x) SELECT x FROM t");
        defer with_q.deinit();
        const wb = with_q.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), wb.columns[0].i64s[0]);
    }

    try parquet.writeSalesFixture("/tmp/glacier_expr_sales.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier_expr_sales.parquet");
    defer session.close();

    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_expr_sales WHERE category LIKE 'f%'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_expr_sales WHERE category NOT LIKE 'f%'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_expr_sales WHERE price IN (50, 80, 90)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_expr_sales WHERE id NOT IN (1, 2)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 8), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_expr_sales WHERE price BETWEEN 100 AND 150");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT CASE WHEN price > 200 THEN 'high' WHEN price > 100 THEN 'mid' ELSE 'low' END AS band FROM glacier_expr_sales WHERE id = 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("low", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute(
            "SELECT CASE WHEN price > 200 THEN 'high' WHEN price > 100 THEN 'mid' ELSE 'low' END FROM glacier_expr_sales WHERE id = 9",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("high", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute(
            "SELECT CASE WHEN price > 200 THEN price ELSE 0 END FROM glacier_expr_sales WHERE id = 9",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 300), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT id FROM glacier_expr_sales WHERE id <= 2 UNION SELECT id FROM glacier_expr_sales WHERE id <= 2",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
    }
    {
        var result = try session.execute(
            "SELECT id FROM glacier_expr_sales WHERE id <= 2 UNION ALL SELECT id FROM glacier_expr_sales WHERE id <= 2",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 4), b.len);
    }
    {
        var result = try session.execute(
            "SELECT price * 2 FROM glacier_expr_sales WHERE id = 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 100), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT price * 1.1 FROM glacier_expr_sales WHERE id = 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectApproxEqAbs(@as(f64, 55.0), b.columns[0].f64s[0], 1e-9);
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_expr_sales WHERE price > id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT abs(price * -1) FROM glacier_expr_sales WHERE id = 1",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 50), b.columns[0].i64s[0]);
    }
}

test "FROM subquery IN subquery scalar CTE LEFT JOIN" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_sub_sales.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier_sub_sales.parquet");
    defer session.close();

    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM (SELECT id FROM glacier_sub_sales WHERE price > 100) t",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_sub_sales WHERE id IN (SELECT id FROM glacier_sub_sales WHERE price > 100)",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_sub_sales WHERE price > (SELECT MIN(price) FROM glacier_sub_sales)",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 9), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "WITH cheap AS (SELECT id, price FROM glacier_sub_sales WHERE price < 100) SELECT COUNT(*) FROM glacier_sub_sales a LEFT JOIN cheap b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "WITH cheap AS (SELECT id FROM glacier_sub_sales WHERE price < 100) SELECT COUNT(*) FROM glacier_sub_sales a LEFT JOIN cheap b ON a.id = b.id WHERE b.id IS NULL",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 6), b.columns[0].i64s[0]);
    }
    try std.testing.expectError(
        error.UnsupportedSql,
        session.execute("SELECT * FROM glacier_sub_sales a WHERE a.id IN (SELECT b.id FROM glacier_sub_sales b WHERE a.id > 0)"),
    );
    try std.testing.expectError(
        error.UnsupportedSql,
        session.execute("WITH t AS (SELECT * FROM t) SELECT * FROM t"),
    );
    {
        var result = try session.execute("SELECT COUNT(*) FROM glacier_sub_sales WHERE EXISTS (SELECT 1)");
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 10),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_sub_sales WHERE NOT EXISTS (SELECT 1 FROM glacier_sub_sales WHERE price < 0)",
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 10),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM glacier_sub_sales WHERE EXISTS (SELECT 1 FROM glacier_sub_sales WHERE price < 0)",
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 0),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
}

test "expr pipeline script: filter markup copy join" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_pipeline_sales.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier_pipeline_sales.parquet");
    defer session.close();

    const marked_path = "/tmp/glacier_pipeline_marked.glacier";

    var warmup = try session.execute("SELECT 2 * 3 + 1");
    defer warmup.deinit();
    const w = warmup.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(i64, 7), w.columns[0].i64s[0]);

    var counted = try session.execute("SELECT COUNT(*) WHERE price * 1.1 > 110");
    defer counted.deinit();
    const n = (counted.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0];
    try std.testing.expect(n > 0);

    var marked = try session.execute(
        "SELECT id, price * 1.1 AS p, category WHERE price * 1.1 > 110",
    );
    defer marked.deinit();
    const rows = marked.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(usize, @intCast(n)), rows.len);
    var sum_p: f64 = 0;
    for (0..rows.len) |i| sum_p += rows.columns[1].f64s[i];

    var copied = try session.execute(
        "COPY (SELECT id, price * 1.1 AS p, category WHERE price * 1.1 > 110) TO '/tmp/glacier_pipeline_marked.glacier'",
    );
    defer copied.deinit();
    const copy_batch = copied.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(n, copy_batch.columns[0].i64s[0]);
    try std.testing.expectEqualStrings(marked_path, copy_batch.columns[1].strAt(0));

    var native_session = try Session.open(gpa, io, marked_path);
    defer native_session.close();

    var native_count = try native_session.execute("SELECT COUNT(*)");
    defer native_count.deinit();
    try std.testing.expectEqual(
        n,
        (native_count.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
    );

    var native_sum = try native_session.execute("SELECT SUM(p)");
    defer native_sum.deinit();
    const got_sum = (native_sum.nextBatch() orelse return error.EmptyResult).columns[0].f64s[0];
    try std.testing.expectApproxEqAbs(sum_p, got_sum, 1e-6);

    var nested = try native_session.execute("SELECT abs(p * -1) WHERE id = 9");
    defer nested.deinit();
    const nested_b = nested.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(usize, 1), nested_b.len);
    try std.testing.expectApproxEqAbs(@as(f64, 330.0), nested_b.columns[0].f64s[0], 1e-6);

    var joined = try session.execute(
        "SELECT COUNT(*) FROM glacier_pipeline_sales a JOIN '/tmp/glacier_pipeline_marked.glacier' b ON a.id = b.id",
    );
    defer joined.deinit();
    try std.testing.expectEqual(
        n,
        (joined.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
    );

    {
        var lowered = try session.execute("SELECT lower(category) LIMIT 1");
        defer lowered.deinit();
        const b = lowered.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        const s = b.columns[0].strAt(0);
        try std.testing.expect(s.len > 0);
        for (s) |ch| try std.testing.expect(!(ch >= 'A' and ch <= 'Z'));
    }
    {
        var distinct = try session.execute("SELECT COUNT(DISTINCT category)");
        defer distinct.deinit();
        try std.testing.expectEqual(
            @as(i64, 3),
            (distinct.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var vals = try session.execute("SELECT * FROM (VALUES (1))");
        defer vals.deinit();
        const b = vals.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    {
        var sub = try session.execute("SELECT substr(category, 1, 2) LIMIT 1");
        defer sub.deinit();
        const b = sub.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(usize, 2), b.columns[0].strAt(0).len);
    }
    {
        var cat = try session.execute("SELECT concat(category, 'x') LIMIT 1");
        defer cat.deinit();
        const s = (cat.nextBatch() orelse return error.EmptyResult).columns[0].strAt(0);
        try std.testing.expect(s.len > 0);
        try std.testing.expectEqual(@as(u8, 'x'), s[s.len - 1]);
    }
    {
        var trunc = try session.execute("SELECT date_trunc('year', id) LIMIT 1");
        defer trunc.deinit();
        try std.testing.expectEqual(
            @as(i64, 0),
            (trunc.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var grouped = try session.execute(
            "SELECT price * 1.1 FROM glacier_pipeline_sales GROUP BY category",
        );
        defer grouped.deinit();
        try std.testing.expectEqual(
            @as(usize, 3),
            (grouped.nextBatch() orelse return error.EmptyResult).len,
        );
    }
    {
        var up = try session.execute("SELECT upper(category) LIMIT 1");
        defer up.deinit();
        try std.testing.expectEqualStrings("FRUIT", (up.nextBatch() orelse return error.EmptyResult).columns[0].strAt(0));
    }
    {
        var len = try session.execute("SELECT length(category) LIMIT 1");
        defer len.deinit();
        try std.testing.expectEqual(
            @as(i64, 5),
            (len.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var tr = try session.execute("SELECT trim('  x  ')");
        defer tr.deinit();
        try std.testing.expectEqualStrings("x", (tr.nextBatch() orelse return error.EmptyResult).columns[0].strAt(0));
    }
    {
        var rep = try session.execute("SELECT replace('aba', 'a', 'z')");
        defer rep.deinit();
        try std.testing.expectEqualStrings("zbz", (rep.nextBatch() orelse return error.EmptyResult).columns[0].strAt(0));
    }
    {
        var y = try session.execute("SELECT year(0), month(0), day(0), EXTRACT(YEAR FROM 0)");
        defer y.deinit();
        const b = y.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1970), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[2].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1970), b.columns[3].i64s[0]);
    }
    {
        var exists_q = try session.execute("SELECT COUNT(*) FROM glacier_pipeline_sales WHERE EXISTS (SELECT 1)");
        defer exists_q.deinit();
        try std.testing.expectEqual(
            @as(i64, 10),
            (exists_q.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var nexists = try session.execute(
            "SELECT COUNT(*) FROM glacier_pipeline_sales WHERE NOT EXISTS (SELECT 1 FROM glacier_pipeline_sales WHERE price < 0)",
        );
        defer nexists.deinit();
        try std.testing.expectEqual(
            @as(i64, 10),
            (nexists.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var scal = try session.execute("SELECT (SELECT 1)");
        defer scal.deinit();
        try std.testing.expectEqual(
            @as(i64, 1),
            (scal.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var sumd = try session.execute("SELECT SUM(DISTINCT price)");
        defer sumd.deinit();
        try std.testing.expectEqual(
            @as(i64, 1275),
            (sumd.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var lr = try session.execute("SELECT left('fruit', 2), right('fruit', 2)");
        defer lr.deinit();
        const b = lr.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("fr", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("it", b.columns[1].strAt(0));
    }
    {
        var pred = try session.execute("SELECT starts_with('fruit', 'fr'), contains('fruit', 'ui'), strpos('fruit', 'ui')");
        defer pred.deinit();
        const b = pred.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(u8, 1), b.columns[0].bools[0]);
        try std.testing.expectEqual(@as(u8, 1), b.columns[1].bools[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[2].i64s[0]);
    }
    {
        var num = try session.execute("SELECT ceil(1.2), floor(1.8), sign(-4), greatest(1, 3, 2), least(1, 3, 2)");
        defer num.deinit();
        const b = num.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(f64, 2), b.columns[0].f64s[0]);
        try std.testing.expectEqual(@as(f64, 1), b.columns[1].f64s[0]);
        try std.testing.expectEqual(@as(i64, -1), b.columns[2].i64s[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[3].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[4].i64s[0]);
    }
    {
        var hms = try session.execute("SELECT hour(3661000000), minute(3661000000), second(3661000000)");
        defer hms.deinit();
        const b = hms.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[2].i64s[0]);
    }

    std.debug.print("pipeline ok: n={d} sum(p)={d:.4} (COPY+JOIN+expr matched)\n", .{ n, got_sum });
}

test "stress mixed SQL surface and volume" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_stress_sales.parquet");
    var session = try Session.open(gpa, io, "/tmp/glacier_stress_sales.parquet");
    defer session.close();

    {
        var result = try session.execute(
            \\WITH
            \\  fruit AS (
            \\    SELECT id, price, category FROM glacier_stress_sales
            \\    WHERE category LIKE 'f%' AND price BETWEEN 50 AND 200
            \\  ),
            \\  totals AS (
            \\    SELECT COUNT(*) AS n, SUM(price) AS s FROM fruit
            \\  )
            \\SELECT
            \\  (SELECT n FROM totals) AS n,
            \\  (SELECT s FROM totals) AS s,
            \\  upper(concat(left(category, 1), right(category, 3))),
            \\  abs(greatest(price, least(price, 1000))),
            \\  CASE WHEN price > 200 THEN 'high' WHEN price > 100 THEN 'mid' ELSE 'low' END
            \\FROM glacier_stress_sales
            \\WHERE EXISTS (SELECT 1 FROM fruit)
            \\  AND id IN (SELECT id FROM fruit)
            \\  AND price > (SELECT MIN(price) FROM glacier_stress_sales)
            \\ORDER BY price DESC, id
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 4), b.len);
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 445), b.columns[1].i64s[0]);
        try std.testing.expectEqualStrings("FUIT", b.columns[2].strAt(0));
        try std.testing.expectEqual(@as(i64, 150), b.columns[3].i64s[0]);
        try std.testing.expectEqualStrings("mid", b.columns[4].strAt(0));
        try std.testing.expectEqual(@as(i64, 90), b.columns[3].i64s[1]);
        try std.testing.expectEqualStrings("low", b.columns[4].strAt(1));
    }

    {
        var result = try session.execute(
            \\SELECT category, COUNT(*) AS n, SUM(price) AS s, SUM(DISTINCT price) AS sd,
            \\       MIN(price) AS mn, MAX(price) AS mx, AVG(price) AS av
            \\FROM glacier_stress_sales
            \\WHERE price >= (SELECT MIN(price) FROM glacier_stress_sales)
            \\  AND EXISTS (SELECT 1 FROM glacier_stress_sales WHERE price > 0)
            \\  AND category IN (SELECT category FROM glacier_stress_sales)
            \\GROUP BY category
            \\HAVING COUNT(*) >= 2
            \\ORDER BY s DESC
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
        try std.testing.expectEqual(@as(i64, 2), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 500), b.columns[2].i64s[0]);
        try std.testing.expectEqual(@as(i64, 500), b.columns[3].i64s[0]);
        try std.testing.expectEqual(@as(i64, 200), b.columns[4].i64s[0]);
        try std.testing.expectEqual(@as(i64, 300), b.columns[5].i64s[0]);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqual(@as(i64, 5), b.columns[1].i64s[1]);
        try std.testing.expectEqual(@as(i64, 445), b.columns[2].i64s[1]);
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(2));
        try std.testing.expectEqual(@as(i64, 3), b.columns[1].i64s[2]);
        try std.testing.expectEqual(@as(i64, 330), b.columns[2].i64s[2]);
    }

    {
        var result = try session.execute(
            \\SELECT COUNT(*) FROM glacier_stress_sales a
            \\JOIN glacier_stress_sales b ON a.category = b.category
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 38),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }

    {
        var result = try session.execute(
            \\SELECT COUNT(*) FROM (
            \\  SELECT * FROM (
            \\    SELECT id, price FROM glacier_stress_sales WHERE price > 80
            \\  ) x WHERE price < 200
            \\) y
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 5),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }

    {
        var result = try session.execute(
            \\WITH hi AS (SELECT id, price FROM glacier_stress_sales WHERE price >= 150),
            \\     lo AS (SELECT id FROM glacier_stress_sales WHERE price < 100)
            \\SELECT COUNT(*) FROM hi a LEFT JOIN lo b ON a.id = b.id WHERE b.id IS NULL
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 3),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }

    {
        var result = try session.execute(
            \\SELECT upper(category), ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn, price
            \\FROM glacier_stress_sales
            \\WHERE category = 'fruit'
            \\ORDER BY price DESC, id
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 5), b.len);
        try std.testing.expectEqualStrings("FRUIT", b.columns[0].strAt(0));
        try std.testing.expectEqual(@as(i64, 1), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 150), b.columns[2].i64s[0]);
        try std.testing.expectEqual(@as(i64, 5), b.columns[1].i64s[4]);
        try std.testing.expectEqual(@as(i64, 50), b.columns[2].i64s[4]);
    }

    {
        var result = try session.execute(
            \\SELECT category FROM glacier_stress_sales WHERE category = 'fruit'
            \\UNION
            \\SELECT category FROM glacier_stress_sales WHERE category = 'dairy'
            \\UNION ALL
            \\SELECT 'veg'
            \\ORDER BY category
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(2));
    }

    {
        var result = try session.execute(
            \\SELECT category, SUM(price) AS s FROM glacier_stress_sales WHERE category = 'fruit' GROUP BY category
            \\UNION
            \\SELECT category, SUM(price) FROM glacier_stress_sales WHERE category = 'veg' GROUP BY category
            \\UNION ALL
            \\SELECT 'dairy', 500
            \\ORDER BY category DESC
            \\LIMIT 2
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(0));
        try std.testing.expectEqual(@as(i64, 330), b.columns[1].i64s[0]);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqual(@as(i64, 445), b.columns[1].i64s[1]);
    }

    {
        var result = try session.execute(
            \\SELECT x FROM (
            \\  SELECT 2 AS x UNION SELECT 1 UNION ALL SELECT 3
            \\) t ORDER BY x LIMIT 2 OFFSET 1
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[1]);
    }

    {
        var result = try session.execute(
            \\SELECT a.category, COUNT(*) AS n
            \\FROM glacier_stress_sales a
            \\JOIN glacier_stress_sales b ON a.id = b.id
            \\WHERE a.price >= (SELECT MIN(price) FROM glacier_stress_sales)
            \\  AND EXISTS (SELECT 1 FROM glacier_stress_sales WHERE category = 'fruit')
            \\GROUP BY a.category
            \\HAVING COUNT(*) >= 2
            \\ORDER BY n DESC, a.category
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(0));
        try std.testing.expectEqual(@as(i64, 5), b.columns[1].i64s[0]);
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(1));
        try std.testing.expectEqual(@as(i64, 3), b.columns[1].i64s[1]);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(2));
        try std.testing.expectEqual(@as(i64, 2), b.columns[1].i64s[2]);
    }

    {
        var result = try session.execute(
            \\SELECT category, s FROM (
            \\  SELECT category, SUM(price) AS s FROM glacier_stress_sales GROUP BY category
            \\) g
            \\WHERE s > (SELECT AVG(price) FROM glacier_stress_sales)
            \\ORDER BY s DESC
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
        try std.testing.expectEqual(@as(i64, 500), b.columns[1].i64s[0]);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqual(@as(i64, 445), b.columns[1].i64s[1]);
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(2));
        try std.testing.expectEqual(@as(i64, 330), b.columns[1].i64s[2]);
    }

    {
        var result = try session.execute(
            \\SELECT
            \\  (SELECT COUNT(*) FROM glacier_stress_sales) AS n,
            \\  (SELECT SUM(price) FROM glacier_stress_sales) AS s,
            \\  (SELECT COUNT(DISTINCT category) FROM glacier_stress_sales) AS d,
            \\  left(replace(concat(upper(trim('  fruit  ')), 'X'), 'X', 'Y'), 6),
            \\  ceil(1.1) + floor(1.9) + sign(-2),
            \\  hour(0), minute(0), second(0), year(0),
            \\  starts_with('fruit', 'fr'), contains('fruit', 'ui'), strpos('fruit', 'ui')
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1275), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[2].i64s[0]);
        try std.testing.expectEqualStrings("FRUITY", b.columns[3].strAt(0));
        try std.testing.expectEqual(@as(f64, 2.0), b.columns[4].f64s[0]);
        try std.testing.expectEqual(@as(i64, 0), b.columns[5].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1970), b.columns[8].i64s[0]);
        try std.testing.expectEqual(@as(u8, 1), b.columns[9].bools[0]);
        try std.testing.expectEqual(@as(u8, 1), b.columns[10].bools[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[11].i64s[0]);
    }

    {
        var result = try session.execute(
            "SELECT (SELECT id FROM glacier_stress_sales WHERE price < 0)",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expect(b.columns[0].isNull(0));
    }

    try std.testing.expectError(
        error.SubqueryCardinality,
        session.execute("SELECT (SELECT id FROM glacier_stress_sales)"),
    );
    try std.testing.expectError(
        error.UnsupportedSql,
        session.execute(
            "SELECT * FROM glacier_stress_sales a WHERE EXISTS (SELECT 1 FROM glacier_stress_sales b WHERE a.id = b.id)",
        ),
    );

    {
        var result = try session.execute(
            \\SELECT COUNT(*) FROM glacier_stress_sales
            \\WHERE (price > 100 AND category IN ('fruit', 'veg'))
            \\   OR (price < 80 AND NOT EXISTS (SELECT 1 FROM glacier_stress_sales WHERE price < 0))
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 5),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }

    {
        var result = try session.execute(
            \\SELECT 1 UNION ALL SELECT 1 UNION SELECT 2 UNION ALL SELECT id
            \\FROM glacier_stress_sales WHERE id = 2
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
    }

    const n: usize = 2500;
    const ids = try gpa.alloc(i64, n);
    defer gpa.free(ids);
    const prices = try gpa.alloc(i64, n);
    defer gpa.free(prices);
    const cats = try gpa.alloc([*:0]const u8, n);
    defer gpa.free(cats);
    const pool = [_][:0]const u8{ "fruit", "veg", "dairy" };
    var expect_sum: i64 = 0;
    for (0..n) |i| {
        ids[i] = @intCast(i + 1);
        const p: i64 = @intCast((i % 97) + 1);
        prices[i] = p;
        expect_sum += p;
        cats[i] = pool[i % 3].ptr;
    }
    try parquet.writeSalesRows("/tmp/glacier_stress_volume.parquet", ids, prices, cats);
    var vol = try Session.open(gpa, io, "/tmp/glacier_stress_volume.parquet");
    defer vol.close();

    {
        var result = try vol.execute(
            \\SELECT COUNT(*), SUM(price), COUNT(DISTINCT category), MIN(price), MAX(price)
            \\FROM glacier_stress_volume
            \\WHERE id IN (SELECT id FROM glacier_stress_volume WHERE price > 0)
            \\  AND EXISTS (SELECT 1)
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2500), b.columns[0].i64s[0]);
        try std.testing.expectEqual(expect_sum, b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 3), b.columns[2].i64s[0]);
        try std.testing.expectEqual(@as(i64, 1), b.columns[3].i64s[0]);
        try std.testing.expectEqual(@as(i64, 97), b.columns[4].i64s[0]);
    }
    {
        var result = try vol.execute(
            "SELECT COUNT(*) FROM glacier_stress_volume a JOIN glacier_stress_volume b ON a.id = b.id",
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 2500),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var result = try vol.execute(
            \\SELECT category, COUNT(*) AS n, SUM(price) AS s
            \\FROM glacier_stress_volume
            \\GROUP BY category
            \\ORDER BY category
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqual(@as(i64, 833), b.columns[1].i64s[0]);
        try std.testing.expectEqual(@as(i64, 834), b.columns[1].i64s[1]);
        try std.testing.expectEqual(@as(i64, 833), b.columns[1].i64s[2]);
        var grouped: i64 = 0;
        grouped += b.columns[1].i64s[0] + b.columns[1].i64s[1] + b.columns[1].i64s[2];
        try std.testing.expectEqual(@as(i64, 2500), grouped);
        var gsum: i64 = 0;
        gsum += b.columns[2].i64s[0] + b.columns[2].i64s[1] + b.columns[2].i64s[2];
        try std.testing.expectEqual(expect_sum, gsum);
    }
    {
        var result = try vol.execute(
            "SELECT COUNT(*) OVER () FROM glacier_stress_volume LIMIT 1",
        );
        defer result.deinit();
        try std.testing.expectEqual(
            @as(i64, 2500),
            (result.nextBatch() orelse return error.EmptyResult).columns[0].i64s[0],
        );
    }
    {
        var result = try vol.execute(
            \\SELECT COUNT(*) FROM (
            \\  SELECT id FROM glacier_stress_volume WHERE price > 50
            \\) t
            \\WHERE id IN (SELECT id FROM glacier_stress_volume WHERE id <= 2000)
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        var expect_hi: i64 = 0;
        for (0..n) |i| {
            const p: i64 = @intCast((i % 97) + 1);
            const id: i64 = @intCast(i + 1);
            if (p > 50 and id <= 2000) expect_hi += 1;
        }
        try std.testing.expectEqual(expect_hi, b.columns[0].i64s[0]);
    }
    {
        var result = try vol.execute(
            \\SELECT category FROM glacier_stress_volume WHERE id <= 5
            \\UNION
            \\SELECT category FROM glacier_stress_volume WHERE id > 2495
            \\ORDER BY category
            \\LIMIT 3
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("dairy", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(1));
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(2));
    }
}

fn expectRestPruneCount(gpa: std.mem.Allocator, io: std.Io, opts: rest_catalog.Options) !void {
    var session = try Session.openRest(gpa, io, opts);
    defer session.close();
    var result = try session.execute("SELECT COUNT(*) FROM default.prune");
    defer result.deinit();
    const b = result.nextBatch() orelse return error.EmptyResult;
    try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    try std.testing.expectEqualStrings("ya29.test", session.gcs_token.?);
    try std.testing.expectEqualStrings("proj", session.gcs_user_project.?);
}

test "REST catalog loadTable COUNT(*)" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_rest_iceberg");
    var srv = try remote_test.spawnRestCatalog(io, "/tmp/glacier_rest_iceberg", "", "none", "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);
    try expectRestPruneCount(gpa, io, .{ .endpoint = endpoint, .warehouse = "wh" });
}

test "REST catalog bearer and extra header" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_rest_iceberg");
    var srv = try remote_test.spawnRestCatalog(io, "/tmp/glacier_rest_iceberg", "tok-1", "bearer", "X-Custom: glacier");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);

    try std.testing.expectError(error.AccessDenied, Session.openRest(gpa, io, .{
        .endpoint = endpoint,
        .token = "wrong",
        .extra_headers = &.{.{ .name = "X-Custom", .value = "glacier" }},
    }));

    const headers = [_]rest_catalog.Header{.{ .name = "X-Custom", .value = "glacier" }};
    try expectRestPruneCount(gpa, io, .{
        .endpoint = endpoint,
        .token = "tok-1",
        .extra_headers = &headers,
    });
}

test "REST catalog OAuth client_credentials" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_rest_iceberg");
    var srv = try remote_test.spawnRestCatalog(io, "/tmp/glacier_rest_iceberg", "oauth-tok", "oauth", "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);
    try expectRestPruneCount(gpa, io, .{
        .endpoint = endpoint,
        .oauth_client_id = "id",
        .oauth_client_secret = "s3cret",
        .auth = .oauth2,
    });
}

test "REST catalog SigV4" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_rest_iceberg");
    var srv = try remote_test.spawnRestCatalog(io, "/tmp/glacier_rest_iceberg", "", "sigv4", "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);

    const ak = try remote_test.captureEnv(gpa, "AWS_ACCESS_KEY_ID");
    const sk = try remote_test.captureEnv(gpa, "AWS_SECRET_ACCESS_KEY");
    const region = try remote_test.captureEnv(gpa, "AWS_REGION");
    const tok = try remote_test.captureEnv(gpa, "AWS_SESSION_TOKEN");
    defer {
        remote_test.restoreEnv(gpa, tok);
        remote_test.restoreEnv(gpa, region);
        remote_test.restoreEnv(gpa, sk);
        remote_test.restoreEnv(gpa, ak);
    }
    _ = remote_test.posix_env.setenv("AWS_ACCESS_KEY_ID", "AKIATEST", 1);
    _ = remote_test.posix_env.setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", 1);
    _ = remote_test.posix_env.setenv("AWS_REGION", "us-east-1", 1);
    _ = remote_test.posix_env.unsetenv("AWS_SESSION_TOKEN");

    try expectRestPruneCount(gpa, io, .{
        .endpoint = endpoint,
        .auth = .sigv4,
        .sigv4_service = "glue",
    });
}

test "ATTACH REST catalog SHOW NAMESPACES and three-level FROM" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_rest_iceberg");
    var srv = try remote_test.spawnRestCatalog(io, "/tmp/glacier_rest_iceberg", "", "none", "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        const stmt = try std.fmt.allocPrint(gpa, "ATTACH '{s}' AS lake", .{endpoint});
        defer gpa.free(stmt);
        var result = try session.execute(stmt);
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("lake", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("iceberg_rest", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SHOW NAMESPACES FROM lake");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("default", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("lake", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SHOW TABLES FROM lake.default");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("prune", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("lake", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("default", b.columns[2].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.default.prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("DESCRIBE lake.default.prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("id", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("long", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("category", b.columns[0].strAt(2));
    }
    {
        var result = try session.execute("USE lake.default");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("lake", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("default", b.columns[1].strAt(0));
    }
}

test "JOIN two attached REST catalogs" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_rest_iceberg_a");
    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_rest_iceberg_b");
    var srv_a = try remote_test.spawnRestCatalog(io, "/tmp/glacier_rest_iceberg_a", "", "none", "");
    defer srv_a.child.kill(io);
    var srv_b = try remote_test.spawnRestCatalog(io, "/tmp/glacier_rest_iceberg_b", "", "none", "");
    defer srv_b.child.kill(io);

    const ep_a = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv_a.port});
    defer gpa.free(ep_a);
    const ep_b = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv_b.port});
    defer gpa.free(ep_b);

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        const stmt = try std.fmt.allocPrint(gpa, "ATTACH '{s}' AS lake_a", .{ep_a});
        defer gpa.free(stmt);
        var result = try session.execute(stmt);
        defer result.deinit();
    }
    {
        const stmt = try std.fmt.allocPrint(gpa, "ATTACH '{s}' AS lake_b", .{ep_b});
        defer gpa.free(stmt);
        var result = try session.execute(stmt);
        defer result.deinit();
    }
    {
        var result = try session.execute("SHOW CATALOGS");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 3), b.len);
        try std.testing.expectEqualStrings("lake_a", b.columns[0].strAt(1));
        try std.testing.expectEqualStrings("iceberg_rest", b.columns[1].strAt(1));
        try std.testing.expectEqualStrings("lake_b", b.columns[0].strAt(2));
        try std.testing.expectEqualStrings("iceberg_rest", b.columns[1].strAt(2));
    }
    {
        var result = try session.execute(
            "SELECT COUNT(*) FROM lake_a.default.prune a JOIN lake_b.default.prune b ON a.id = b.id",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
}

test "open HTTP catalog URI registers iceberg_rest" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_open_rest_uri");
    var srv = try remote_test.spawnRestCatalog(io, "/tmp/glacier_open_rest_uri", "", "none", "");
    defer srv.child.kill(io);
    const ep = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(ep);

    var session = try Session.open(gpa, io, ep);
    defer session.close();
    {
        var result = try session.execute("SHOW CATALOGS");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 1), b.len);
        try std.testing.expectEqualStrings("rest", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("iceberg_rest", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM rest.default.prune");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
}

test "CREATE INSERT Iceberg via native catalog" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_native_write";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_native_write' AS lake");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("lake", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("glacier", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("CREATE NAMESPACE lake.sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("sales", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute("CREATE TABLE lake.sales.orders (id BIGINT, category STRING)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("orders", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("lake", b.columns[1].strAt(0));
        try std.testing.expectEqualStrings("sales", b.columns[2].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 0), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("INSERT INTO lake.sales.orders VALUES (1, 'fruit'), (2, 'veg')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("INSERT INTO lake.sales.orders VALUES (3, 'dairy')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.cats AS SELECT category, COUNT(*) AS n FROM lake.sales.orders GROUP BY category",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.cats");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT * FROM glacier.tables");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
    }
    {
        var result = try session.execute("DROP TABLE lake.sales.cats");
        defer result.deinit();
    }
    try std.testing.expectError(error.TableNotFound, session.execute("SELECT COUNT(*) FROM lake.sales.cats"));
}

test "CREATE INSERT PARTITIONED BY identity prunes on scan" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_part_write";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    {
        var result = try session.execute("ATTACH '/tmp/glacier_part_write' AS lake");
        defer result.deinit();
    }
    {
        var result = try session.execute("CREATE NAMESPACE lake.sales");
        defer result.deinit();
    }
    try std.testing.expectError(
        error.ColumnNotFound,
        session.execute("CREATE TABLE lake.sales.bad (id BIGINT) PARTITIONED BY (category)"),
    );
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.orders (id BIGINT, category STRING) PARTITIONED BY (category)",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "INSERT INTO lake.sales.orders VALUES (1, 'fruit'), (2, 'veg'), (3, 'fruit')",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.orders WHERE category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_pruned);
    }
    {
        var result = try session.execute("SELECT path FROM lake.sales.orders.files ORDER BY path");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expect(std.mem.indexOf(u8, b.columns[0].strAt(0), "category=fruit") != null or
            std.mem.indexOf(u8, b.columns[0].strAt(1), "category=fruit") != null);
        try std.testing.expect(std.mem.indexOf(u8, b.columns[0].strAt(0), "category=veg") != null or
            std.mem.indexOf(u8, b.columns[0].strAt(1), "category=veg") != null);
    }
    {
        var result = try session.execute(
            "CREATE TABLE lake.sales.bycat PARTITIONED BY (category) AS SELECT * FROM lake.sales.orders",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM lake.sales.bycat WHERE category = 'veg'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_pruned);
    }
}

test "CREATE TABLE on files catalog is refused" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var session = try Session.openEmpty(gpa, io);
    defer session.close();
    try std.testing.expectError(error.WriteUnsupported, session.execute("CREATE TABLE t (id BIGINT)"));
    try std.testing.expectError(error.WriteUnsupported, session.execute("INSERT INTO t VALUES (1)"));
    try std.testing.expectError(error.WriteUnsupported, session.execute("COPY t FROM '/tmp/x.parquet'"));
    try std.testing.expectError(error.WriteUnsupported, session.execute("DELETE FROM t WHERE id = 1"));
    try std.testing.expectError(error.WriteUnsupported, session.execute("UPDATE t SET id = 2"));
    try std.testing.expectError(error.WriteUnsupported, session.execute("MERGE INTO t USING t s ON t.id = s.id WHEN MATCHED THEN DELETE"));
    try std.testing.expectError(error.WriteUnsupported, session.execute("ALTER TABLE t ADD COLUMN note STRING"));
}

test "CREATE INSERT Iceberg Hadoop warehouse" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_hadoop_write";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try iceberg.writeWarehouseFixture(gpa, io, root);

    var session = try Session.open(gpa, io, root);
    defer session.close();
    {
        var result = try session.execute("CREATE TABLE hadoop.sales.extra (id BIGINT, category STRING)");
        defer result.deinit();
    }
    {
        var result = try session.execute("INSERT INTO hadoop.sales.extra VALUES (1, 'fruit'), (2, 'veg')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM hadoop.sales.extra");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
}

test "CREATE INSERT Iceberg REST commitTable" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_rest_write";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var srv = try remote_test.spawnRestWriteCatalog(io, root, "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);
    var session = try Session.openRest(gpa, io, .{ .endpoint = endpoint });
    defer session.close();
    {
        var result = try session.execute("CREATE NAMESPACE sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("sales", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("rest", b.columns[1].strAt(0));
    }
    {
        var result = try session.execute("CREATE TABLE sales.orders (id BIGINT, category STRING)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("orders", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 0), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("INSERT INTO sales.orders VALUES (1, 'fruit'), (2, 'veg')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        const a = session.catalog_arena.allocator();
        const client = session.rest orelse return error.TestUnexpectedResult;
        const loaded = try client.loadTable(a, session.transport(), "sales", "orders");
        var stale = loaded.metadata;
        stale.current_snapshot_id = null;
        try std.testing.expectError(error.CommitConflict, client.commitAppend(
            a,
            session.transport(),
            "sales",
            "orders",
            stale,
            .{
                .snapshot = .{
                    .snapshot_id = 1,
                    .timestamp_ms = 1,
                    .manifest_list = "metadata/snap-1.avro",
                    .sequence_number = 1,
                    .schema_id = 0,
                },
                .record_count = 1,
            },
        ));
    }
    {
        var result = try session.execute("INSERT INTO sales.orders VALUES (3, 'dairy')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "CREATE TABLE sales.cats AS SELECT category, COUNT(*) AS n FROM sales.orders GROUP BY category",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.cats");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("DROP TABLE sales.cats");
        defer result.deinit();
    }
    try std.testing.expectError(error.TableNotFound, session.execute("SELECT COUNT(*) FROM sales.cats"));
}

test "CREATE INSERT Iceberg REST glacier serve" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    const rest_server = @import("table/rest_server.zig");
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_rest_serve";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);

    var h = try rest_server.bind(gpa, io, root, "127.0.0.1:0");
    var fut = try io.concurrent(rest_server.serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{h.port()});
    defer gpa.free(endpoint);
    var sess = try Session.openRest(gpa, io, .{ .endpoint = endpoint });
    defer sess.close();
    {
        var result = try sess.execute("CREATE NAMESPACE sales");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("sales", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("rest", b.columns[1].strAt(0));
    }
    {
        var result = try sess.execute("CREATE TABLE sales.orders (id BIGINT, category STRING)");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("orders", b.columns[0].strAt(0));
    }
    {
        var result = try sess.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 0), b.columns[0].i64s[0]);
    }
    {
        var result = try sess.execute("INSERT INTO sales.orders VALUES (1, 'fruit'), (2, 'veg')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try sess.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try sess.execute("SELECT COUNT(*) FROM glacier.snapshots");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expect(b.columns[0].i64s[0] >= 1);
    }
    {
        var result = try sess.execute("SELECT COUNT(*) FROM glacier.files");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expect(b.columns[0].i64s[0] >= 1);
    }
    {
        var result = try sess.execute("SELECT COUNT(*) FROM sales.orders.snapshots");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expect(b.columns[0].i64s[0] >= 1);
    }
    {
        const a = sess.catalog_arena.allocator();
        const client = sess.rest orelse return error.TestUnexpectedResult;
        const loaded = try client.loadTable(a, sess.transport(), "sales", "orders");
        var stale = loaded.metadata;
        stale.current_snapshot_id = null;
        try std.testing.expectError(error.CommitConflict, client.commitAppend(
            a,
            sess.transport(),
            "sales",
            "orders",
            stale,
            .{
                .snapshot = .{
                    .snapshot_id = 1,
                    .timestamp_ms = 1,
                    .manifest_list = "metadata/snap-1.avro",
                    .sequence_number = 1,
                    .schema_id = 0,
                },
                .record_count = 1,
            },
        ));
    }
}

test "CREATE INSERT Iceberg REST PARTITIONED BY identity" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_rest_part";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var srv = try remote_test.spawnRestWriteCatalog(io, root, "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);
    var session = try Session.openRest(gpa, io, .{ .endpoint = endpoint });
    defer session.close();
    {
        var result = try session.execute("CREATE NAMESPACE sales");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "CREATE TABLE sales.orders (id BIGINT, category STRING) PARTITIONED BY (identity(category))",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "INSERT INTO sales.orders VALUES (1, 'fruit'), (2, 'veg'), (3, 'fruit')",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders WHERE category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_pruned);
    }
}

test "COPY FROM parquet into Iceberg REST catalog" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try parquet.writeSalesFixture("/tmp/glacier_copy_from_rest.parquet");
    const root = "/tmp/glacier_rest_copy_from";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var srv = try remote_test.spawnRestWriteCatalog(io, root, "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);
    var session = try Session.openRest(gpa, io, .{ .endpoint = endpoint });
    defer session.close();
    {
        var result = try session.execute("CREATE NAMESPACE sales");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "CREATE TABLE sales.orders (id BIGINT, price BIGINT, category STRING) PARTITIONED BY (category)",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("COPY sales.orders FROM '/tmp/glacier_copy_from_rest.parquet'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 10), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders WHERE category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 5), b.columns[0].i64s[0]);
        try std.testing.expectEqual(@as(usize, 1), result.scan_stats.files_opened);
        try std.testing.expectEqual(@as(usize, 2), result.scan_stats.files_pruned);
    }
}

test "DELETE FROM Iceberg REST equality deletes" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_rest_delete";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var srv = try remote_test.spawnRestWriteCatalog(io, root, "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);
    var session = try Session.openRest(gpa, io, .{ .endpoint = endpoint });
    defer session.close();
    {
        var result = try session.execute("CREATE NAMESPACE sales");
        defer result.deinit();
    }
    {
        var result = try session.execute("CREATE TABLE sales.orders (id BIGINT, category STRING)");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "INSERT INTO sales.orders VALUES (1, 'fruit'), (2, 'veg'), (3, 'fruit')",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("DELETE FROM sales.orders WHERE category = 'fruit'");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("INSERT INTO sales.orders VALUES (4, 'dairy')");
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
}

test "UPDATE ALTER Iceberg REST" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_rest_update";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var srv = try remote_test.spawnRestWriteCatalog(io, root, "");
    defer srv.child.kill(io);

    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{srv.port});
    defer gpa.free(endpoint);
    var session = try Session.openRest(gpa, io, .{ .endpoint = endpoint });
    defer session.close();
    {
        var result = try session.execute("CREATE NAMESPACE sales");
        defer result.deinit();
    }
    {
        var result = try session.execute("CREATE TABLE sales.orders (id BIGINT, category STRING)");
        defer result.deinit();
    }
    {
        var result = try session.execute(
            "INSERT INTO sales.orders VALUES (1, 'fruit'), (2, 'veg')",
        );
        defer result.deinit();
    }
    {
        var result = try session.execute("UPDATE sales.orders SET category = 'x' WHERE id = 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT category FROM sales.orders WHERE id = 1");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqualStrings("x", b.columns[0].strAt(0));
    }
    {
        var result = try session.execute("ALTER TABLE sales.orders ADD COLUMN note STRING");
        defer result.deinit();
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders WHERE note IS NULL");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute(
            "INSERT INTO sales.orders SELECT id + 10, category FROM sales.orders",
        );
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 4), b.columns[0].i64s[0]);
    }
}

test "CREATE INSERT Iceberg REST PutObject s3 location" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const store = "/tmp/glacier_rest_s3_store";
    const root = "/tmp/glacier_rest_s3_cat";
    std.Io.Dir.cwd().deleteTree(io, store) catch {};
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, store);

    var files = try remote_test.spawnFileServer(io, store, "/glacier-test", true);
    defer files.child.kill(io);
    var rest = try remote_test.spawnRestWriteCatalog(io, root, "s3://glacier-test");
    defer rest.child.kill(io);

    const s3_endpoint = try std.fmt.allocPrintSentinel(gpa, "http://127.0.0.1:{d}", .{files.port}, 0);
    defer gpa.free(s3_endpoint);
    const rest_endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{rest.port});
    defer gpa.free(rest_endpoint);

    const ak = try remote_test.captureEnv(gpa, "AWS_ACCESS_KEY_ID");
    const sk = try remote_test.captureEnv(gpa, "AWS_SECRET_ACCESS_KEY");
    const region = try remote_test.captureEnv(gpa, "AWS_REGION");
    const ep = try remote_test.captureEnv(gpa, "AWS_ENDPOINT_URL");
    const tok = try remote_test.captureEnv(gpa, "AWS_SESSION_TOKEN");
    defer {
        remote_test.restoreEnv(gpa, tok);
        remote_test.restoreEnv(gpa, ep);
        remote_test.restoreEnv(gpa, region);
        remote_test.restoreEnv(gpa, sk);
        remote_test.restoreEnv(gpa, ak);
    }
    _ = remote_test.posix_env.setenv("AWS_ACCESS_KEY_ID", "AKIATEST", 1);
    _ = remote_test.posix_env.setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", 1);
    _ = remote_test.posix_env.setenv("AWS_REGION", "us-east-1", 1);
    _ = remote_test.posix_env.setenv("AWS_ENDPOINT_URL", s3_endpoint, 1);
    _ = remote_test.posix_env.unsetenv("AWS_SESSION_TOKEN");

    var session = try Session.openRest(gpa, io, .{ .endpoint = rest_endpoint });
    defer session.close();
    {
        var result = try session.execute("CREATE NAMESPACE sales");
        defer result.deinit();
    }
    {
        var result = try session.execute("CREATE TABLE sales.orders (id BIGINT, category STRING)");
        defer result.deinit();
    }
    {
        var result = try session.execute("INSERT INTO sales.orders VALUES (1, 'fruit'), (2, 'veg')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 2), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT category FROM sales.orders ORDER BY id");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(usize, 2), b.len);
        try std.testing.expectEqualStrings("fruit", b.columns[0].strAt(0));
        try std.testing.expectEqualStrings("veg", b.columns[0].strAt(1));
    }
    {
        var result = try session.execute("INSERT INTO sales.orders VALUES (3, 'dairy')");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 1), b.columns[0].i64s[0]);
    }
    {
        var result = try session.execute("SELECT COUNT(*) FROM sales.orders");
        defer result.deinit();
        const b = result.nextBatch() orelse return error.EmptyResult;
        try std.testing.expectEqual(@as(i64, 3), b.columns[0].i64s[0]);
    }

    var found_parquet = false;
    var found_avro = false;
    {
        var data_dir = std.Io.Dir.cwd().openDir(io, store ++ "/sales/orders/data", .{ .iterate = true }) catch
            return error.TestUnexpectedResult;
        defer data_dir.close(io);
        var it = data_dir.iterate();
        while (try it.next(io)) |entry| {
            if (std.mem.endsWith(u8, entry.name, ".parquet")) found_parquet = true;
        }
    }
    {
        var meta_dir = std.Io.Dir.cwd().openDir(io, store ++ "/sales/orders/metadata", .{ .iterate = true }) catch
            return error.TestUnexpectedResult;
        defer meta_dir.close(io);
        var it = meta_dir.iterate();
        while (try it.next(io)) |entry| {
            if (std.mem.endsWith(u8, entry.name, ".avro")) found_avro = true;
        }
    }
    try std.testing.expect(found_parquet);
    try std.testing.expect(found_avro);
}
