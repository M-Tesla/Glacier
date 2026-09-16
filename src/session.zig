//! Session is the only entry the REPL / C ABI should call.

const std = @import("std");
const sql = @import("sql/parser.zig");
const physical = @import("execution/physical.zig");
const batch_mod = @import("execution/batch.zig");
const parquet = @import("formats/parquet_wrap.zig");
const avro = @import("formats/avro_wrap.zig");
const iceberg = @import("table/iceberg.zig");
const rest_catalog = @import("table/rest_catalog.zig");
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
        return .{
            .gpa = gpa,
            .io = io,
            .http = undefined,
            .catalog_arena = catalog_arena,
            .path = path_owned,
            .table_name = table_name,
            .files = files,
            .schema_fields = &.{},
        };
    }

    const table = try iceberg.openTable(a, t, path_owned);
    const schema_fields = if (table.metadata.currentSchema()) |s| s.fields else &.{};
    return .{
        .gpa = gpa,
        .io = io,
        .http = undefined,
        .catalog_arena = catalog_arena,
        .path = path_owned,
        .table_name = table_name,
        .files = table.files,
        .schema_fields = schema_fields,
    };
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
        return .{
            .gpa = gpa,
            .io = io,
            .http = .{ .allocator = gpa, .io = io },
            .catalog_arena = catalog_arena,
            .path = try a.dupe(u8, ""),
            .table_name = try a.dupe(u8, ""),
            .files = &.{},
            .schema_fields = &.{},
        };
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
        if (resolved.default_table) |name| {
            session.files = session.filesFor(a, name) catch |err| {
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
        return .{
            .gpa = gpa,
            .io = io,
            .http = .{ .allocator = gpa, .io = io },
            .catalog_arena = catalog_arena,
            .path = files[0].path,
            .table_name = try a.dupe(u8, "memory"),
            .files = files,
            .schema_fields = &.{},
        };
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
        }
    }

    fn executeCopy(self: *Session, a: std.mem.Allocator, c: sql.Copy) !Batch {
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
            const files = try self.filesFor(a, query.from);
            const right_files: ?[]const iceberg.DataFile = if (query.join) |j| try self.filesFor(a, j.table) else null;
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
            const left = try self.resolveSource(a, q.from, q.from_sub, stats, env);
            const right = try self.resolveSource(a, j.table, j.sub, stats, env);
            return physical.joinAndTail(a, left, right, q);
        }
        if (q.from_sub != null or (q.from.len > 0 and findNamed(env, q.from) != null)) {
            const input = try self.resolveSource(a, q.from, q.from_sub, stats, env);
            return physical.executeOnBatch(a, input, q);
        }
        const files = try self.filesFor(a, q.from);
        const fallback: ?[]const iceberg.SchemaField = if (self.schema_fields.len == 0) null else self.schema_fields;
        return physical.execute(a, self.transport(), files, q, fallback, stats, null);
    }

    fn resolveSource(
        self: *Session,
        a: std.mem.Allocator,
        name: []const u8,
        sub: ?*sql.Query,
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
        const files = try self.filesFor(a, name);
        const fallback: ?[]const iceberg.SchemaField = if (self.schema_fields.len == 0) null else self.schema_fields;
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

    fn filesFor(self: *Session, arena: std.mem.Allocator, from: []const u8) ![]const iceberg.DataFile {
        if (from.len == 0) return self.files;
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
        if (std.ascii.eqlIgnoreCase(stem, self.table_name)) return self.files;
        if (self.rest) |client| {
            if (self.table_cache.get(from)) |cached| return cached;
            const ident = rest_catalog.splitIdent(from, client.default_namespace);
            const loaded = try client.loadTable(self.catalog_arena.allocator(), self.transport(), ident.ns, ident.name);
            try self.applyCatalogConfig(loaded.config);
            const cat_a = self.catalog_arena.allocator();
            const table = try iceberg.openFromMetadata(
                cat_a,
                self.transport(),
                loaded.metadata,
                rest_catalog.fileBase(loaded),
            );
            const key = try cat_a.dupe(u8, from);
            try self.table_cache.put(cat_a, key, table.files);
            if (self.schema_fields.len == 0) {
                if (table.metadata.currentSchema()) |s| self.schema_fields = s.fields;
            }
            return table.files;
        }
        return error.TableNotFound;
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

fn isParquetPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".parquet");
}

fn isAvroPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".avro");
}

fn isGlacierPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".glacier");
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

const remote_test = if (@import("builtin").is_test) struct {
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

    fn spawnRestCatalog(io: std.Io, root: []const u8, token: []const u8, mode: []const u8, header: []const u8) !struct { child: std.process.Child, port: u16 } {
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
