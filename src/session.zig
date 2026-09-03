//! Session is the only entry the REPL / C ABI should call.

const std = @import("std");
const sql = @import("sql/parser.zig");
const physical = @import("execution/physical.zig");
const batch_mod = @import("execution/batch.zig");
const parquet = @import("formats/parquet_wrap.zig");
const avro = @import("formats/avro_wrap.zig");
const iceberg = @import("table/iceberg.zig");
const errmod = @import("error.zig");
const vfs = @import("vfs/source.zig");
const cache = @import("vfs/cache.zig");
const spill = @import("vfs/spill.zig");
const native = @import("formats/native.zig");

pub const Batch = batch_mod.Batch;
pub const Column = batch_mod.Column;
pub const DataType = batch_mod.DataType;
pub const ScanStats = physical.ScanStats;
pub const GlacierError = errmod.GlacierError;

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
    error_buf: [512]u8 = undefined,
    last_error: ?GlacierError = null,

    pub fn lastError(self: *const Session) ?GlacierError {
        return self.last_error;
    }

    fn transport(self: *Session) vfs.Transport {
        return .{ .allocator = self.gpa, .io = self.io, .http = &self.http };
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
        return acc;
    }

    fn runLeafEnv(
        self: *Session,
        a: std.mem.Allocator,
        query: sql.Query,
        stats: *ScanStats,
        env: []const NamedBatch,
    ) !Batch {
        var q = try self.rewritePreds(a, query, stats, env);
        if (q.isLiteralOnly()) {
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
            if (item == .case) {
                for (item.case.arms) |arm| {
                    if (predHasSub(arm.when)) need = true;
                }
            }
        }
        if (!need) return items;
        const out = try a.dupe(sql.SelectItem, items);
        for (out) |*item| {
            if (item.* != .case) continue;
            const arms = try a.dupe(sql.CaseArm, item.case.arms);
            for (arms) |*arm| {
                arm.when = try self.rewriteBool(a, arm.when, outer, stats, env);
            }
            item.case.arms = arms;
        }
        return out;
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
                const node = try a.create(sql.BoolExpr);
                node.* = .{ .cmp = .{ .op = p.op, .left = p.left, .literal = lit } };
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
        if (item == .case) {
            for (item.case.arms) |arm| {
                if (predHasSub(arm.when)) return true;
            }
        }
    }
    return false;
}

fn predHasSub(expr: ?*sql.BoolExpr) bool {
    const e = expr orelse return false;
    return switch (e.*) {
        .in_query, .cmp_query => true,
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

    try std.testing.expectError(error.UnsupportedSql, session.execute("SELECT (SELECT 1) FROM sales"));
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

        try std.testing.expectError(error.SchemaMismatch, empty.execute("SELECT 1 UNION SELECT 'x'"));
        try std.testing.expectError(error.UnsupportedSql, empty.execute("SELECT (SELECT 1)"));

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
}
