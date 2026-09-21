//! Public C ABI (`callconv(.c)` export). Format C is behind `formats/*_wrap.zig`.

const std = @import("std");
const builtin = @import("builtin");
const session_mod = @import("session.zig");
const parquet = @import("formats/parquet_wrap.zig");
const arrow = @import("formats/arrow_wrap.zig");
const errmod = @import("error.zig");

pub const ArrowSchema = arrow.Schema;
pub const ArrowArray = arrow.Array;

const Database = struct {
    gpa: std.mem.Allocator,
    threaded: std.Io.Threaded,
    session: session_mod.Session,
};

const ResultHandle = struct {
    gpa: std.mem.Allocator,
    result: ?session_mod.Result = null,
    err: ?[:0]u8 = null,
};

fn heap() std.mem.Allocator {
    if (comptime builtin.cpu.arch == .wasm32) return std.heap.wasm_allocator;
    return std.heap.c_allocator;
}

fn setErr(err_out: ?*?[*:0]u8, msg: []const u8) void {
    const slot = err_out orelse return;
    const z = heap().dupeZ(u8, msg) catch return;
    slot.* = z.ptr;
}

fn failResult(gpa: std.mem.Allocator, msg: []const u8) ?*ResultHandle {
    const h = gpa.create(ResultHandle) catch return null;
    const z = gpa.dupeZ(u8, msg) catch {
        gpa.destroy(h);
        return null;
    };
    h.* = .{ .gpa = gpa, .err = z };
    return h;
}

fn threadedInit(gpa: std.mem.Allocator) std.Io.Threaded {
    if (builtin.cpu.arch == .wasm32) {
        return std.Io.Threaded.init(gpa, .{ .disable_memory_mapping = true });
    }
    return std.Io.Threaded.init(gpa, .{});
}

fn failOpen(gpa: std.mem.Allocator, db: *Database, err_out: ?*?[*:0]u8, msg: []const u8) ?*Database {
    db.threaded.deinit();
    gpa.destroy(db);
    setErr(err_out, msg);
    return null;
}

pub export fn glacier_open(path: ?[*:0]const u8, err_out: ?*?[*:0]u8) callconv(.c) ?*Database {
    const gpa = heap();
    const db = gpa.create(Database) catch {
        setErr(err_out, "out of memory");
        return null;
    };
    db.gpa = gpa;
    db.threaded = threadedInit(gpa);
    if (path == null) {
        db.session = session_mod.Session.openEmpty(gpa, db.threaded.io()) catch {
            return failOpen(gpa, db, err_out, "out of memory");
        };
        return db;
    }
    db.session = session_mod.Session.open(gpa, db.threaded.io(), std.mem.span(path.?)) catch |e| {
        db.threaded.deinit();
        gpa.destroy(db);
        const ge = session_mod.lastOpenError() orelse errmod.GlacierError.fromZig(e);
        setErr(err_out, ge.message);
        return null;
    };
    return db;
}

fn cSlice(p: ?[*:0]const u8) []const u8 {
    return if (p) |z| std.mem.span(z) else "";
}

fn cOptSlice(p: ?[*:0]const u8) ?[]const u8 {
    const s = cSlice(p);
    return if (s.len == 0) null else s;
}

pub export fn glacier_open_catalog(
    uri: ?[*:0]const u8,
    warehouse: ?[*:0]const u8,
    token: ?[*:0]const u8,
    err_out: ?*?[*:0]u8,
) callconv(.c) ?*Database {
    const endpoint = cOptSlice(uri) orelse {
        setErr(err_out, "catalog uri is required");
        return null;
    };
    const gpa = heap();
    const db = gpa.create(Database) catch {
        setErr(err_out, "out of memory");
        return null;
    };
    db.gpa = gpa;
    db.threaded = threadedInit(gpa);
    db.session = session_mod.Session.openRest(gpa, db.threaded.io(), .{
        .endpoint = endpoint,
        .warehouse = cSlice(warehouse),
        .token = cOptSlice(token),
    }) catch |e| {
        db.threaded.deinit();
        gpa.destroy(db);
        const ge = session_mod.lastOpenError() orelse errmod.GlacierError.fromZig(e);
        setErr(err_out, ge.message);
        return null;
    };
    return db;
}

pub export fn glacier_open_buffer(buf: ?*const anyopaque, n: usize, err_out: ?*?[*:0]u8) callconv(.c) ?*Database {
    const ptr = buf orelse {
        setErr(err_out, "buffer is required");
        return null;
    };
    if (n == 0) {
        setErr(err_out, "buffer is empty");
        return null;
    }
    const gpa = heap();
    const db = gpa.create(Database) catch {
        setErr(err_out, "out of memory");
        return null;
    };
    db.gpa = gpa;
    db.threaded = threadedInit(gpa);
    const bytes: [*]const u8 = @ptrCast(ptr);
    db.session = session_mod.Session.openMemory(gpa, db.threaded.io(), bytes[0..n], .parquet) catch |e| {
        db.threaded.deinit();
        gpa.destroy(db);
        const ge = session_mod.lastOpenError() orelse errmod.GlacierError.fromZig(e);
        setErr(err_out, ge.message);
        return null;
    };
    return db;
}

pub export fn glacier_close(db: ?*Database) callconv(.c) void {
    const h = db orelse return;
    h.session.close();
    h.threaded.deinit();
    h.gpa.destroy(h);
}

pub export fn glacier_connect(db: ?*Database, err_out: ?*?[*:0]u8) callconv(.c) ?*Database {
    if (db == null) {
        setErr(err_out, "database is required");
        return null;
    }
    return db;
}

pub export fn glacier_disconnect(_: ?*Database) callconv(.c) void {}

pub export fn glacier_query(conn: ?*Database, sql: ?[*:0]const u8, err_out: ?*?[*:0]u8) callconv(.c) ?*ResultHandle {
    const db = conn orelse {
        setErr(err_out, "connection is required");
        return null;
    };
    const sql_z = sql orelse {
        setErr(err_out, "sql is required");
        return null;
    };
    const executed = db.session.execute(std.mem.span(sql_z)) catch |e| {
        const ge = db.session.lastError() orelse errmod.GlacierError.fromZig(e);
        return failResult(db.gpa, ge.message) orelse {
            setErr(err_out, ge.message);
            return null;
        };
    };
    const h = db.gpa.create(ResultHandle) catch {
        var tmp = executed;
        tmp.deinit();
        setErr(err_out, "out of memory");
        return null;
    };
    h.* = .{ .gpa = db.gpa, .result = executed };
    return h;
}

pub export fn glacier_result_destroy(result: ?*ResultHandle) callconv(.c) void {
    const h = result orelse return;
    if (h.result) |*r| r.deinit();
    if (h.err) |e| h.gpa.free(e);
    h.gpa.destroy(h);
}

pub export fn glacier_result_error(result: ?*ResultHandle) callconv(.c) ?[*:0]const u8 {
    const h = result orelse return null;
    return if (h.err) |e| e.ptr else null;
}

pub export fn glacier_result_arrow(result: ?*ResultHandle, array: ?*ArrowArray, schema: ?*ArrowSchema) callconv(.c) c_int {
    const h = result orelse return -1;
    const out_array = array orelse return -1;
    const out_schema = schema orelse return -1;
    if (h.err != null) return -1;
    const batch = (h.result orelse return -1).batch;
    return arrow.exportBatch(h.gpa, batch, out_array, out_schema);
}

pub export fn glacier_result_next_arrow(result: ?*ResultHandle, array: ?*ArrowArray, schema: ?*ArrowSchema) callconv(.c) c_int {
    const h = result orelse return -1;
    const out_array = array orelse return -1;
    const out_schema = schema orelse return -1;
    if (h.err != null) return -1;
    const r = &(h.result orelse return -1);
    const batch = r.nextBatch() orelse return 0;
    const rc = arrow.exportBatch(h.gpa, batch, out_array, out_schema);
    return if (rc == 0) 1 else -1;
}

pub export fn glacier_malloc(n: usize) callconv(.c) ?*anyopaque {
    if (n == 0) return null;
    const s = heap().alloc(u8, n) catch return null;
    return s.ptr;
}

pub export fn glacier_malloc_free(p: ?*anyopaque, n: usize) callconv(.c) void {
    const ptr = p orelse return;
    if (n == 0) return;
    const bytes: [*]u8 = @ptrCast(ptr);
    heap().free(bytes[0..n]);
}

pub export fn glacier_free(p: ?*anyopaque) callconv(.c) void {
    const ptr = p orelse return;
    const bytes: [*:0]u8 = @ptrCast(ptr);
    heap().free(std.mem.span(bytes));
}

pub export fn glacier_version() callconv(.c) [*:0]const u8 {
    return "0.2.1";
}

pub export fn glacier_api_version() callconv(.c) c_int {
    return 1;
}

const QueryArrow = struct {
    result: *ResultHandle,
    array: ArrowArray,
    schema: ArrowSchema,

    fn deinit(self: *QueryArrow) void {
        if (self.array.release) |rel| rel(&self.array);
        if (self.schema.release) |rel| rel(&self.schema);
        glacier_result_destroy(self.result);
    }

    fn child(self: QueryArrow, i: usize) *ArrowArray {
        return self.array.children.?[i].?;
    }

    fn i64s(self: QueryArrow, col: usize) []const i64 {
        const cld = self.child(col);
        const ptr: [*]const i64 = @ptrCast(@alignCast(cld.buffers.?[1].?));
        return ptr[0..@intCast(cld.length)];
    }

    fn utf8At(self: QueryArrow, col: usize, row: usize) []const u8 {
        const cld = self.child(col);
        const off: [*]const i32 = @ptrCast(@alignCast(cld.buffers.?[1].?));
        const bytes: [*]const u8 = @ptrCast(cld.buffers.?[2].?);
        const start: usize = @intCast(off[row]);
        const end: usize = @intCast(off[row + 1]);
        return bytes[start..end];
    }
};

fn runSql(db: *Database, sql: [*:0]const u8) !QueryArrow {
    var err: ?[*:0]u8 = null;
    const q = glacier_query(db, sql, &err) orelse {
        return error.QueryFailed;
    };
    if (glacier_result_error(q) != null) {
        glacier_result_destroy(q);
        return error.QueryFailed;
    }
    var out: QueryArrow = .{ .result = q, .array = .{}, .schema = .{} };
    if (glacier_result_arrow(q, &out.array, &out.schema) != 0) {
        glacier_result_destroy(q);
        return error.ArrowFailed;
    }
    return out;
}

test "C ABI executes arbitrary SQL not a hardcoded select" {
    try parquet.writeSalesFixture("/tmp/sales.parquet");

    var err: ?[*:0]u8 = null;
    const db = glacier_open("/tmp/sales.parquet", &err) orelse return error.OpenFailed;
    defer glacier_close(db);
    _ = glacier_connect(db, &err) orelse return error.ConnectFailed;

    try std.testing.expectEqualStrings("0.2.1", std.mem.span(glacier_version()));
    try std.testing.expectEqual(@as(c_int, 1), glacier_api_version());

    {
        var q = try runSql(db, "SELECT * WHERE price > 100");
        defer q.deinit();
        try std.testing.expectEqual(@as(i64, 5), q.array.length);
        try std.testing.expectEqual(@as(i64, 3), q.array.n_children);
        try std.testing.expectEqualStrings("id", std.mem.span(q.schema.children.?[0].?.name.?));
        const prices = q.i64s(1);
        for (prices) |p| try std.testing.expect(p > 100);
    }

    {
        var q = try runSql(db, "SELECT category, COUNT(*) AS n GROUP BY category");
        defer q.deinit();
        try std.testing.expectEqual(@as(i64, 3), q.array.length);
        try std.testing.expectEqualStrings("n", std.mem.span(q.schema.children.?[1].?.name.?));
        var fruit: i64 = 0;
        var veg: i64 = 0;
        var dairy: i64 = 0;
        var i: usize = 0;
        while (i < 3) : (i += 1) {
            const name = q.utf8At(0, i);
            const n = q.i64s(1)[i];
            if (std.mem.eql(u8, name, "fruit")) fruit = n;
            if (std.mem.eql(u8, name, "veg")) veg = n;
            if (std.mem.eql(u8, name, "dairy")) dairy = n;
        }
        try std.testing.expectEqual(@as(i64, 5), fruit);
        try std.testing.expectEqual(@as(i64, 3), veg);
        try std.testing.expectEqual(@as(i64, 2), dairy);
    }

    {
        var q = try runSql(db, "SELECT id");
        defer q.deinit();
        try std.testing.expectEqual(@as(i64, 1), q.schema.n_children);
        try std.testing.expectEqual(@as(i64, 10), q.array.length);
        try std.testing.expectEqualStrings("id", std.mem.span(q.schema.children.?[0].?.name.?));
        try std.testing.expectEqualStrings("l", std.mem.span(q.schema.children.?[0].?.format.?));
        const ids = q.i64s(0);
        try std.testing.expectEqual(@as(i64, 1), ids[0]);
        try std.testing.expectEqual(@as(i64, 10), ids[9]);
    }

    {
        var q = try runSql(db, "SELECT id ORDER BY price DESC LIMIT 1");
        defer q.deinit();
        try std.testing.expectEqual(@as(i64, 1), q.array.length);
        try std.testing.expectEqual(@as(i64, 9), q.i64s(0)[0]);
    }

    {
        var qerr: ?[*:0]u8 = null;
        const bad = glacier_query(db, "SELECT * FROM a JOIN b", &qerr);
        try std.testing.expect(bad != null);
        defer glacier_result_destroy(bad);
        try std.testing.expect(qerr == null);
        const msg = glacier_result_error(bad) orelse return error.MissingResultError;
        try std.testing.expectEqualStrings("JOIN is not supported", std.mem.span(msg));
        var arr: ArrowArray = .{};
        var sch: ArrowSchema = .{};
        try std.testing.expectEqual(@as(c_int, -1), glacier_result_arrow(bad, &arr, &sch));
    }
}

test "C ABI next_arrow streams batches" {
    try parquet.writeSalesFixture("/tmp/sales.parquet");

    var err: ?[*:0]u8 = null;
    const db = glacier_open("/tmp/sales.parquet", &err) orelse return error.OpenFailed;
    defer glacier_close(db);
    _ = glacier_connect(db, &err) orelse return error.ConnectFailed;
    db.session.stream_rows = 3;

    const q = glacier_query(db, "SELECT id ORDER BY id", &err) orelse return error.QueryFailed;
    defer glacier_result_destroy(q);
    if (glacier_result_error(q) != null) return error.QueryFailed;

    var n_batches: usize = 0;
    var n_rows: usize = 0;
    var ids: [10]i64 = undefined;
    while (true) {
        var arr: ArrowArray = .{};
        var sch: ArrowSchema = .{};
        const rc = glacier_result_next_arrow(q, &arr, &sch);
        if (rc == 0) break;
        try std.testing.expectEqual(@as(c_int, 1), rc);
        defer {
            if (arr.release) |rel| rel(&arr);
            if (sch.release) |rel| rel(&sch);
        }
        try std.testing.expect(arr.length > 0);
        try std.testing.expect(arr.length <= 3);
        const cld = arr.children.?[0].?;
        const ptr: [*]const i64 = @ptrCast(@alignCast(cld.buffers.?[1].?));
        const chunk = ptr[0..@intCast(cld.length)];
        for (chunk) |id| {
            ids[n_rows] = id;
            n_rows += 1;
        }
        n_batches += 1;
    }
    try std.testing.expect(n_batches >= 2);
    try std.testing.expectEqual(@as(usize, 10), n_rows);
    try std.testing.expectEqual(@as(i64, 1), ids[0]);
    try std.testing.expectEqual(@as(i64, 10), ids[9]);
    {
        var arr: ArrowArray = .{};
        var sch: ArrowSchema = .{};
        try std.testing.expectEqual(@as(c_int, 0), glacier_result_next_arrow(q, &arr, &sch));
    }
}

test "C ABI SELECT 1 on empty session and open_buffer" {
    try parquet.writeSalesFixture("/tmp/sales-abi.parquet");

    {
        var err: ?[*:0]u8 = null;
        const db = glacier_open(null, &err) orelse return error.OpenFailed;
        defer glacier_close(db);
        var q = try runSql(db, "select 1");
        defer q.deinit();
        try std.testing.expectEqual(@as(i64, 1), q.array.length);
        try std.testing.expectEqual(@as(i64, 1), q.i64s(0)[0]);
    }

    const FileSource = @import("vfs/source.zig").FileSource;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    var src = try FileSource.openPath(threaded.io(), "/tmp/sales-abi.parquet");
    defer src.close();
    const bytes = try src.readAll(gpa);
    defer gpa.free(bytes);

    var err: ?[*:0]u8 = null;
    const db = glacier_open_buffer(bytes.ptr, bytes.len, &err) orelse return error.OpenBufferFailed;
    defer glacier_close(db);
    var q = try runSql(db, "SELECT COUNT(*)");
    defer q.deinit();
    try std.testing.expectEqual(@as(i64, 1), q.array.length);
    try std.testing.expectEqual(@as(i64, 10), q.i64s(0)[0]);
}

test "C ABI glacier_open REST URI and glacier_open_catalog" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    const iceberg = @import("table/iceberg.zig");
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try iceberg.writePruneFixture(gpa, io, "/tmp/glacier_abi_rest");
    var srv = try session_mod.remote_test.spawnRestCatalog(io, "/tmp/glacier_abi_rest", "", "none", "");
    defer srv.child.kill(io);
    const uri_owned = try std.fmt.allocPrintSentinel(gpa, "http://127.0.0.1:{d}", .{srv.port}, 0);
    defer gpa.free(uri_owned);

    {
        var err: ?[*:0]u8 = null;
        const db = glacier_open_catalog(null, null, null, &err);
        try std.testing.expect(db == null);
        const msg = err orelse return error.MissingOpenError;
        defer glacier_free(msg);
        try std.testing.expectEqualStrings("catalog uri is required", std.mem.span(msg));
    }

    {
        var err: ?[*:0]u8 = null;
        const db = glacier_open(uri_owned, &err) orelse return error.OpenFailed;
        defer glacier_close(db);
        var q = try runSql(db, "SHOW CATALOGS");
        defer q.deinit();
        try std.testing.expectEqual(@as(i64, 1), q.array.length);
        try std.testing.expectEqualStrings("rest", q.utf8At(0, 0));
        try std.testing.expectEqualStrings("iceberg_rest", q.utf8At(1, 0));
    }

    {
        var err: ?[*:0]u8 = null;
        const db = glacier_open_catalog(uri_owned, "", null, &err) orelse return error.OpenCatalogFailed;
        defer glacier_close(db);
        {
            var q = try runSql(db, "SELECT COUNT(*) FROM rest.default.prune");
            defer q.deinit();
            try std.testing.expectEqual(@as(i64, 10), q.i64s(0)[0]);
        }
        {
            var q = try runSql(db, "SELECT name FROM glacier.catalogs");
            defer q.deinit();
            try std.testing.expectEqualStrings("rest", q.utf8At(0, 0));
        }
    }
}
