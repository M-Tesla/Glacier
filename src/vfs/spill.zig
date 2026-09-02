//! Query temp files under `{GLACIER_TEMP}/spill`. Used when ORDER BY /
//! GROUP BY / DISTINCT pass `GLACIER_MEM` (default 256 MiB).

const std = @import("std");
const cache = @import("cache.zig");
const batch_mod = @import("../execution/batch.zig");

const Batch = batch_mod.Batch;
const Column = batch_mod.Column;
const DataType = batch_mod.DataType;

const magic = "GSP1";
const n_rows_offset: u64 = 8;
pub const partition_count: usize = 16;
pub const default_cap: usize = 256 * 1024 * 1024;

pub var debug_files_written: usize = 0;

var dir_seq: std.atomic.Value(u32) = .init(0);

pub fn memoryCap() usize {
    if (std.c.getenv("GLACIER_MEM")) |p| {
        const s = std.mem.trim(u8, std.mem.span(p), " \t");
        if (s.len > 0) {
            if (std.fmt.parseInt(u64, s, 10)) |n| {
                if (n > 0) return @intCast(n);
            } else |_| {}
        }
    }
    return default_cap;
}

pub fn batchBytes(b: Batch) usize {
    var n: usize = @sizeOf(Batch) + b.columns.len * @sizeOf(Column);
    for (b.columns) |c| {
        n += c.name.len;
        n += c.bools.len;
        n += c.i32s.len * @sizeOf(i32);
        n += c.i64s.len * @sizeOf(i64);
        n += c.f32s.len * @sizeOf(f32);
        n += c.f64s.len * @sizeOf(f64);
        n += c.utf8.offsets.len * @sizeOf(u32) + c.utf8.bytes.len;
        n += c.uuids.len * 16;
        n += c.i128s.len * @sizeOf(i128);
        n += c.valid.len;
    }
    return n;
}

pub const Dir = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    path: []u8,
    seq: u32 = 0,

    pub fn init(gpa: std.mem.Allocator, io: std.Io) !Dir {
        const root = try std.fs.path.join(gpa, &.{ cache.tempRoot(), "spill" });
        defer gpa.free(root);
        try std.Io.Dir.cwd().createDirPath(io, root);
        const n = dir_seq.fetchAdd(1, .monotonic);
        const path = try std.fmt.allocPrint(gpa, "{s}/{d}-{d}", .{ root, std.c.getpid(), n });
        try std.Io.Dir.cwd().createDirPath(io, path);
        return .{ .gpa = gpa, .io = io, .path = path };
    }

    pub fn deinit(self: *Dir) void {
        std.Io.Dir.cwd().deleteTree(self.io, self.path) catch {};
        self.gpa.free(self.path);
        self.* = undefined;
    }

    pub fn nextPath(self: *Dir, arena: std.mem.Allocator) ![]u8 {
        self.seq += 1;
        return std.fmt.allocPrint(arena, "{s}/run-{d}.gsp", .{ self.path, self.seq });
    }
};

pub const ColMeta = struct {
    name: []const u8,
    data_type: DataType,
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
};

pub const Cell = struct {
    boolean: u8 = 0,
    i32: i32 = 0,
    i64: i64 = 0,
    f32: f32 = 0,
    f64: f64 = 0,
    uuid: [16]u8 = @splat(0),
    i128: i128 = 0,
    str: []const u8 = "",
    str_start: usize = 0,
    str_len: usize = 0,
    valid: u8 = 1,
};

pub const RunWriter = struct {
    file: std.Io.File,
    io: std.Io,
    pos: u64,
    n_rows: u64 = 0,
    n_cols: u32,

    pub fn create(io: std.Io, path: []const u8, schema: Batch) !RunWriter {
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
        errdefer file.close(io);
        var self: RunWriter = .{
            .file = file,
            .io = io,
            .pos = 0,
            .n_cols = @intCast(schema.columns.len),
        };
        try self.writeAll(magic);
        try self.writeU32(self.n_cols);
        try self.writeU64(0);
        for (schema.columns) |col| {
            try self.writeU32(@intCast(col.name.len));
            try self.writeAll(col.name);
            try self.writeU8(@intFromEnum(col.data_type));
            try self.writeI32(col.decimal_precision);
            try self.writeI32(col.decimal_scale);
        }
        debug_files_written += 1;
        return self;
    }

    pub fn appendRow(self: *RunWriter, batch: Batch, row: usize) !void {
        if (batch.columns.len != self.n_cols) return error.SchemaMismatch;
        for (batch.columns) |col| try writeCell(self, col, row);
        self.n_rows += 1;
    }

    pub fn close(self: *RunWriter) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, self.n_rows, .little);
        try self.file.writePositionalAll(self.io, &buf, n_rows_offset);
        self.file.close(self.io);
        self.* = undefined;
    }

    fn writeAll(self: *RunWriter, bytes: []const u8) !void {
        if (bytes.len == 0) return;
        try self.file.writePositionalAll(self.io, bytes, self.pos);
        self.pos += bytes.len;
    }

    fn writeU8(self: *RunWriter, v: u8) !void {
        try self.writeAll(&.{v});
    }

    fn writeU32(self: *RunWriter, v: u32) !void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, v, .little);
        try self.writeAll(&b);
    }

    fn writeI32(self: *RunWriter, v: i32) !void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(i32, &b, v, .little);
        try self.writeAll(&b);
    }

    fn writeU64(self: *RunWriter, v: u64) !void {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, v, .little);
        try self.writeAll(&b);
    }
};

fn writeCell(w: *RunWriter, col: Column, row: usize) !void {
    try w.writeU8(if (col.isValid(row)) 1 else 0);
    switch (col.data_type) {
        .boolean => try w.writeU8(col.bools[row]),
        .int32 => {
            var b: [4]u8 = undefined;
            std.mem.writeInt(i32, &b, col.i32s[row], .little);
            try w.writeAll(&b);
        },
        .int64, .timestamp, .timestamptz => {
            var b: [8]u8 = undefined;
            std.mem.writeInt(i64, &b, col.i64s[row], .little);
            try w.writeAll(&b);
        },
        .float32 => {
            var b: [4]u8 = undefined;
            std.mem.writeInt(u32, &b, @bitCast(col.f32s[row]), .little);
            try w.writeAll(&b);
        },
        .float64 => {
            var b: [8]u8 = undefined;
            std.mem.writeInt(u64, &b, @bitCast(col.f64s[row]), .little);
            try w.writeAll(&b);
        },
        .utf8 => {
            const s = col.strAt(row);
            try w.writeU32(@intCast(s.len));
            try w.writeAll(s);
        },
        .uuid => try w.writeAll(&col.uuids[row]),
        .decimal128 => {
            var b: [16]u8 = undefined;
            std.mem.writeInt(i128, &b, col.i128s[row], .little);
            try w.writeAll(&b);
        },
    }
}

pub fn writeBatch(io: std.Io, path: []const u8, batch: Batch) !void {
    var w = try RunWriter.create(io, path, batch);
    errdefer w.file.close(io);
    var row: usize = 0;
    while (row < batch.len) : (row += 1) {
        try w.appendRow(batch, row);
    }
    try w.close();
}

pub const Cursor = struct {
    file: std.Io.File,
    io: std.Io,
    allocator: std.mem.Allocator,
    pos: u64,
    n_rows: u64,
    remaining: u64,
    cols: []ColMeta,
    cells: []Cell,
    scratch: std.ArrayList(u8),

    pub fn open(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Cursor {
        const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
        errdefer file.close(io);
        var self: Cursor = .{
            .file = file,
            .io = io,
            .allocator = allocator,
            .pos = 0,
            .n_rows = 0,
            .remaining = 0,
            .cols = &.{},
            .cells = &.{},
            .scratch = .empty,
        };
        var mag: [4]u8 = undefined;
        try self.readExact(&mag);
        if (!std.mem.eql(u8, &mag, magic)) return error.InvalidSpillFile;
        const n_cols = try self.readU32();
        self.n_rows = try self.readU64();
        self.remaining = self.n_rows;
        self.cols = try allocator.alloc(ColMeta, n_cols);
        errdefer allocator.free(self.cols);
        for (self.cols) |*col| {
            const nlen = try self.readU32();
            const name = try allocator.alloc(u8, nlen);
            try self.readExact(name);
            const tag = try self.readU8();
            const dt = std.enums.fromInt(DataType, tag) orelse return error.InvalidSpillFile;
            const prec = try self.readI32();
            const scale = try self.readI32();
            col.* = .{
                .name = name,
                .data_type = dt,
                .decimal_precision = prec,
                .decimal_scale = scale,
            };
        }
        self.cells = try allocator.alloc(Cell, n_cols);
        return self;
    }

    pub fn close(self: *Cursor) void {
        self.file.close(self.io);
        self.scratch.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn columnIndex(self: Cursor, name: []const u8) ?usize {
        for (self.cols, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, name)) return i;
        }
        return null;
    }

    pub fn next(self: *Cursor) !bool {
        if (self.remaining == 0) return false;
        self.scratch.clearRetainingCapacity();
        for (self.cols, self.cells) |meta, *cell| {
            try self.readCell(meta.data_type, cell);
        }
        for (self.cols, self.cells) |meta, *cell| {
            if (meta.data_type == .utf8) {
                cell.str = self.scratch.items[cell.str_start..][0..cell.str_len];
            }
        }
        self.remaining -= 1;
        return true;
    }

    fn readCell(self: *Cursor, dt: DataType, cell: *Cell) !void {
        cell.valid = try self.readU8();
        switch (dt) {
            .boolean => cell.boolean = try self.readU8(),
            .int32 => cell.i32 = try self.readI32(),
            .int64, .timestamp, .timestamptz => cell.i64 = try self.readI64(),
            .float32 => cell.f32 = @bitCast(try self.readU32()),
            .float64 => cell.f64 = @bitCast(try self.readU64()),
            .utf8 => {
                const n = try self.readU32();
                const start = self.scratch.items.len;
                try self.scratch.resize(self.allocator, start + n);
                if (n > 0) try self.readExact(self.scratch.items[start..][0..n]);
                cell.str_start = start;
                cell.str_len = n;
                cell.str = &.{};
            },
            .uuid => try self.readExact(&cell.uuid),
            .decimal128 => cell.i128 = try self.readI128(),
        }
    }

    fn readExact(self: *Cursor, dest: []u8) !void {
        if (dest.len == 0) return;
        const n = try self.file.readPositionalAll(self.io, dest, self.pos);
        if (n != dest.len) return error.UnexpectedEndOfFile;
        self.pos += n;
    }

    fn readU8(self: *Cursor) !u8 {
        var b: [1]u8 = undefined;
        try self.readExact(&b);
        return b[0];
    }

    fn readU32(self: *Cursor) !u32 {
        var b: [4]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(u32, &b, .little);
    }

    fn readI32(self: *Cursor) !i32 {
        var b: [4]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(i32, &b, .little);
    }

    fn readU64(self: *Cursor) !u64 {
        var b: [8]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(u64, &b, .little);
    }

    fn readI64(self: *Cursor) !i64 {
        var b: [8]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(i64, &b, .little);
    }

    fn readI128(self: *Cursor) !i128 {
        var b: [16]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(i128, &b, .little);
    }
};

pub const Grow = struct {
    allocator: std.mem.Allocator,
    cols: []GrowCol,
    len: usize = 0,

    const GrowCol = struct {
        meta: ColMeta,
        bools: std.ArrayList(u8) = .empty,
        i32s: std.ArrayList(i32) = .empty,
        i64s: std.ArrayList(i64) = .empty,
        f32s: std.ArrayList(f32) = .empty,
        f64s: std.ArrayList(f64) = .empty,
        utf8_bytes: std.ArrayList(u8) = .empty,
        utf8_offsets: std.ArrayList(u32) = .empty,
        uuids: std.ArrayList([16]u8) = .empty,
        i128s: std.ArrayList(i128) = .empty,
        valids: std.ArrayList(u8) = .empty,
    };

    pub fn init(allocator: std.mem.Allocator, metas: []const ColMeta) !Grow {
        const cols = try allocator.alloc(GrowCol, metas.len);
        for (metas, cols) |m, *c| {
            c.* = .{ .meta = m };
            if (m.data_type == .utf8) {
                try c.utf8_offsets.append(allocator, 0);
            }
        }
        return .{ .allocator = allocator, .cols = cols };
    }

    pub fn initFromBatch(allocator: std.mem.Allocator, batch: Batch) !Grow {
        const metas = try allocator.alloc(ColMeta, batch.columns.len);
        for (batch.columns, metas) |col, *m| {
            m.* = .{
                .name = col.name,
                .data_type = col.data_type,
                .decimal_precision = col.decimal_precision,
                .decimal_scale = col.decimal_scale,
            };
        }
        return init(allocator, metas);
    }

    pub fn appendCursor(self: *Grow, cur: *const Cursor) !void {
        if (cur.cols.len != self.cols.len) return error.SchemaMismatch;
        for (self.cols, cur.cols, cur.cells) |*gc, meta, cell| {
            try appendCell(self.allocator, gc, meta.data_type, cell);
        }
        self.len += 1;
    }

    pub fn appendBatch(self: *Grow, batch: Batch, row: usize) !void {
        if (batch.columns.len != self.cols.len) return error.SchemaMismatch;
        for (self.cols, batch.columns) |*gc, col| {
            try appendFromColumn(self.allocator, gc, col, row);
        }
        self.len += 1;
    }

    pub fn freeze(self: *Grow) !Batch {
        const columns = try self.allocator.alloc(Column, self.cols.len);
        for (self.cols, columns) |*gc, *out| {
            out.* = .{
                .name = gc.meta.name,
                .data_type = gc.meta.data_type,
                .len = self.len,
                .decimal_precision = gc.meta.decimal_precision,
                .decimal_scale = gc.meta.decimal_scale,
            };
            switch (gc.meta.data_type) {
                .boolean => out.bools = try gc.bools.toOwnedSlice(self.allocator),
                .int32 => out.i32s = try gc.i32s.toOwnedSlice(self.allocator),
                .int64, .timestamp, .timestamptz => out.i64s = try gc.i64s.toOwnedSlice(self.allocator),
                .float32 => out.f32s = try gc.f32s.toOwnedSlice(self.allocator),
                .float64 => out.f64s = try gc.f64s.toOwnedSlice(self.allocator),
                .utf8 => out.utf8 = .{
                    .offsets = try gc.utf8_offsets.toOwnedSlice(self.allocator),
                    .bytes = try gc.utf8_bytes.toOwnedSlice(self.allocator),
                },
                .uuid => out.uuids = try gc.uuids.toOwnedSlice(self.allocator),
                .decimal128 => out.i128s = try gc.i128s.toOwnedSlice(self.allocator),
            }
            const valids = try gc.valids.toOwnedSlice(self.allocator);
            var any_null = false;
            for (valids) |v| {
                if (v == 0) {
                    any_null = true;
                    break;
                }
            }
            if (any_null) {
                out.valid = valids;
            } else {
                self.allocator.free(valids);
                out.valid = &.{};
            }
        }
        return .{ .columns = columns, .len = self.len };
    }
};

fn appendCell(allocator: std.mem.Allocator, gc: *Grow.GrowCol, dt: DataType, cell: Cell) !void {
    try gc.valids.append(allocator, cell.valid);
    switch (dt) {
        .boolean => try gc.bools.append(allocator, cell.boolean),
        .int32 => try gc.i32s.append(allocator, cell.i32),
        .int64, .timestamp, .timestamptz => try gc.i64s.append(allocator, cell.i64),
        .float32 => try gc.f32s.append(allocator, cell.f32),
        .float64 => try gc.f64s.append(allocator, cell.f64),
        .utf8 => {
            try gc.utf8_bytes.appendSlice(allocator, cell.str);
            try gc.utf8_offsets.append(allocator, @intCast(gc.utf8_bytes.items.len));
        },
        .uuid => try gc.uuids.append(allocator, cell.uuid),
        .decimal128 => try gc.i128s.append(allocator, cell.i128),
    }
}

fn appendFromColumn(allocator: std.mem.Allocator, gc: *Grow.GrowCol, col: Column, row: usize) !void {
    try gc.valids.append(allocator, if (col.isValid(row)) 1 else 0);
    switch (col.data_type) {
        .boolean => try gc.bools.append(allocator, col.bools[row]),
        .int32 => try gc.i32s.append(allocator, col.i32s[row]),
        .int64, .timestamp, .timestamptz => try gc.i64s.append(allocator, col.i64s[row]),
        .float32 => try gc.f32s.append(allocator, col.f32s[row]),
        .float64 => try gc.f64s.append(allocator, col.f64s[row]),
        .utf8 => {
            const s = col.strAt(row);
            try gc.utf8_bytes.appendSlice(allocator, s);
            try gc.utf8_offsets.append(allocator, @intCast(gc.utf8_bytes.items.len));
        },
        .uuid => try gc.uuids.append(allocator, col.uuids[row]),
        .decimal128 => try gc.i128s.append(allocator, col.i128s[row]),
    }
}

pub fn readAll(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Batch {
    var cur = try Cursor.open(allocator, io, path);
    defer cur.close();
    var grow = try Grow.init(allocator, cur.cols);
    while (try cur.next()) {
        try grow.appendCursor(&cur);
    }
    return grow.freeze();
}

test "spill run roundtrip" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var tmp = try Dir.init(gpa, io);
    defer tmp.deinit();

    var ids = [_]i64{ 7, 8, 9 };
    var offs = [_]u32{ 0, 1, 4, 7 };
    var bytes = [_]u8{ 'a', 'b', 'b', 'b', 'c', 'c', 'c' };
    var columns = [_]Column{
        .{ .name = "id", .data_type = .int64, .len = 3, .i64s = &ids },
        .{
            .name = "cat",
            .data_type = .utf8,
            .len = 3,
            .utf8 = .{ .offsets = &offs, .bytes = &bytes },
        },
    };
    const batch: Batch = .{ .columns = &columns, .len = 3 };

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const path = try tmp.nextPath(a);
    try writeBatch(io, path, batch);
    const got = try readAll(a, io, path);
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expectEqual(@as(i64, 8), got.columns[0].i64s[1]);
    try std.testing.expectEqualStrings("bbb", got.columns[1].strAt(1));
    try std.testing.expectEqualStrings("ccc", got.columns[1].strAt(2));
}
