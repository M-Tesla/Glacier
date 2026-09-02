//! Native `.glacier` (GLC1): uncompressed columnar pages for COPY.
//! Destination defaults to `{GLACIER_TEMP}/copy`. Not a warehouse (no WAL).

const std = @import("std");
const batch_mod = @import("../execution/batch.zig");
const cache = @import("../vfs/cache.zig");
const iceberg = @import("../table/iceberg.zig");
const sql = @import("../sql/parser.zig");
const aws = @import("../kernel/aws.zig");
const vfs = @import("../vfs/source.zig");

const Batch = batch_mod.Batch;
const Column = batch_mod.Column;
const DataType = batch_mod.DataType;

const magic = "GLC1";
const version: u32 = 2;
const n_rows_off: u64 = 12;
const n_pages_off: u64 = 20;

pub const ColMeta = struct {
    name: []const u8,
    data_type: DataType,
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
};

pub fn resolveDest(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    const with_ext = if (std.ascii.endsWithIgnoreCase(raw, ".glacier"))
        raw
    else
        try std.fmt.allocPrint(allocator, "{s}.glacier", .{raw});
    defer if (with_ext.ptr != raw.ptr) allocator.free(with_ext);

    if (std.fs.path.isAbsolute(with_ext)) return allocator.dupe(u8, with_ext);

    const dir = try std.fs.path.join(allocator, &.{ cache.tempRoot(), "copy" });
    defer allocator.free(dir);
    return std.fs.path.join(allocator, &.{ dir, with_ext });
}

pub const Writer = struct {
    file: std.Io.File,
    io: std.Io,
    pos: u64,
    n_cols: u32,
    n_rows: u64 = 0,
    n_pages: u32 = 0,
    types: []DataType,

    pub fn create(allocator: std.mem.Allocator, io: std.Io, path: []const u8, schema: Batch) !Writer {
        const parent = std.fs.path.dirname(path);
        if (parent) |p| {
            if (p.len > 0) try std.Io.Dir.cwd().createDirPath(io, p);
        }
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
        errdefer file.close(io);
        const types = try allocator.alloc(DataType, schema.columns.len);
        errdefer allocator.free(types);
        for (schema.columns, types) |col, *dt| dt.* = col.data_type;

        var self: Writer = .{
            .file = file,
            .io = io,
            .pos = 0,
            .n_cols = @intCast(schema.columns.len),
            .types = types,
        };
        try self.writeAll(magic);
        try self.writeU32(version);
        try self.writeU32(self.n_cols);
        try self.writeU64(0);
        try self.writeU32(0);
        try self.writeU32(0);
        for (schema.columns) |col| {
            try self.writeU32(@intCast(col.name.len));
            try self.writeAll(col.name);
            try self.writeU8(@intFromEnum(col.data_type));
            try self.writeI32(col.decimal_precision);
            try self.writeI32(col.decimal_scale);
        }
        return self;
    }

    pub fn writePage(self: *Writer, batch: Batch) !void {
        if (batch.columns.len != self.n_cols) return error.SchemaMismatch;
        if (batch.len == 0) return;
        try self.writeU32(@intCast(batch.len));
        for (batch.columns) |col| try writeStats(self, col);
        if (batch.columns.len > 64) return error.UnsupportedType;
        var nbytes: [64]u32 = undefined;
        for (batch.columns, 0..) |col, i| {
            nbytes[i] = payloadBytes(col);
            try self.writeU32(nbytes[i]);
        }
        for (batch.columns) |col| try writePayload(self, col);
        self.n_rows += batch.len;
        self.n_pages += 1;
    }

    pub fn close(self: *Writer, allocator: std.mem.Allocator) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], self.n_rows, .little);
        try self.file.writePositionalAll(self.io, buf[0..8], n_rows_off);
        std.mem.writeInt(u32, buf[0..4], self.n_pages, .little);
        try self.file.writePositionalAll(self.io, buf[0..4], n_pages_off);
        self.file.close(self.io);
        allocator.free(self.types);
        self.* = undefined;
    }

    fn writeAll(self: *Writer, bytes: []const u8) !void {
        if (bytes.len == 0) return;
        try self.file.writePositionalAll(self.io, bytes, self.pos);
        self.pos += bytes.len;
    }

    fn writeU8(self: *Writer, v: u8) !void {
        try self.writeAll(&.{v});
    }

    fn writeU32(self: *Writer, v: u32) !void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, v, .little);
        try self.writeAll(&b);
    }

    fn writeI32(self: *Writer, v: i32) !void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(i32, &b, v, .little);
        try self.writeAll(&b);
    }

    fn writeU64(self: *Writer, v: u64) !void {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, v, .little);
        try self.writeAll(&b);
    }
};

fn payloadBytes(col: Column) u32 {
    const n = col.len;
    const typed: u32 = switch (col.data_type) {
        .boolean => @intCast(n),
        .int32, .float32 => @intCast(n * 4),
        .int64, .timestamp, .timestamptz, .float64 => @intCast(n * 8),
        .utf8 => @intCast((n + 1) * 4 + col.utf8.bytes.len),
        .uuid => @intCast(n * 16),
        .decimal128 => @intCast(n * 16),
    };
    return typed + 4 + @as(u32, @intCast(col.valid.len));
}

fn writeStats(w: *Writer, col: Column) !void {
    const first = firstValidRow(col) orelse {
        try w.writeU8(0);
        return;
    };
    try w.writeU8(1);
    switch (col.data_type) {
        .boolean => {
            var min = col.bools[first];
            var max = min;
            for (col.bools[0..col.len], 0..) |v, i| {
                if (col.isNull(i)) continue;
                min = @min(min, v);
                max = @max(max, v);
            }
            try w.writeU8(min);
            try w.writeU8(max);
        },
        .int32 => {
            var min = col.i32s[first];
            var max = min;
            for (col.i32s[0..col.len], 0..) |v, i| {
                if (col.isNull(i)) continue;
                min = @min(min, v);
                max = @max(max, v);
            }
            var b: [8]u8 = undefined;
            std.mem.writeInt(i32, b[0..4], min, .little);
            std.mem.writeInt(i32, b[4..8], max, .little);
            try w.writeAll(&b);
        },
        .int64, .timestamp, .timestamptz => {
            var min = col.i64s[first];
            var max = min;
            for (col.i64s[0..col.len], 0..) |v, i| {
                if (col.isNull(i)) continue;
                min = @min(min, v);
                max = @max(max, v);
            }
            var b: [16]u8 = undefined;
            std.mem.writeInt(i64, b[0..8], min, .little);
            std.mem.writeInt(i64, b[8..16], max, .little);
            try w.writeAll(&b);
        },
        .float32 => {
            var min = col.f32s[first];
            var max = min;
            for (col.f32s[0..col.len], 0..) |v, i| {
                if (col.isNull(i)) continue;
                min = @min(min, v);
                max = @max(max, v);
            }
            var b: [8]u8 = undefined;
            std.mem.writeInt(u32, b[0..4], @bitCast(min), .little);
            std.mem.writeInt(u32, b[4..8], @bitCast(max), .little);
            try w.writeAll(&b);
        },
        .float64 => {
            var min = col.f64s[first];
            var max = min;
            for (col.f64s[0..col.len], 0..) |v, i| {
                if (col.isNull(i)) continue;
                min = @min(min, v);
                max = @max(max, v);
            }
            var b: [16]u8 = undefined;
            std.mem.writeInt(u64, b[0..8], @bitCast(min), .little);
            std.mem.writeInt(u64, b[8..16], @bitCast(max), .little);
            try w.writeAll(&b);
        },
        .utf8 => {
            var min_s = col.strAt(first);
            var max_s = min_s;
            var i: usize = 0;
            while (i < col.len) : (i += 1) {
                if (col.isNull(i)) continue;
                const s = col.strAt(i);
                if (std.mem.order(u8, s, min_s) == .lt) min_s = s;
                if (std.mem.order(u8, s, max_s) == .gt) max_s = s;
            }
            try w.writeU32(@intCast(min_s.len));
            try w.writeAll(min_s);
            try w.writeU32(@intCast(max_s.len));
            try w.writeAll(max_s);
        },
        .uuid => {
            var min_u = col.uuids[first];
            var max_u = min_u;
            for (col.uuids[0..col.len], 0..) |v, i| {
                if (col.isNull(i)) continue;
                if (std.mem.order(u8, &v, &min_u) == .lt) min_u = v;
                if (std.mem.order(u8, &v, &max_u) == .gt) max_u = v;
            }
            try w.writeAll(&min_u);
            try w.writeAll(&max_u);
        },
        .decimal128 => {
            var min = col.i128s[first];
            var max = min;
            for (col.i128s[0..col.len], 0..) |v, i| {
                if (col.isNull(i)) continue;
                min = @min(min, v);
                max = @max(max, v);
            }
            var b: [32]u8 = undefined;
            std.mem.writeInt(i128, b[0..16], min, .little);
            std.mem.writeInt(i128, b[16..32], max, .little);
            try w.writeAll(&b);
        },
    }
}

fn writePayload(w: *Writer, col: Column) !void {
    const n = col.len;
    try w.writeU32(@intCast(col.valid.len));
    if (col.valid.len > 0) try w.writeAll(col.valid);
    switch (col.data_type) {
        .boolean => try w.writeAll(col.bools[0..n]),
        .int32 => try w.writeAll(std.mem.sliceAsBytes(col.i32s[0..n])),
        .int64, .timestamp, .timestamptz => try w.writeAll(std.mem.sliceAsBytes(col.i64s[0..n])),
        .float32 => try w.writeAll(std.mem.sliceAsBytes(col.f32s[0..n])),
        .float64 => try w.writeAll(std.mem.sliceAsBytes(col.f64s[0..n])),
        .utf8 => {
            try w.writeAll(std.mem.sliceAsBytes(col.utf8.offsets[0 .. n + 1]));
            try w.writeAll(col.utf8.bytes);
        },
        .uuid => try w.writeAll(std.mem.sliceAsBytes(col.uuids[0..n])),
        .decimal128 => try w.writeAll(std.mem.sliceAsBytes(col.i128s[0..n])),
    }
}

pub fn writeBatch(allocator: std.mem.Allocator, io: std.Io, path: []const u8, batch: Batch) !void {
    var w = try Writer.create(allocator, io, path, batch);
    errdefer {
        w.file.close(io);
        allocator.free(w.types);
    }
    try w.writePage(batch);
    try w.close(allocator);
}

pub const Reader = struct {
    file: std.Io.File,
    io: std.Io,
    allocator: std.mem.Allocator,
    pos: u64,
    n_cols: u32,
    n_rows: u64,
    n_pages: u32,
    pages_left: u32,
    cols: []ColMeta,

    pub fn open(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Reader {
        const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
        errdefer file.close(io);
        var self: Reader = .{
            .file = file,
            .io = io,
            .allocator = allocator,
            .pos = 0,
            .n_cols = 0,
            .n_rows = 0,
            .n_pages = 0,
            .pages_left = 0,
            .cols = &.{},
        };
        var mag: [4]u8 = undefined;
        try self.readExact(&mag);
        if (!std.mem.eql(u8, &mag, magic)) return error.InvalidNativeFile;
        if (try self.readU32() != version) return error.InvalidNativeFile;
        self.n_cols = try self.readU32();
        self.n_rows = try self.readU64();
        self.n_pages = try self.readU32();
        _ = try self.readU32();
        self.pages_left = self.n_pages;
        self.cols = try allocator.alloc(ColMeta, self.n_cols);
        for (self.cols) |*col| {
            const nlen = try self.readU32();
            const name = try allocator.alloc(u8, nlen);
            try self.readExact(name);
            const tag = try self.readU8();
            const dt = std.enums.fromInt(DataType, tag) orelse return error.InvalidNativeFile;
            col.* = .{
                .name = name,
                .data_type = dt,
                .decimal_precision = try self.readI32(),
                .decimal_scale = try self.readI32(),
            };
        }
        return self;
    }

    pub fn openLocation(allocator: std.mem.Allocator, t: vfs.Transport, path: []const u8) !Reader {
        if (aws.isRemote(path)) {
            const cached = try cache.materialize(t, path);
            defer t.allocator.free(cached);
            return open(allocator, t.io, cached);
        }
        return open(allocator, t.io, path);
    }

    pub fn close(self: *Reader) void {
        self.file.close(self.io);
        for (self.cols) |col| self.allocator.free(col.name);
        self.allocator.free(self.cols);
        self.* = undefined;
    }

    pub fn numRows(self: Reader) u64 {
        return self.n_rows;
    }

    pub fn nextPage(
        self: *Reader,
        allocator: std.mem.Allocator,
        wanted: ?[]const []const u8,
        where: ?*const sql.BoolExpr,
    ) !?Batch {
        while (self.pages_left > 0) {
            const nrows = try self.readU32();
            var bounds_buf: [32]iceberg.ColBound = undefined;
            var n_bounds: usize = 0;

            for (self.cols) |meta| {
                const has = try self.readU8();
                if (has == 0) continue;
                if (n_bounds < bounds_buf.len) {
                    if (try self.readBound(allocator, meta, &bounds_buf[n_bounds])) {
                        n_bounds += 1;
                    }
                } else {
                    var dummy: iceberg.ColBound = undefined;
                    _ = try self.readBound(allocator, meta, &dummy);
                }
            }

            var nbytes: [64]u32 = undefined;
            if (self.n_cols > nbytes.len) return error.UnsupportedType;
            var i: u32 = 0;
            while (i < self.n_cols) : (i += 1) {
                nbytes[i] = try self.readU32();
            }

            var skip = false;
            if (where) |expr| {
                const fake: iceberg.DataFile = .{
                    .path = "",
                    .bounds = bounds_buf[0..n_bounds],
                };
                skip = iceberg.canSkipFile(fake, expr);
            }
            if (skip) {
                var skip_n: u64 = 0;
                i = 0;
                while (i < self.n_cols) : (i += 1) skip_n += nbytes[i];
                self.pos += skip_n;
                self.pages_left -= 1;
                continue;
            }

            const keep = try pickCols(allocator, self.cols, wanted);
            const columns = try allocator.alloc(Column, keep.len);
            var out_i: usize = 0;
            i = 0;
            while (i < self.n_cols) : (i += 1) {
                const meta = self.cols[i];
                const take = containsName(keep, meta.name);
                if (take) {
                    columns[out_i] = try decodePayload(allocator, self, meta, nrows, nbytes[i]);
                    out_i += 1;
                } else {
                    self.pos += nbytes[i];
                }
            }
            self.pages_left -= 1;
            return .{ .columns = columns, .len = nrows };
        }
        return null;
    }

    fn readBound(self: *Reader, allocator: std.mem.Allocator, meta: ColMeta, out: *iceberg.ColBound) !bool {
        switch (meta.data_type) {
            .boolean, .int32, .int64, .timestamp, .timestamptz => {
                const lo: i64 = switch (meta.data_type) {
                    .boolean => try self.readU8(),
                    .int32 => try self.readI32(),
                    else => try self.readI64(),
                };
                const hi: i64 = switch (meta.data_type) {
                    .boolean => try self.readU8(),
                    .int32 => try self.readI32(),
                    else => try self.readI64(),
                };
                out.* = .{
                    .column = meta.name,
                    .lower = .{ .int = lo },
                    .upper = .{ .int = hi },
                };
                return true;
            },
            .float32 => {
                const lo: f32 = @bitCast(try self.readU32());
                const hi: f32 = @bitCast(try self.readU32());
                out.* = .{
                    .column = meta.name,
                    .lower = .{ .float = lo },
                    .upper = .{ .float = hi },
                };
                return true;
            },
            .float64 => {
                const lo: f64 = @bitCast(try self.readU64());
                const hi: f64 = @bitCast(try self.readU64());
                out.* = .{
                    .column = meta.name,
                    .lower = .{ .float = lo },
                    .upper = .{ .float = hi },
                };
                return true;
            },
            .utf8 => {
                const nmin = try self.readU32();
                const lo_buf = try allocator.alloc(u8, nmin);
                try self.readExact(lo_buf);
                const nmax = try self.readU32();
                const hi_buf = try allocator.alloc(u8, nmax);
                try self.readExact(hi_buf);
                out.* = .{
                    .column = meta.name,
                    .lower = .{ .string = lo_buf },
                    .upper = .{ .string = hi_buf },
                };
                return true;
            },
            .uuid, .decimal128 => {
                const n: usize = if (meta.data_type == .uuid) 32 else 32;
                var dump: [32]u8 = undefined;
                try self.readExact(dump[0..n]);
                return false;
            },
        }
    }

    fn readExact(self: *Reader, dest: []u8) !void {
        if (dest.len == 0) return;
        const n = try self.file.readPositionalAll(self.io, dest, self.pos);
        if (n != dest.len) return error.UnexpectedEndOfFile;
        self.pos += n;
    }

    fn readU8(self: *Reader) !u8 {
        var b: [1]u8 = undefined;
        try self.readExact(&b);
        return b[0];
    }

    fn readU32(self: *Reader) !u32 {
        var b: [4]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(u32, &b, .little);
    }

    fn readI32(self: *Reader) !i32 {
        var b: [4]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(i32, &b, .little);
    }

    fn readU64(self: *Reader) !u64 {
        var b: [8]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(u64, &b, .little);
    }

    fn readI64(self: *Reader) !i64 {
        var b: [8]u8 = undefined;
        try self.readExact(&b);
        return std.mem.readInt(i64, &b, .little);
    }
};

fn pickCols(allocator: std.mem.Allocator, cols: []const ColMeta, wanted: ?[]const []const u8) ![]const []const u8 {
    if (wanted == null) {
        const names = try allocator.alloc([]const u8, cols.len);
        for (cols, names) |c, *n| n.* = c.name;
        return names;
    }
    const w = wanted.?;
    if (w.len == 0) return w;
    var names: std.ArrayList([]const u8) = .empty;
    for (cols) |c| {
        if (containsName(w, c.name)) try names.append(allocator, c.name);
    }
    return names.items;
}

fn containsName(names: []const []const u8, name: []const u8) bool {
    for (names) |n| {
        if (std.ascii.eqlIgnoreCase(n, name)) return true;
    }
    return false;
}

fn decodePayload(allocator: std.mem.Allocator, r: *Reader, meta: ColMeta, nrows: u32, nbytes: u32) !Column {
    const vlen = try r.readU32();
    if (vlen != 0 and vlen != nrows) return error.InvalidNativeFile;
    var valid: []u8 = &.{};
    if (vlen > 0) {
        valid = try allocator.alloc(u8, vlen);
        try r.readExact(valid);
    }
    const typed_nbytes = nbytes - 4 - vlen;
    var col: Column = .{
        .name = try allocator.dupe(u8, meta.name),
        .data_type = meta.data_type,
        .len = nrows,
        .decimal_precision = meta.decimal_precision,
        .decimal_scale = meta.decimal_scale,
        .valid = valid,
    };
    const n: usize = nrows;
    switch (meta.data_type) {
        .boolean => {
            col.bools = try allocator.alloc(u8, n);
            try r.readExact(col.bools);
        },
        .int32 => {
            col.i32s = try allocator.alloc(i32, n);
            try r.readExact(std.mem.sliceAsBytes(col.i32s));
        },
        .int64, .timestamp, .timestamptz => {
            col.i64s = try allocator.alloc(i64, n);
            try r.readExact(std.mem.sliceAsBytes(col.i64s));
        },
        .float32 => {
            col.f32s = try allocator.alloc(f32, n);
            try r.readExact(std.mem.sliceAsBytes(col.f32s));
        },
        .float64 => {
            col.f64s = try allocator.alloc(f64, n);
            try r.readExact(std.mem.sliceAsBytes(col.f64s));
        },
        .utf8 => {
            col.utf8.offsets = try allocator.alloc(u32, n + 1);
            try r.readExact(std.mem.sliceAsBytes(col.utf8.offsets));
            const blen = typed_nbytes - @as(u32, @intCast((n + 1) * 4));
            col.utf8.bytes = try allocator.alloc(u8, blen);
            try r.readExact(col.utf8.bytes);
        },
        .uuid => {
            col.uuids = try allocator.alloc([16]u8, n);
            try r.readExact(std.mem.sliceAsBytes(col.uuids));
        },
        .decimal128 => {
            col.i128s = try allocator.alloc(i128, n);
            try r.readExact(std.mem.sliceAsBytes(col.i128s));
        },
    }
    return col;
}

fn firstValidRow(col: Column) ?usize {
    var i: usize = 0;
    while (i < col.len) : (i += 1) {
        if (col.isValid(i)) return i;
    }
    return null;
}

pub fn emptySchema(allocator: std.mem.Allocator, cols: []const ColMeta) !Batch {
    const columns = try allocator.alloc(Column, cols.len);
    for (cols, 0..) |m, i| {
        columns[i] = .{
            .name = try allocator.dupe(u8, m.name),
            .data_type = m.data_type,
            .len = 0,
            .decimal_precision = m.decimal_precision,
            .decimal_scale = m.decimal_scale,
        };
        if (m.data_type == .utf8) {
            columns[i].utf8.offsets = try allocator.alloc(u32, 1);
            columns[i].utf8.offsets[0] = 0;
        }
    }
    return .{ .columns = columns, .len = 0 };
}

test "native page roundtrip" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var ids = [_]i64{ 1, 2, 3 };
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
    const path = "/tmp/glacier_native_roundtrip.glacier";
    try writeBatch(a, io, path, batch);

    var r = try Reader.open(a, io, path);
    defer r.close();
    try std.testing.expectEqual(@as(u64, 3), r.numRows());
    const page = (try r.nextPage(a, null, null)) orelse return error.EmptyPage;
    try std.testing.expectEqual(@as(usize, 3), page.len);
    try std.testing.expectEqual(@as(i64, 2), page.columns[0].i64s[1]);
    try std.testing.expectEqualStrings("bbb", page.columns[1].strAt(1));
    try std.testing.expect((try r.nextPage(a, null, null)) == null);
}
