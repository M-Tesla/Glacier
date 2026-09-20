//! Thin Zig wrapper around carquet (C). Does not reimplement Parquet.

const std = @import("std");
const FileSource = @import("../vfs/source.zig").FileSource;
const aws = @import("../kernel/aws.zig");
const cache = @import("../vfs/cache.zig");
const batch_mod = @import("../execution/batch.zig");

pub const LogicalC = extern struct {
    id: i32,
    precision: i32,
    scale: i32,
    time_unit: i32,
    utc: i32,
    type_length: i32,
};

pub const WriteCol = extern struct {
    name: [*:0]const u8,
    physical: i32,
    logical: i32,
    values: ?*const anyopaque = null,
    utf8_offsets: ?[*]const u32 = null,
    utf8_bytes: ?[*]const u8 = null,
    n_rows: i64,
};

pub const ColStats = extern struct {
    has_min_max: i32,
    has_null_count: i32,
    null_count: i64,
    num_values: i64,
    min_len: i32,
    max_len: i32,
    min_bytes: [32]u8,
    max_bytes: [32]u8,
};

const c = struct {
    extern fn glacier_carquet_init() c_int;
    extern fn glacier_carquet_cleanup() void;
    extern fn glacier_carquet_open_buffer(buf: ?*const anyopaque, size: usize, err_buf: [*]u8, err_len: usize) ?*anyopaque;
    extern fn glacier_carquet_open_path(path: ?*const anyopaque, path_len: usize, err_buf: [*]u8, err_len: usize) ?*anyopaque;
    extern fn glacier_carquet_batch_open(
        reader: ?*anyopaque,
        cols: ?[*]const i32,
        n_cols: i32,
        batch_size: i32,
        err_buf: [*]u8,
        err_len: usize,
    ) ?*anyopaque;
    extern fn glacier_carquet_batch_next(br: ?*anyopaque, out_batch: *?*anyopaque) c_int;
    extern fn glacier_carquet_batch_close(br: ?*anyopaque) void;
    extern fn glacier_carquet_row_batch_free(batch: ?*anyopaque) void;
    extern fn glacier_carquet_rb_nrows(batch: ?*anyopaque) i64;
    extern fn glacier_carquet_rb_column(
        batch: ?*anyopaque,
        index: i32,
        data: *?*const anyopaque,
        nulls: *?*const u8,
        n: *i64,
    ) c_int;
    extern fn glacier_carquet_rb_column_list(
        batch: ?*anyopaque,
        index: i32,
        offsets: *?[*]const i32,
        num_lists: *i64,
        values: *?*const anyopaque,
        value_validity: *?[*]const u8,
        num_values: *i64,
        list_validity: *?[*]const u8,
    ) c_int;
    extern fn glacier_carquet_close(reader: ?*anyopaque) void;
    extern fn glacier_carquet_num_rows(reader: ?*anyopaque) i64;
    extern fn glacier_carquet_num_columns(reader: ?*anyopaque) i32;
    extern fn glacier_carquet_num_row_groups(reader: ?*anyopaque) i32;
    extern fn glacier_carquet_column_name(reader: ?*anyopaque, index: i32) ?[*:0]const u8;
    extern fn glacier_carquet_column_type(reader: ?*anyopaque, index: i32) c_int;
    extern fn glacier_carquet_column_logical(reader: ?*anyopaque, index: i32, out: *LogicalC) c_int;
    extern fn glacier_carquet_column_stats(reader: ?*anyopaque, row_group: i32, column: i32, out: *ColStats) c_int;
    extern fn glacier_carquet_read_i64_prefix(reader: ?*anyopaque, col: i32, out: [*]i64, max: i64) i64;
    extern fn glacier_carquet_write_i64_fixture(path: [*:0]const u8, compression: c_int) c_int;
    extern fn glacier_carquet_write_sales_fixture(path: [*:0]const u8) c_int;
    extern fn glacier_carquet_write_sales_rows(
        path: [*:0]const u8,
        ids: [*]const i64,
        prices: [*]const i64,
        cats: [*]const [*:0]const u8,
        n: i32,
    ) c_int;
    extern fn glacier_carquet_write_nulls_fixture(path: [*:0]const u8) c_int;
    extern fn glacier_carquet_read_all(reader: ?*anyopaque, col: i32, out: [*]u8, max: i64) i64;
    extern fn glacier_carquet_free_byte_arrays(out: ?*anyopaque, n: i64) void;
    extern fn glacier_carquet_column_rep_level(reader: ?*anyopaque, index: i32) i16;
    extern fn glacier_carquet_write_types_fixture(path: [*:0]const u8) c_int;
    extern fn glacier_carquet_write_row_groups_fixture(path: [*:0]const u8) c_int;
    extern fn glacier_carquet_write_nested_fixture(path: [*:0]const u8) c_int;
    extern fn glacier_carquet_write_logical_fixture(path: [*:0]const u8) c_int;
    extern fn glacier_carquet_write_pos_deletes(
        path: [*:0]const u8,
        file_paths: [*]const [*:0]const u8,
        positions: [*]const i64,
        n: i32,
    ) c_int;
    extern fn glacier_carquet_write_eq_i64_deletes(
        path: [*:0]const u8,
        col_name: [*:0]const u8,
        vals: [*]const i64,
        n: i32,
    ) c_int;
    extern fn glacier_carquet_write_struct_fixture(path: [*:0]const u8) c_int;
    extern fn glacier_carquet_write_columns(path: [*:0]const u8, cols: [*]const WriteCol, n_cols: i32) c_int;
};

pub const PhysicalType = enum(c_int) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    int96 = 3,
    float = 4,
    double = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,
    _,
};

pub const LogicalId = enum(i32) {
    unknown = 0,
    string = 1,
    decimal = 5,
    date = 6,
    time = 7,
    timestamp = 8,
    uuid = 13,
    _,
};

pub const Column = struct {
    name: []const u8,
    physical_type: PhysicalType,
};

pub fn columnLogical(reader: Reader, index: i32) LogicalC {
    var out: LogicalC = std.mem.zeroes(LogicalC);
    _ = c.glacier_carquet_column_logical(reader.handle, index, &out);
    return out;
}

pub const Reader = struct {
    allocator: std.mem.Allocator,
    buffer: []const u8,
    owns_buffer: bool,
    owned_source: ?FileSource,
    handle: *anyopaque,

    pub fn close(self: *Reader) void {
        c.glacier_carquet_close(self.handle);
        if (self.owns_buffer) self.allocator.free(self.buffer);
        if (self.owned_source) |*s| s.close();
        self.* = undefined;
    }

    pub fn numRows(self: Reader) i64 {
        return c.glacier_carquet_num_rows(self.handle);
    }

    pub fn numColumns(self: Reader) i32 {
        return c.glacier_carquet_num_columns(self.handle);
    }

    pub fn numRowGroups(self: Reader) i32 {
        return c.glacier_carquet_num_row_groups(self.handle);
    }

    pub fn column(self: Reader, index: i32) ?Column {
        const name_z = c.glacier_carquet_column_name(self.handle, index) orelse return null;
        return .{
            .name = std.mem.span(name_z),
            .physical_type = @enumFromInt(c.glacier_carquet_column_type(self.handle, index)),
        };
    }

    pub fn columnRepLevel(self: Reader, index: i32) i16 {
        return c.glacier_carquet_column_rep_level(self.handle, index);
    }

    pub fn columnStats(self: Reader, row_group: i32, col: i32) !ColStats {
        var out: ColStats = std.mem.zeroes(ColStats);
        if (c.glacier_carquet_column_stats(self.handle, row_group, col, &out) != 0)
            return error.ParquetStatsFailed;
        return out;
    }

    pub fn readI64Prefix(self: Reader, col: i32, out: []i64) i64 {
        if (out.len == 0) return 0;
        return c.glacier_carquet_read_i64_prefix(self.handle, col, out.ptr, @intCast(out.len));
    }

    pub fn readAllValues(self: Reader, col: i32, out: anytype) i64 {
        const bytes = std.mem.sliceAsBytes(out);
        return c.glacier_carquet_read_all(self.handle, col, bytes.ptr, @intCast(out.len));
    }

    pub fn openBatch(self: Reader, columns: []const i32, batch_size: i32) !BatchReader {
        var err_buf: [256]u8 = undefined;
        @memset(&err_buf, 0);
        const handle = c.glacier_carquet_batch_open(
            self.handle,
            if (columns.len == 0) null else columns.ptr,
            @intCast(columns.len),
            batch_size,
            &err_buf,
            err_buf.len,
        ) orelse {
            setDetail(std.mem.sliceTo(&err_buf, 0));
            return error.ParquetOpenFailed;
        };
        return .{ .handle = handle };
    }
};

pub const RowBatch = struct {
    ptr: *anyopaque,

    pub fn deinit(self: *RowBatch) void {
        c.glacier_carquet_row_batch_free(self.ptr);
        self.* = undefined;
    }

    pub fn numRows(self: RowBatch) i64 {
        return c.glacier_carquet_rb_nrows(self.ptr);
    }

    pub fn columnValues(self: RowBatch, index: i32) !struct { ptr: [*]const u8, n: i64, nulls: ?[*]const u8 } {
        var data: ?*const anyopaque = null;
        var nulls: ?*const u8 = null;
        var n: i64 = 0;
        if (c.glacier_carquet_rb_column(self.ptr, index, &data, &nulls, &n) != 0)
            return error.ParquetOpenFailed;
        if (n <= 0) return .{ .ptr = undefined, .n = 0, .nulls = null };
        const p = data orelse return error.ParquetOpenFailed;
        return .{ .ptr = @ptrCast(p), .n = n, .nulls = if (nulls) |nb| @ptrCast(nb) else null };
    }

    pub const ListCol = struct {
        offsets: []const i32,
        values: [*]const u8,
        n_lists: i64,
        n_values: i64,
        value_validity: ?[*]const u8,
        list_validity: ?[*]const u8,
    };

    pub fn columnList(self: RowBatch, index: i32) !ListCol {
        var offsets: ?[*]const i32 = null;
        var n_lists: i64 = 0;
        var values: ?*const anyopaque = null;
        var value_validity: ?[*]const u8 = null;
        var n_values: i64 = 0;
        var list_validity: ?[*]const u8 = null;
        if (c.glacier_carquet_rb_column_list(
            self.ptr,
            index,
            &offsets,
            &n_lists,
            &values,
            &value_validity,
            &n_values,
            &list_validity,
        ) != 0) return error.ParquetOpenFailed;
        const off = offsets orelse return error.ParquetOpenFailed;
        const val = values orelse return error.UnsupportedNested;
        const off_len: usize = @intCast(if (n_lists < 0) 0 else n_lists + 1);
        return .{
            .offsets = off[0..off_len],
            .values = @ptrCast(val),
            .n_lists = n_lists,
            .n_values = n_values,
            .value_validity = value_validity,
            .list_validity = list_validity,
        };
    }
};

pub const BatchReader = struct {
    handle: *anyopaque,

    pub fn close(self: *BatchReader) void {
        c.glacier_carquet_batch_close(self.handle);
        self.* = undefined;
    }

    pub fn next(self: *BatchReader) !?RowBatch {
        var out: ?*anyopaque = null;
        const rc = c.glacier_carquet_batch_next(self.handle, &out);
        if (rc < 0) return error.ParquetOpenFailed;
        if (rc == 0 or out == null) return null;
        return .{ .ptr = out.? };
    }
};

var initialized: bool = false;
var detail_buf: [256]u8 = undefined;
var detail_len: usize = 0;

pub fn lastDetail() []const u8 {
    return detail_buf[0..detail_len];
}

fn setDetail(s: []const u8) void {
    const n = @min(s.len, detail_buf.len);
    if (n > 0) @memcpy(detail_buf[0..n], s[0..n]);
    detail_len = n;
}

fn ensureInit() !void {
    if (initialized) return;
    if (c.glacier_carquet_init() != 0) return error.CarquetInitFailed;
    initialized = true;
}

pub fn openSource(allocator: std.mem.Allocator, source: FileSource) !Reader {
    try ensureInit();
    if (source.view()) |slice| {
        const owned: ?FileSource = if (std.meta.activeTag(source.backend) == .mmap) source else null;
        return openBuffer(allocator, slice, false, owned);
    }
    const buffer = try source.readAll(allocator);
    errdefer allocator.free(buffer);
    return openOwnedBuffer(allocator, buffer);
}

pub const Compression = enum(c_int) {
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lz4 = 5,
    zstd = 6,
};

pub fn writeI64Fixture(path: [:0]const u8, compression: Compression) !void {
    try ensureInit();
    if (c.glacier_carquet_write_i64_fixture(path.ptr, @intFromEnum(compression)) != 0)
        return error.ParquetWriteFailed;
}

pub fn writeSalesFixture(path: [:0]const u8) !void {
    try ensureInit();
    if (c.glacier_carquet_write_sales_fixture(path.ptr) != 0)
        return error.ParquetWriteFailed;
}

pub fn writeSalesRows(path: [:0]const u8, ids: []const i64, prices: []const i64, cats: []const [*:0]const u8) !void {
    try ensureInit();
    if (ids.len != prices.len or ids.len != cats.len or ids.len == 0) return error.ParquetWriteFailed;
    if (c.glacier_carquet_write_sales_rows(path.ptr, ids.ptr, prices.ptr, cats.ptr, @intCast(ids.len)) != 0)
        return error.ParquetWriteFailed;
}

pub fn writeNullsFixture(path: [:0]const u8) !void {
    try ensureInit();
    if (c.glacier_carquet_write_nulls_fixture(path.ptr) != 0)
        return error.ParquetWriteFailed;
}

pub fn writeTypesFixture(path: [:0]const u8) !void {
    try ensureInit();
    if (c.glacier_carquet_write_types_fixture(path.ptr) != 0) return error.ParquetWriteFailed;
}

pub fn writeRowGroupsFixture(path: [:0]const u8) !void {
    try ensureInit();
    if (c.glacier_carquet_write_row_groups_fixture(path.ptr) != 0) return error.ParquetWriteFailed;
}

pub fn writeNestedFixture(path: [:0]const u8) !void {
    try ensureInit();
    if (c.glacier_carquet_write_nested_fixture(path.ptr) != 0) return error.ParquetWriteFailed;
}

pub fn writeLogicalFixture(path: [:0]const u8) !void {
    try ensureInit();
    if (c.glacier_carquet_write_logical_fixture(path.ptr) != 0) return error.ParquetWriteFailed;
}

pub fn writePosDeletes(
    path: [:0]const u8,
    file_paths: []const [*:0]const u8,
    positions: []const i64,
) !void {
    try ensureInit();
    if (file_paths.len == 0 or file_paths.len != positions.len) return error.ParquetWriteFailed;
    if (c.glacier_carquet_write_pos_deletes(path.ptr, file_paths.ptr, positions.ptr, @intCast(file_paths.len)) != 0)
        return error.ParquetWriteFailed;
}

pub fn writeEqI64Deletes(path: [:0]const u8, col_name: [:0]const u8, vals: []const i64) !void {
    try ensureInit();
    if (vals.len == 0) return error.ParquetWriteFailed;
    if (c.glacier_carquet_write_eq_i64_deletes(path.ptr, col_name.ptr, vals.ptr, @intCast(vals.len)) != 0)
        return error.ParquetWriteFailed;
}

pub fn writeStructFixture(path: [:0]const u8) !void {
    try ensureInit();
    if (c.glacier_carquet_write_struct_fixture(path.ptr) != 0) return error.ParquetWriteFailed;
}

pub fn writeColumns(path: [:0]const u8, cols: []const WriteCol) !void {
    try ensureInit();
    if (cols.len == 0) return error.ParquetWriteFailed;
    if (c.glacier_carquet_write_columns(path.ptr, cols.ptr, @intCast(cols.len)) != 0)
        return error.ParquetWriteFailed;
}

pub fn writeBatch(allocator: std.mem.Allocator, path: [:0]const u8, input: batch_mod.Batch) !void {
    if (input.len == 0 or input.columns.len == 0) return error.ParquetWriteFailed;
    const cols = try allocator.alloc(WriteCol, input.columns.len);
    for (input.columns, 0..) |col, i| {
        const name = try allocator.dupeZ(u8, col.name);
        var physical: i32 = undefined;
        var logical: i32 = 0;
        var values: ?*const anyopaque = null;
        var utf8_offsets: ?[*]const u32 = null;
        var utf8_bytes: ?[*]const u8 = null;
        switch (col.data_type) {
            .boolean => {
                physical = 0;
                values = if (col.bools.len > 0) col.bools.ptr else null;
            },
            .int32 => {
                physical = 1;
                values = if (col.i32s.len > 0) col.i32s.ptr else null;
            },
            .int64, .timestamp, .timestamptz => {
                physical = 2;
                values = if (col.i64s.len > 0) col.i64s.ptr else null;
            },
            .float32 => {
                physical = 4;
                values = if (col.f32s.len > 0) col.f32s.ptr else null;
            },
            .float64 => {
                physical = 5;
                values = if (col.f64s.len > 0) col.f64s.ptr else null;
            },
            .utf8 => {
                physical = 6;
                logical = 1;
                utf8_offsets = col.utf8.offsets.ptr;
                utf8_bytes = if (col.utf8.bytes.len > 0) col.utf8.bytes.ptr else null;
            },
            else => return error.UnsupportedType,
        }
        cols[i] = .{
            .name = name.ptr,
            .physical = physical,
            .logical = logical,
            .values = values,
            .utf8_offsets = utf8_offsets,
            .utf8_bytes = utf8_bytes,
            .n_rows = @intCast(input.len),
        };
    }
    try writeColumns(path, cols);
}

pub const ByteArray = extern struct {
    data: [*]u8,
    length: i32,
};

pub fn freeByteArrays(out: []ByteArray) void {
    if (out.len == 0) return;
    c.glacier_carquet_free_byte_arrays(out.ptr, @intCast(out.len));
}

pub fn openMemory(allocator: std.mem.Allocator, buffer: []const u8) !Reader {
    try ensureInit();
    return openBuffer(allocator, buffer, false, null);
}

pub fn openLocalPath(allocator: std.mem.Allocator, path: []const u8) !Reader {
    try ensureInit();
    const zpath = try allocator.dupeZ(u8, path);
    defer allocator.free(zpath);
    var err_buf: [256]u8 = undefined;
    @memset(&err_buf, 0);
    const handle = c.glacier_carquet_open_path(zpath.ptr, zpath.len, &err_buf, err_buf.len) orelse {
        setDetail(std.mem.sliceTo(&err_buf, 0));
        return error.ParquetOpenFailed;
    };
    return .{
        .allocator = allocator,
        .buffer = &.{},
        .owns_buffer = false,
        .owned_source = null,
        .handle = handle,
    };
}

pub fn openPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Reader {
    _ = io;
    return openLocalPath(allocator, path);
}

pub fn openLocation(allocator: std.mem.Allocator, t: @import("../vfs/source.zig").Transport, path: []const u8) !Reader {
    if (aws.isRemote(path)) {
        const cached = try cache.materialize(t, path);
        defer t.allocator.free(cached);
        return openLocalPath(allocator, cached);
    }
    return openLocalPath(allocator, path);
}

fn openOwnedBuffer(allocator: std.mem.Allocator, buffer: []u8) !Reader {
    return openBuffer(allocator, buffer, true, null);
}

fn openBuffer(allocator: std.mem.Allocator, buffer: []const u8, owns_buffer: bool, owned_source: ?FileSource) !Reader {
    var err_buf: [256]u8 = undefined;
    @memset(&err_buf, 0);
    const handle = c.glacier_carquet_open_buffer(buffer.ptr, buffer.len, &err_buf, err_buf.len) orelse {
        var src = owned_source;
        if (src) |*s| s.close();
        if (owns_buffer) allocator.free(@constCast(buffer));
        setDetail(std.mem.sliceTo(&err_buf, 0));
        return error.ParquetOpenFailed;
    };
    return .{
        .allocator = allocator,
        .buffer = buffer,
        .owns_buffer = owns_buffer,
        .owned_source = owned_source,
        .handle = handle,
    };
}

test "write and read uncompressed i64 parquet" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeI64Fixture("/tmp/glacier-unit.parquet", .uncompressed);
    var reader = try openPath(gpa, io, "/tmp/glacier-unit.parquet");
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 5), reader.numRows());
    try std.testing.expectEqual(@as(i32, 1), reader.numColumns());
    var sample: [5]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 5), reader.readI64Prefix(0, &sample));
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, &sample);
    try std.testing.expect(!reader.owns_buffer);
    try std.testing.expectEqual(@as(usize, 0), reader.buffer.len);
}

test "write and read snappy i64 parquet" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeI64Fixture("/tmp/glacier-unit-snappy.parquet", .snappy);
    var reader = try openPath(gpa, io, "/tmp/glacier-unit-snappy.parquet");
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 5), reader.numRows());
    var sample: [5]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 5), reader.readI64Prefix(0, &sample));
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, &sample);
}

fn expectFooterMinMax(
    gpa: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    min_id: i64,
    max_id: i64,
    min_cat: []const u8,
    max_cat: []const u8,
) !void {
    var reader = try openPath(gpa, io, path);
    defer reader.close();
    try std.testing.expectEqual(@as(i32, 1), reader.numRowGroups());
    const id = try reader.columnStats(0, 0);
    try std.testing.expectEqual(@as(i32, 1), id.has_min_max);
    try std.testing.expectEqual(@as(i32, 1), id.has_null_count);
    try std.testing.expectEqual(@as(i64, 0), id.null_count);
    try std.testing.expectEqual(@as(i64, 2), id.num_values);
    try std.testing.expectEqual(@as(i32, 8), id.min_len);
    try std.testing.expectEqual(@as(i32, 8), id.max_len);
    try std.testing.expectEqual(min_id, std.mem.readInt(i64, id.min_bytes[0..8], .little));
    try std.testing.expectEqual(max_id, std.mem.readInt(i64, id.max_bytes[0..8], .little));
    const cat = try reader.columnStats(0, 1);
    try std.testing.expectEqual(@as(i32, 1), cat.has_min_max);
    try std.testing.expectEqual(@as(i64, 2), cat.num_values);
    try std.testing.expectEqualSlices(u8, min_cat, cat.min_bytes[0..@intCast(cat.min_len)]);
    try std.testing.expectEqualSlices(u8, max_cat, cat.max_bytes[0..@intCast(cat.max_len)]);
}

test "written parquet footer has column min max" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const ids = [_]i64{ 1, 2 };
    const cats = "fruitveg";
    const offs = [_]u32{ 0, 5, 8 };
    const cols = [_]WriteCol{
        .{ .name = "id", .physical = 2, .logical = 0, .values = &ids, .n_rows = 2 },
        .{
            .name = "category",
            .physical = 6,
            .logical = 1,
            .utf8_offsets = &offs,
            .utf8_bytes = cats,
            .n_rows = 2,
        },
    };
    try writeColumns("/tmp/glacier-footer-stats.parquet", &cols);
    try expectFooterMinMax(gpa, io, "/tmp/glacier-footer-stats.parquet", 1, 2, "fruit", "veg");
}

test "open parquet from FileSource memory" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeI64Fixture("/tmp/glacier-mem.parquet", .uncompressed);
    var file_src = try FileSource.openPath(io, "/tmp/glacier-mem.parquet");
    const bytes = try file_src.readAll(gpa);
    defer gpa.free(bytes);
    file_src.close();

    var mem = FileSource.fromMemory(bytes);
    defer mem.close();
    var reader = try openSource(gpa, mem);
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 5), reader.numRows());
    var sample: [5]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 5), reader.readI64Prefix(0, &sample));
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, &sample);
}

test "gzip lz4 zstd i64 parquet round-trip" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const cases = [_]struct { path: [:0]const u8, codec: Compression }{
        .{ .path = "/tmp/glacier-gzip.parquet", .codec = .gzip },
        .{ .path = "/tmp/glacier-lz4.parquet", .codec = .lz4 },
        .{ .path = "/tmp/glacier-zstd.parquet", .codec = .zstd },
    };
    for (cases) |cse| {
        try writeI64Fixture(cse.path, cse.codec);
        var reader = try openPath(gpa, io, cse.path);
        defer reader.close();
        try std.testing.expectEqual(@as(i64, 5), reader.numRows());
        var sample: [5]i64 = undefined;
        try std.testing.expectEqual(@as(i64, 5), reader.readI64Prefix(0, &sample));
        try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, &sample);
    }
}

test "read all row groups" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeRowGroupsFixture("/tmp/glacier-rgs.parquet");
    var reader = try openPath(gpa, io, "/tmp/glacier-rgs.parquet");
    defer reader.close();
    try std.testing.expectEqual(@as(i32, 2), reader.numRowGroups());
    try std.testing.expectEqual(@as(i64, 6), reader.numRows());
    var ids: [6]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 6), reader.readAllValues(0, ids[0..]));
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5, 6 }, &ids);
}

test "types fixture physical columns" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeTypesFixture("/tmp/glacier-types-wrap.parquet");
    var reader = try openPath(gpa, io, "/tmp/glacier-types-wrap.parquet");
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 3), reader.numRows());
    try std.testing.expectEqual(@as(i32, 6), reader.numColumns());
    try std.testing.expectEqual(PhysicalType.boolean, reader.column(0).?.physical_type);
    try std.testing.expectEqual(PhysicalType.int32, reader.column(1).?.physical_type);
    try std.testing.expectEqual(PhysicalType.int64, reader.column(2).?.physical_type);
    try std.testing.expectEqual(PhysicalType.float, reader.column(3).?.physical_type);
    try std.testing.expectEqual(PhysicalType.double, reader.column(4).?.physical_type);
    try std.testing.expectEqual(PhysicalType.byte_array, reader.column(5).?.physical_type);
    var flags: [3]u8 = undefined;
    try std.testing.expectEqual(@as(i64, 3), reader.readAllValues(0, flags[0..]));
    try std.testing.expectEqualSlices(u8, &.{ 1, 0, 1 }, &flags);
    var f32s: [3]f32 = undefined;
    try std.testing.expectEqual(@as(i64, 3), reader.readAllValues(3, f32s[0..]));
    try std.testing.expectEqual(@as(f32, 1.5), f32s[0]);
    try std.testing.expectEqual(@as(f32, 3.5), f32s[2]);
}

test "byte array strings survive column reader close" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeSalesFixture("/tmp/glacier-ba-lifetime.parquet");
    var reader = try openPath(gpa, io, "/tmp/glacier-ba-lifetime.parquet");
    defer reader.close();
    var bas: [10]ByteArray = std.mem.zeroes([10]ByteArray);
    try std.testing.expectEqual(@as(i64, 10), reader.readAllValues(2, bas[0..]));
    defer freeByteArrays(bas[0..]);
    try std.testing.expectEqualStrings("fruit", bas[0].data[0..@intCast(bas[0].length)]);
    try std.testing.expectEqualStrings("dairy", bas[8].data[0..@intCast(bas[8].length)]);
}

test "nested parquet has repetition level" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeNestedFixture("/tmp/glacier-nested-wrap.parquet");
    var reader = try openPath(gpa, io, "/tmp/glacier-nested-wrap.parquet");
    defer reader.close();
    var i: i32 = 0;
    var nested = false;
    while (i < reader.numColumns()) : (i += 1) {
        if (reader.columnRepLevel(i) > 0) nested = true;
    }
    try std.testing.expect(nested);
}

test "open parquet from FileSource mmap" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeI64Fixture("/tmp/glacier-mmap.parquet", .uncompressed);
    var src = try FileSource.openMapped(io, "/tmp/glacier-mmap.parquet");
    try std.testing.expect(src.view() != null);
    var reader = try openSource(gpa, src);
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 5), reader.numRows());
    var sample: [5]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 5), reader.readI64Prefix(0, &sample));
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, &sample);
}

test "open parquet with disable_memory_mapping" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{ .disable_memory_mapping = true });
    defer threaded.deinit();
    const io = threaded.io();

    try writeI64Fixture("/tmp/glacier-mmap-off.parquet", .uncompressed);
    var reader = try openPath(gpa, io, "/tmp/glacier-mmap-off.parquet");
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 5), reader.numRows());
    var sample: [5]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 5), reader.readI64Prefix(0, &sample));
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5 }, &sample);
}

test "batch reader streams row groups in chunks" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    try writeRowGroupsFixture("/tmp/glacier-batch-rgs.parquet");
    var reader = try openPath(gpa, io, "/tmp/glacier-batch-rgs.parquet");
    defer reader.close();
    try std.testing.expect(!reader.owns_buffer);

    var br = try reader.openBatch(&.{0}, 2);
    defer br.close();
    var total: i64 = 0;
    var n_batches: usize = 0;
    while (try br.next()) |rb_val| {
        var rb = rb_val;
        defer rb.deinit();
        const n = rb.numRows();
        try std.testing.expect(n > 0 and n <= 2);
        const col = try rb.columnValues(0);
        try std.testing.expectEqual(n, col.n);
        total += n;
        n_batches += 1;
    }
    try std.testing.expectEqual(@as(i64, 6), total);
    try std.testing.expect(n_batches >= 3);
}
