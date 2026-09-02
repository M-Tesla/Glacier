//! Thin Zig wrapper around libavro (C). Does not reimplement Avro.

const std = @import("std");
const FileSource = @import("../vfs/source.zig").FileSource;

const c = struct {
    const Bound = extern struct {
        field_id: i32,
        bytes: [*]u8,
        len: usize,
    };

    const DataFile = extern struct {
        path: ?[*:0]u8,
        format: ?[*:0]u8,
        status: i32,
        content: i32,
        record_count: i64,
        lower: ?[*]Bound,
        n_lower: usize,
        upper: ?[*]Bound,
        n_upper: usize,
    };

    extern fn glacier_avro_open_buffer(buf: ?*const anyopaque, size: usize, err_buf: [*]u8, err_len: usize) ?*anyopaque;
    extern fn glacier_avro_close(reader: ?*anyopaque) void;
    extern fn glacier_avro_num_rows(reader: ?*anyopaque) i64;
    extern fn glacier_avro_num_columns(reader: ?*anyopaque) i32;
    extern fn glacier_avro_column_name(reader: ?*anyopaque, index: i32) ?[*:0]const u8;
    extern fn glacier_avro_column_type(reader: ?*anyopaque, index: i32) c_int;
    extern fn glacier_avro_read_all(reader: ?*anyopaque, col: i32, out: [*]u8, max: i64) i64;
    extern fn glacier_avro_collect_field_strings(
        buf: ?*const anyopaque,
        size: usize,
        field_name: [*:0]const u8,
        out: *?[*][*:0]u8,
        out_n: *usize,
        err_buf: [*]u8,
        err_len: usize,
    ) c_int;
    extern fn glacier_avro_read_iceberg_entries(
        buf: ?*const anyopaque,
        size: usize,
        out: *?[*]DataFile,
        out_n: *usize,
        err_buf: [*]u8,
        err_len: usize,
    ) c_int;
    extern fn glacier_avro_free_iceberg_entries(files: ?[*]DataFile, n: usize) void;
    extern fn glacier_avro_free_strings(s: ?[*][*:0]u8, n: usize) void;
    extern fn glacier_avro_write_iceberg_manifest_list(path: [*:0]const u8, manifest_path: [*:0]const u8) c_int;
    extern fn glacier_avro_write_iceberg_manifest(
        path: [*:0]const u8,
        file_paths: [*]const [*:0]const u8,
        counts: [*]const i64,
        lower: [*]const i64,
        upper: [*]const i64,
        n: c_int,
        bound_field_id: i32,
    ) c_int;
    extern fn glacier_avro_write_sales_rows(
        path: [*:0]const u8,
        ids: [*]const i64,
        prices: [*]const i64,
        cats: [*]const [*:0]const u8,
        n: c_int,
        codec: [*:0]const u8,
    ) c_int;
};

pub const PhysicalType = enum(c_int) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    double = 5,
    byte_array = 6,
    _,
};

pub const Column = struct {
    name: []const u8,
    physical_type: PhysicalType,
};

pub const ByteArray = extern struct {
    data: [*]u8,
    length: i32,
};

pub const BoundBytes = struct {
    field_id: i32,
    bytes: []u8,
};

pub const ManifestEntry = struct {
    path: []u8,
    format: []u8,
    status: i32,
    content: i32,
    record_count: i64,
    lower: []BoundBytes,
    upper: []BoundBytes,
};

pub const Reader = struct {
    allocator: std.mem.Allocator,
    buffer: []const u8,
    owns_buffer: bool,
    owned_source: ?FileSource,
    handle: *anyopaque,

    pub fn close(self: *Reader) void {
        c.glacier_avro_close(self.handle);
        if (self.owns_buffer) self.allocator.free(self.buffer);
        if (self.owned_source) |*s| s.close();
        self.* = undefined;
    }

    pub fn numRows(self: Reader) i64 {
        return c.glacier_avro_num_rows(self.handle);
    }

    pub fn numColumns(self: Reader) i32 {
        return c.glacier_avro_num_columns(self.handle);
    }

    pub fn column(self: Reader, index: i32) ?Column {
        const name_z = c.glacier_avro_column_name(self.handle, index) orelse return null;
        return .{
            .name = std.mem.span(name_z),
            .physical_type = @enumFromInt(c.glacier_avro_column_type(self.handle, index)),
        };
    }

    pub fn readAllValues(self: Reader, col: i32, out: anytype) i64 {
        const bytes = std.mem.sliceAsBytes(out);
        return c.glacier_avro_read_all(self.handle, col, bytes.ptr, @intCast(out.len));
    }
};

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

pub fn openSource(allocator: std.mem.Allocator, source: FileSource) !Reader {
    if (source.view()) |slice| {
        const owned: ?FileSource = if (std.meta.activeTag(source.backend) == .mmap) source else null;
        return openBuffer(allocator, slice, false, owned);
    }
    const buffer = try source.readAll(allocator);
    errdefer allocator.free(buffer);
    return openOwnedBuffer(allocator, buffer);
}

pub fn openMemory(allocator: std.mem.Allocator, buffer: []const u8) !Reader {
    return openBuffer(allocator, buffer, false, null);
}

pub fn openPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Reader {
    var source = try FileSource.openMapped(io, path);
    if (source.view() != null) return openSource(allocator, source);
    defer source.close();
    return openSource(allocator, source);
}

pub fn openLocation(allocator: std.mem.Allocator, t: @import("../vfs/source.zig").Transport, path: []const u8) !Reader {
    var source = try FileSource.openLocation(t, path);
    if (source.view() != null) return openSource(allocator, source);
    defer source.close();
    return openSource(allocator, source);
}

fn openOwnedBuffer(allocator: std.mem.Allocator, buffer: []u8) !Reader {
    return openBuffer(allocator, buffer, true, null);
}

fn openBuffer(allocator: std.mem.Allocator, buffer: []const u8, owns_buffer: bool, owned_source: ?FileSource) !Reader {
    var err_buf: [256]u8 = undefined;
    @memset(&err_buf, 0);
    const handle = c.glacier_avro_open_buffer(buffer.ptr, buffer.len, &err_buf, err_buf.len) orelse {
        var src = owned_source;
        if (src) |*s| s.close();
        if (owns_buffer) allocator.free(@constCast(buffer));
        setDetail(std.mem.sliceTo(&err_buf, 0));
        return error.AvroOpenFailed;
    };
    return .{
        .allocator = allocator,
        .buffer = buffer,
        .owns_buffer = owns_buffer,
        .owned_source = owned_source,
        .handle = handle,
    };
}

pub fn collectStringField(allocator: std.mem.Allocator, buf: []const u8, field_name: [:0]const u8) ![][]u8 {
    var err_buf: [256]u8 = undefined;
    @memset(&err_buf, 0);
    var ptr: ?[*][*:0]u8 = null;
    var n: usize = 0;
    if (c.glacier_avro_collect_field_strings(buf.ptr, buf.len, field_name.ptr, &ptr, &n, &err_buf, err_buf.len) != 0) {
        setDetail(std.mem.sliceTo(&err_buf, 0));
        return error.AvroOpenFailed;
    }
    defer c.glacier_avro_free_strings(ptr, n);
    const src = (ptr orelse return error.AvroOpenFailed)[0..n];
    const out = try allocator.alloc([]u8, n);
    for (src, 0..) |z, i| {
        out[i] = try allocator.dupe(u8, std.mem.span(z));
    }
    return out;
}

pub fn readIcebergEntries(allocator: std.mem.Allocator, buf: []const u8) ![]ManifestEntry {
    var err_buf: [256]u8 = undefined;
    @memset(&err_buf, 0);
    var ptr: ?[*]c.DataFile = null;
    var n: usize = 0;
    if (c.glacier_avro_read_iceberg_entries(buf.ptr, buf.len, &ptr, &n, &err_buf, err_buf.len) != 0) {
        setDetail(std.mem.sliceTo(&err_buf, 0));
        return error.AvroOpenFailed;
    }
    defer c.glacier_avro_free_iceberg_entries(ptr, n);
    const src = (ptr orelse return error.AvroOpenFailed)[0..n];
    const out = try allocator.alloc(ManifestEntry, n);
    for (src, 0..) |e, i| {
        out[i] = .{
            .path = try allocator.dupe(u8, if (e.path) |p| std.mem.span(p) else ""),
            .format = try allocator.dupe(u8, if (e.format) |p| std.mem.span(p) else "PARQUET"),
            .status = e.status,
            .content = e.content,
            .record_count = e.record_count,
            .lower = try copyBounds(allocator, e.lower, e.n_lower),
            .upper = try copyBounds(allocator, e.upper, e.n_upper),
        };
    }
    return out;
}

fn copyBounds(allocator: std.mem.Allocator, ptr: ?[*]c.Bound, n: usize) ![]BoundBytes {
    if (n == 0 or ptr == null) return &.{};
    const src = ptr.?[0..n];
    const out = try allocator.alloc(BoundBytes, n);
    for (src, 0..) |b, i| {
        out[i] = .{
            .field_id = b.field_id,
            .bytes = try allocator.dupe(u8, b.bytes[0..b.len]),
        };
    }
    return out;
}

pub fn writeManifestList(path: [:0]const u8, manifest_path: [:0]const u8) !void {
    if (c.glacier_avro_write_iceberg_manifest_list(path.ptr, manifest_path.ptr) != 0)
        return error.AvroWriteFailed;
}

pub fn writeDataManifest(
    path: [:0]const u8,
    file_paths: []const [*:0]const u8,
    counts: []const i64,
    lower: []const i64,
    upper: []const i64,
    bound_field_id: i32,
) !void {
    if (file_paths.len == 0 or file_paths.len != counts.len or file_paths.len != lower.len or file_paths.len != upper.len)
        return error.AvroWriteFailed;
    if (c.glacier_avro_write_iceberg_manifest(
        path.ptr,
        file_paths.ptr,
        counts.ptr,
        lower.ptr,
        upper.ptr,
        @intCast(file_paths.len),
        bound_field_id,
    ) != 0) return error.AvroWriteFailed;
}

pub const Codec = enum {
    uncompressed,
    deflate,
    snappy,

    fn name(self: Codec) [*:0]const u8 {
        return switch (self) {
            .uncompressed => "null",
            .deflate => "deflate",
            .snappy => "snappy",
        };
    }
};

pub fn writeSalesRows(path: [:0]const u8, ids: []const i64, prices: []const i64, cats: []const [*:0]const u8) !void {
    try writeSalesRowsCodec(path, ids, prices, cats, .uncompressed);
}

pub fn writeSalesRowsCodec(
    path: [:0]const u8,
    ids: []const i64,
    prices: []const i64,
    cats: []const [*:0]const u8,
    codec: Codec,
) !void {
    if (ids.len != prices.len or ids.len != cats.len or ids.len == 0) return error.AvroWriteFailed;
    if (c.glacier_avro_write_sales_rows(path.ptr, ids.ptr, prices.ptr, cats.ptr, @intCast(ids.len), codec.name()) != 0)
        return error.AvroWriteFailed;
}

test "write and read sales avro" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const ids = [_]i64{ 1, 2, 3 };
    const prices = [_]i64{ 50, 80, 100 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg" };
    try writeSalesRows("/tmp/glacier-unit.avro", &ids, &prices, &cats);

    var reader = try openPath(gpa, io, "/tmp/glacier-unit.avro");
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 3), reader.numRows());
    try std.testing.expectEqual(@as(i32, 3), reader.numColumns());
    var sample: [3]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 3), reader.readAllValues(1, sample[0..]));
    try std.testing.expectEqualSlices(i64, &.{ 50, 80, 100 }, &sample);
}

test "open avro from FileSource memory" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();

    const ids = [_]i64{ 1, 2, 3 };
    const prices = [_]i64{ 50, 80, 100 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg" };
    try writeSalesRows("/tmp/glacier-mem.avro", &ids, &prices, &cats);

    var file_src = try FileSource.openPath(threaded.io(), "/tmp/glacier-mem.avro");
    const bytes = try file_src.readAll(gpa);
    defer gpa.free(bytes);
    file_src.close();

    var mem = FileSource.fromMemory(bytes);
    defer mem.close();
    var reader = try openSource(gpa, mem);
    defer reader.close();
    try std.testing.expectEqual(@as(i64, 3), reader.numRows());
    var sample: [3]i64 = undefined;
    try std.testing.expectEqual(@as(i64, 3), reader.readAllValues(1, sample[0..]));
    try std.testing.expectEqualSlices(i64, &.{ 50, 80, 100 }, &sample);
}

test "deflate and snappy avro round-trip" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const prices = [_]i64{ 50, 80, 100, 120, 150, 200, 90, 110, 300, 75 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg", "veg", "fruit", "dairy", "fruit", "veg", "dairy", "fruit" };
    const cases = [_]struct { path: [:0]const u8, codec: Codec, tag: []const u8 }{
        .{ .path = "/tmp/glacier-deflate.avro", .codec = .deflate, .tag = "deflate" },
        .{ .path = "/tmp/glacier-snappy.avro", .codec = .snappy, .tag = "snappy" },
    };
    for (cases) |cse| {
        try writeSalesRowsCodec(cse.path, &ids, &prices, &cats, cse.codec);
        var file_src = try FileSource.openPath(io, cse.path);
        const bytes = try file_src.readAll(gpa);
        defer gpa.free(bytes);
        file_src.close();
        try std.testing.expect(std.mem.indexOf(u8, bytes, cse.tag) != null);

        var reader = try openPath(gpa, io, cse.path);
        defer reader.close();
        try std.testing.expectEqual(@as(i64, 10), reader.numRows());
        var sample: [10]i64 = undefined;
        try std.testing.expectEqual(@as(i64, 10), reader.readAllValues(1, sample[0..]));
        try std.testing.expectEqualSlices(i64, &prices, &sample);
    }
}
