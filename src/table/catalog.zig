//! Named catalog in a Session. Iceberg is a table format, not the catalog itself.

const std = @import("std");
const iceberg = @import("iceberg.zig");
const rest_catalog = @import("rest_catalog.zig");
const batch = @import("../execution/batch.zig");

pub const Kind = enum {
    files,
    iceberg_rest,
    iceberg_hadoop,
    glacier,

    pub fn label(self: Kind) []const u8 {
        return switch (self) {
            .files => "files",
            .iceberg_rest => "iceberg_rest",
            .iceberg_hadoop => "iceberg_hadoop",
            .glacier => "glacier",
        };
    }
};

pub const Entry = struct {
    name: []const u8,
    kind: Kind,
    table_name: []const u8 = "",
    files: []const iceberg.DataFile = &.{},
    rest: ?*rest_catalog.Client = null,
    schema_fields: []const iceberg.SchemaField = &.{},
    warehouse: []const u8 = "",
};

pub fn utf8Col(allocator: std.mem.Allocator, name: []const u8, values: []const []const u8) !batch.Column {
    const n = values.len;
    const offs = try allocator.alloc(u32, n + 1);
    offs[0] = 0;
    var total: usize = 0;
    for (values, 0..) |v, i| {
        total += v.len;
        offs[i + 1] = @intCast(total);
    }
    const bytes = try allocator.alloc(u8, total);
    var pos: usize = 0;
    for (values) |v| {
        @memcpy(bytes[pos..][0..v.len], v);
        pos += v.len;
    }
    return .{
        .name = try allocator.dupe(u8, name),
        .data_type = .utf8,
        .len = n,
        .utf8 = .{ .offsets = offs, .bytes = bytes },
    };
}

pub fn utf8Cols(
    allocator: std.mem.Allocator,
    names: []const []const u8,
    values: []const []const []const u8,
) !batch.Batch {
    if (names.len == 0 or names.len != values.len) return error.InvalidSyntax;
    const n = values[0].len;
    for (values[1..]) |col| {
        if (col.len != n) return error.InvalidSyntax;
    }
    const columns = try allocator.alloc(batch.Column, names.len);
    for (names, values, 0..) |name, col, i| {
        columns[i] = try utf8Col(allocator, name, col);
    }
    return .{ .columns = columns, .len = n };
}

pub fn i64Col(allocator: std.mem.Allocator, name: []const u8, values: []const i64) !batch.Column {
    return .{
        .name = try allocator.dupe(u8, name),
        .data_type = .int64,
        .len = values.len,
        .i64s = try allocator.dupe(i64, values),
    };
}

test "utf8Cols builds two string columns" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const b = try utf8Cols(a, &.{ "name", "type" }, &.{
        &.{ "files", "rest" },
        &.{ "files", "iceberg_rest" },
    });
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expectEqualStrings("files", b.columns[0].strAt(0));
    try std.testing.expectEqualStrings("iceberg_rest", b.columns[1].strAt(1));
}

test "i64Col copies values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const col = try i64Col(arena.allocator(), "snapshot_id", &.{ 1, 2 });
    try std.testing.expectEqual(@as(usize, 2), col.len);
    try std.testing.expectEqual(@as(i64, 1), col.i64s[0]);
    try std.testing.expectEqual(@as(i64, 2), col.i64s[1]);
}
