//! Columnar batch. Arena-owned; Result.deinit frees everything.

const std = @import("std");

pub const DataType = enum {
    boolean,
    int32,
    int64,
    float32,
    float64,
    utf8,
    timestamp,
    timestamptz,
    uuid,
    decimal128,

    pub fn storesI64(self: DataType) bool {
        return switch (self) {
            .int64, .timestamp, .timestamptz => true,
            else => false,
        };
    }
};

pub const ColumnSchema = struct {
    name: []const u8,
    data_type: DataType,
};

pub const Utf8 = struct {
    offsets: []u32,
    bytes: []u8,
};

pub const Column = struct {
    name: []const u8,
    data_type: DataType,
    len: usize,
    bools: []u8 = &.{},
    i32s: []i32 = &.{},
    i64s: []i64 = &.{},
    f32s: []f32 = &.{},
    f64s: []f64 = &.{},
    utf8: Utf8 = .{ .offsets = &.{}, .bytes = &.{} },
    uuids: [][16]u8 = &.{},
    i128s: []i128 = &.{},
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
    /// Empty means every row is non-null. Otherwise `0` = null, `1` = valid.
    valid: []u8 = &.{},

    pub fn strAt(self: Column, row: usize) []const u8 {
        const start = self.utf8.offsets[row];
        const end = self.utf8.offsets[row + 1];
        return self.utf8.bytes[start..end];
    }

    pub fn isNull(self: Column, row: usize) bool {
        return self.valid.len > 0 and self.valid[row] == 0;
    }

    pub fn isValid(self: Column, row: usize) bool {
        return !self.isNull(row);
    }
};

pub const Batch = struct {
    columns: []Column,
    len: usize,

    pub fn schema(self: Batch, allocator: std.mem.Allocator) ![]ColumnSchema {
        const out = try allocator.alloc(ColumnSchema, self.columns.len);
        for (self.columns, 0..) |col, i| {
            out[i] = .{ .name = col.name, .data_type = col.data_type };
        }
        return out;
    }

    pub fn columnIndex(self: Batch, name: []const u8) ?usize {
        for (self.columns, 0..) |col, i| {
            if (std.ascii.eqlIgnoreCase(col.name, name)) return i;
        }
        return null;
    }

    /// Exact name, or a unique `alias.col` suffix. Two matches → AmbiguousColumn.
    pub fn lookup(self: Batch, name: []const u8) !usize {
        if (self.columnIndex(name)) |i| return i;
        var found: ?usize = null;
        for (self.columns, 0..) |col, i| {
            const dot = std.mem.lastIndexOfScalar(u8, col.name, '.') orelse continue;
            if (!std.ascii.eqlIgnoreCase(col.name[dot + 1 ..], name)) continue;
            if (found != null) return error.AmbiguousColumn;
            found = i;
        }
        return found orelse error.ColumnNotFound;
    }

    pub fn project(self: Batch, allocator: std.mem.Allocator, names: []const []const u8) !Batch {
        var cols = try allocator.alloc(Column, names.len);
        for (names, 0..) |name, i| {
            const idx = self.columnIndex(name) orelse return error.ColumnNotFound;
            cols[i] = self.columns[idx];
        }
        return .{ .columns = cols, .len = self.len };
    }

    pub fn limit(self: Batch, n: u64) Batch {
        const keep: usize = @intCast(@min(n, @as(u64, @intCast(self.len))));
        if (keep == self.len) return self;
        var copy = self;
        copy.len = keep;
        for (copy.columns) |*col| col.len = keep;
        return copy;
    }
};
