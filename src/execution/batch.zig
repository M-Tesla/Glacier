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
    /// `t.id` also matches a unique column named `id` (FROM subquery / CTE).
    pub fn lookup(self: Batch, name: []const u8) !usize {
        if (self.columnIndex(name)) |i| return i;
        if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| {
            if (self.columnIndex(name[dot + 1 ..])) |i| return i;
        }
        var found: ?usize = null;
        const tail = if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| name[dot + 1 ..] else name;
        for (self.columns, 0..) |col, i| {
            const cdot = std.mem.lastIndexOfScalar(u8, col.name, '.') orelse continue;
            if (!std.ascii.eqlIgnoreCase(col.name[cdot + 1 ..], tail)) continue;
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

    pub fn gather(self: Batch, allocator: std.mem.Allocator, idx: []const usize) !Batch {
        const columns = try allocator.alloc(Column, self.columns.len);
        for (self.columns, 0..) |src, ci| {
            columns[ci] = try gatherColumn(allocator, src, idx);
        }
        return .{ .columns = columns, .len = idx.len };
    }
};

fn gatherColumn(allocator: std.mem.Allocator, src: Column, idx: []const usize) !Column {
    var dst: Column = .{
        .name = src.name,
        .data_type = src.data_type,
        .len = idx.len,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    switch (src.data_type) {
        .boolean => {
            dst.bools = try allocator.alloc(u8, idx.len);
            for (idx, 0..) |row, i| dst.bools[i] = src.bools[row];
        },
        .int32 => {
            dst.i32s = try allocator.alloc(i32, idx.len);
            for (idx, 0..) |row, i| dst.i32s[i] = src.i32s[row];
        },
        .int64, .timestamp, .timestamptz => {
            dst.i64s = try allocator.alloc(i64, idx.len);
            for (idx, 0..) |row, i| dst.i64s[i] = src.i64s[row];
        },
        .float32 => {
            dst.f32s = try allocator.alloc(f32, idx.len);
            for (idx, 0..) |row, i| dst.f32s[i] = src.f32s[row];
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, idx.len);
            for (idx, 0..) |row, i| dst.f64s[i] = src.f64s[row];
        },
        .utf8 => {
            var nbytes: usize = 0;
            for (idx) |row| nbytes += src.strAt(row).len;
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, idx.len + 1);
            var off: u32 = 0;
            for (idx, 0..) |row, i| {
                offsets[i] = off;
                const s = src.strAt(row);
                if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
                off += @intCast(s.len);
            }
            offsets[idx.len] = off;
            dst.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
        .uuid => {
            dst.uuids = try allocator.alloc([16]u8, idx.len);
            for (idx, 0..) |row, i| dst.uuids[i] = src.uuids[row];
        },
        .decimal128 => {
            dst.i128s = try allocator.alloc(i128, idx.len);
            for (idx, 0..) |row, i| dst.i128s[i] = src.i128s[row];
        },
    }
    if (src.valid.len > 0) {
        const valid = try allocator.alloc(u8, idx.len);
        var any_null = false;
        for (idx, 0..) |row, i| {
            valid[i] = src.valid[row];
            if (valid[i] == 0) any_null = true;
        }
        dst.valid = if (any_null) valid else blk: {
            allocator.free(valid);
            break :blk &.{};
        };
    }
    return dst;
}
