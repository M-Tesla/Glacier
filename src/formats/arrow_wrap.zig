//! Thin Zig wrapper around nanoarrow (C). Does not reimplement Arrow.

const std = @import("std");
const batch_mod = @import("../execution/batch.zig");

const c = struct {
    const Col = extern struct {
        name: ?[*:0]const u8,
        type: i32,
        bools: ?[*]const u8,
        i32s: ?[*]const i32,
        i64s: ?[*]const i64,
        f32s: ?[*]const f32,
        f64s: ?[*]const f64,
        utf8_offsets: ?[*]const u32,
        utf8_bytes: ?[*]const u8,
        uuids: ?[*]const u8,
        i128s_le: ?[*]const u8,
        decimal_precision: i32,
        decimal_scale: i32,
        valid: ?[*]const u8,
    };

    extern fn glacier_arrow_export(
        cols: ?[*]const Col,
        n_cols: i32,
        n_rows: i64,
        out_array: *Array,
        out_schema: *Schema,
    ) c_int;
};

pub const Schema = extern struct {
    format: ?[*:0]const u8 = null,
    name: ?[*:0]const u8 = null,
    metadata: ?[*:0]const u8 = null,
    flags: i64 = 0,
    n_children: i64 = 0,
    children: ?[*]?*Schema = null,
    dictionary: ?*Schema = null,
    release: ?*const fn (*Schema) callconv(.c) void = null,
    private_data: ?*anyopaque = null,
};

pub const Array = extern struct {
    length: i64 = 0,
    null_count: i64 = 0,
    offset: i64 = 0,
    n_buffers: i64 = 0,
    n_children: i64 = 0,
    buffers: ?[*]?*const anyopaque = null,
    children: ?[*]?*Array = null,
    dictionary: ?*Array = null,
    release: ?*const fn (*Array) callconv(.c) void = null,
    private_data: ?*anyopaque = null,
};

pub fn exportBatch(
    allocator: std.mem.Allocator,
    batch: batch_mod.Batch,
    out_array: *Array,
    out_schema: *Schema,
) c_int {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cols = a.alloc(c.Col, batch.columns.len) catch return -1;
    for (batch.columns, 0..) |col, i| {
        var i128s_le: ?[*]const u8 = null;
        if (col.data_type == .decimal128 and col.i128s.len > 0) {
            const raw = a.alloc(u8, col.i128s.len * 16) catch return -1;
            for (col.i128s, 0..) |v, ri| {
                var tmp: [16]u8 = undefined;
                std.mem.writeInt(i128, &tmp, v, .little);
                @memcpy(raw[ri * 16 ..][0..16], &tmp);
            }
            i128s_le = raw.ptr;
        }
        cols[i] = .{
            .name = (a.dupeZ(u8, col.name) catch return -1).ptr,
            .type = switch (col.data_type) {
                .boolean => 0,
                .int32 => 1,
                .int64 => 2,
                .float32 => 5,
                .float64 => 3,
                .utf8 => 4,
                .timestamp => 6,
                .timestamptz => 7,
                .uuid => 8,
                .decimal128 => 9,
            },
            .bools = if (col.bools.len > 0) col.bools.ptr else null,
            .i32s = if (col.i32s.len > 0) col.i32s.ptr else null,
            .i64s = if (col.i64s.len > 0) col.i64s.ptr else null,
            .f32s = if (col.f32s.len > 0) col.f32s.ptr else null,
            .f64s = if (col.f64s.len > 0) col.f64s.ptr else null,
            .utf8_offsets = if (col.utf8.offsets.len > 0) col.utf8.offsets.ptr else null,
            .utf8_bytes = if (col.utf8.bytes.len > 0) col.utf8.bytes.ptr else null,
            .uuids = if (col.uuids.len > 0) @ptrCast(col.uuids.ptr) else null,
            .i128s_le = i128s_le,
            .decimal_precision = col.decimal_precision,
            .decimal_scale = col.decimal_scale,
            .valid = if (col.valid.len > 0) col.valid.ptr else null,
        };
    }

    return c.glacier_arrow_export(
        if (cols.len == 0) null else cols.ptr,
        @intCast(batch.columns.len),
        @intCast(batch.len),
        out_array,
        out_schema,
    );
}
