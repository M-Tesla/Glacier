//! Arrow IPC (schema + record batch) from Glacier batches. Flight uses this
//! instead of Arrow C++. Encapsulated messages include the 8-byte prefix;
//! FlightData.data_header is the FlatBuffer Message without that prefix.

const std = @import("std");
const batch_mod = @import("../execution/batch.zig");

const Batch = batch_mod.Batch;
const Column = batch_mod.Column;
const DataType = batch_mod.DataType;

pub const Encoded = struct {
    header: []u8,
    body: []u8,

    pub fn deinit(self: Encoded, allocator: std.mem.Allocator) void {
        allocator.free(self.header);
        allocator.free(self.body);
    }
};

pub fn encodeSchema(allocator: std.mem.Allocator, batch: Batch) ![]u8 {
    var b = try Builder.init(allocator);
    defer b.deinit();
    const schema = try writeSchema(&b, batch);
    const msg = try writeMessage(&b, 1, schema, 0);
    return encapsulate(allocator, b.finish(msg));
}

pub fn encodeSchemaHeader(allocator: std.mem.Allocator, batch: Batch) ![]u8 {
    var b = try Builder.init(allocator);
    defer b.deinit();
    const schema = try writeSchema(&b, batch);
    const msg = try writeMessage(&b, 1, schema, 0);
    return allocator.dupe(u8, b.finish(msg));
}

pub fn encodeBatch(allocator: std.mem.Allocator, batch: Batch) !Encoded {
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(allocator);
    var nodes: std.ArrayList(FieldNode) = .empty;
    defer nodes.deinit(allocator);
    var buffers: std.ArrayList(IpcBuffer) = .empty;
    defer buffers.deinit(allocator);

    try nodes.append(allocator, .{ .length = @intCast(batch.len), .null_count = 0 });
    try buffers.append(allocator, .{ .offset = 0, .length = 0 });

    for (batch.columns) |col| {
        try appendColumn(allocator, &body, &nodes, &buffers, col, batch.len);
    }

    var fb = try Builder.init(allocator);
    defer fb.deinit();
    const rec = try writeRecordBatch(&fb, @intCast(batch.len), nodes.items, buffers.items);
    const msg = try writeMessage(&fb, 3, rec, @intCast(body.items.len));
    return .{
        .header = try allocator.dupe(u8, fb.finish(msg)),
        .body = try body.toOwnedSlice(allocator),
    };
}

pub fn encapsulate(allocator: std.mem.Allocator, header: []const u8) ![]u8 {
    const padded = (header.len + 7) & ~@as(usize, 7);
    const out = try allocator.alloc(u8, 8 + padded);
    std.mem.writeInt(u32, out[0..4], 0xFFFFFFFF, .little);
    std.mem.writeInt(i32, out[4..8], @intCast(padded), .little);
    @memcpy(out[8..][0..header.len], header);
    if (padded > header.len) @memset(out[8 + header.len ..], 0);
    return out;
}

const FieldNode = struct { length: i64, null_count: i64 };
const IpcBuffer = struct { offset: i64, length: i64 };

fn pad8(n: usize) usize {
    return (n + 7) & ~@as(usize, 7);
}

fn appendColumn(
    allocator: std.mem.Allocator,
    body: *std.ArrayList(u8),
    nodes: *std.ArrayList(FieldNode),
    buffers: *std.ArrayList(IpcBuffer),
    col: Column,
    len: usize,
) !void {
    var null_count: i64 = 0;
    if (col.valid.len > 0) {
        for (col.valid[0..len]) |v| {
            if (v == 0) null_count += 1;
        }
    }
    try nodes.append(allocator, .{ .length = @intCast(len), .null_count = null_count });

    if (null_count == 0) {
        try pushBuf(allocator, body, buffers, &.{});
    } else {
        const nbytes = (len + 7) / 8;
        const bits = try allocator.alloc(u8, nbytes);
        defer allocator.free(bits);
        @memset(bits, 0);
        var i: usize = 0;
        while (i < len) : (i += 1) {
            if (col.valid[i] != 0) bits[i / 8] |= @as(u8, 1) << @intCast(i % 8);
        }
        try pushBuf(allocator, body, buffers, bits);
    }

    switch (col.data_type) {
        .boolean => {
            const nbytes = (len + 7) / 8;
            const bits = try allocator.alloc(u8, nbytes);
            defer allocator.free(bits);
            @memset(bits, 0);
            var i: usize = 0;
            while (i < len) : (i += 1) {
                if (col.bools.len > i and col.bools[i] != 0)
                    bits[i / 8] |= @as(u8, 1) << @intCast(i % 8);
            }
            try pushBuf(allocator, body, buffers, bits);
        },
        .int32 => {
            const raw: []const u8 = if (len == 0) &.{} else std.mem.sliceAsBytes(col.i32s[0..len]);
            try pushBuf(allocator, body, buffers, raw);
        },
        .int64, .timestamp, .timestamptz => {
            const raw: []const u8 = if (len == 0) &.{} else std.mem.sliceAsBytes(col.i64s[0..len]);
            try pushBuf(allocator, body, buffers, raw);
        },
        .float32 => {
            const raw: []const u8 = if (len == 0) &.{} else std.mem.sliceAsBytes(col.f32s[0..len]);
            try pushBuf(allocator, body, buffers, raw);
        },
        .float64 => {
            const raw: []const u8 = if (len == 0) &.{} else std.mem.sliceAsBytes(col.f64s[0..len]);
            try pushBuf(allocator, body, buffers, raw);
        },
        .utf8 => {
            const offs: []const u8 = if (col.utf8.offsets.len == 0)
                &.{}
            else
                std.mem.sliceAsBytes(col.utf8.offsets[0 .. len + 1]);
            try pushBuf(allocator, body, buffers, offs);
            const last: usize = if (col.utf8.offsets.len > len) @intCast(col.utf8.offsets[len]) else 0;
            try pushBuf(allocator, body, buffers, col.utf8.bytes[0..last]);
        },
        .uuid => {
            const raw: []const u8 = if (len == 0) &.{} else std.mem.sliceAsBytes(col.uuids[0..len]);
            try pushBuf(allocator, body, buffers, raw);
        },
        .decimal128 => {
            const raw: []const u8 = if (len == 0) &.{} else std.mem.sliceAsBytes(col.i128s[0..len]);
            try pushBuf(allocator, body, buffers, raw);
        },
    }
}

fn pushBuf(
    allocator: std.mem.Allocator,
    body: *std.ArrayList(u8),
    buffers: *std.ArrayList(IpcBuffer),
    bytes: []const u8,
) !void {
    const off: i64 = @intCast(body.items.len);
    try body.appendSlice(allocator, bytes);
    const padded = pad8(body.items.len);
    if (padded > body.items.len) {
        try body.appendNTimes(allocator, 0, padded - body.items.len);
    }
    try buffers.append(allocator, .{ .offset = off, .length = @intCast(bytes.len) });
}

/// Decode the first int64 column of a Flight record batch (tests / CLI).
pub fn firstI64s(allocator: std.mem.Allocator, header: []const u8, body: []const u8) ![]i64 {
    const rec = try parseRecordBatch(header);
    if (rec.nodes.len < 2 or rec.buffers.len < 3) return error.InvalidIpc;
    const len: usize = @intCast(rec.nodes[1].length);
    const buf = rec.buffers[2];
    const start: usize = @intCast(buf.offset);
    const n: usize = @intCast(buf.length);
    if (start + n > body.len) return error.InvalidIpc;
    if (n < len * 8) return error.InvalidIpc;
    const out = try allocator.alloc(i64, len);
    const src = body[start..][0 .. len * 8];
    @memcpy(std.mem.sliceAsBytes(out), src);
    return out;
}

const ParsedBatch = struct {
    nodes: []const FieldNode,
    buffers: []const IpcBuffer,
};

fn parseRecordBatch(header: []const u8) !ParsedBatch {
    if (header.len < 8) return error.InvalidIpc;
    const root_off = std.mem.readInt(u32, header[0..4], .little);
    if (root_off >= header.len) return error.InvalidIpc;
    const rec_pos = try unionTable(header, root_off, 2);
    const nodes = try readStructVec(header, try offsetTable(header, rec_pos, 1), FieldNode);
    const buffers = try readStructVec(header, try offsetTable(header, rec_pos, 2), IpcBuffer);
    return .{ .nodes = nodes, .buffers = buffers };
}

fn tableField(buf: []const u8, table: u32, slot: u16) ?u16 {
    if (table + 4 > buf.len) return null;
    const vt_rel = std.mem.readInt(i32, buf[table..][0..4], .little);
    const vt: i64 = @as(i64, table) + vt_rel;
    if (vt < 0 or @as(u64, @intCast(vt)) + 4 > buf.len) return null;
    const vt_u: usize = @intCast(vt);
    const vt_size = std.mem.readInt(u16, buf[vt_u..][0..2], .little);
    const idx: usize = 4 + @as(usize, slot) * 2;
    if (idx + 2 > vt_size) return null;
    const off = std.mem.readInt(i16, buf[vt_u + idx ..][0..2], .little);
    if (off == 0) return null;
    return @intCast(off);
}

fn unionTable(buf: []const u8, table: u32, slot: u16) !u32 {
    const field_off = tableField(buf, table, slot) orelse return error.InvalidIpc;
    const field_pos = table + field_off;
    if (field_pos + 4 > buf.len) return error.InvalidIpc;
    const rel = std.mem.readInt(u32, buf[field_pos..][0..4], .little);
    return field_pos + rel;
}

fn offsetTable(buf: []const u8, table: u32, slot: u16) !u32 {
    return unionTable(buf, table, slot);
}

fn readStructVec(buf: []const u8, pos: u32, comptime T: type) ![]const T {
    if (pos + 4 > buf.len) return error.InvalidIpc;
    const len = std.mem.readInt(u32, buf[pos..][0..4], .little);
    const bytes = @sizeOf(T) * len;
    if (pos + 4 + bytes > buf.len) return error.InvalidIpc;
    const ptr: [*]const T = @ptrCast(@alignCast(buf[pos + 4 ..].ptr));
    return ptr[0..len];
}

/// FlatBuffers builder: write from the end of a buffer (Apache algorithm).
const Builder = struct {
    allocator: std.mem.Allocator,
    storage: []u8,
    pos: usize,
    cap: usize,
    vtable: [16]u32 = @splat(0),
    nested_end: u32 = 0,

    fn init(allocator: std.mem.Allocator) !Builder {
        const cap: usize = 256;
        const storage = try allocator.alloc(u8, cap);
        return .{ .allocator = allocator, .storage = storage, .pos = cap, .cap = cap };
    }

    fn deinit(self: *Builder) void {
        self.allocator.free(self.storage);
    }

    fn size(self: Builder) u32 {
        return @intCast(self.cap - self.pos);
    }

    fn grow(self: *Builder, need: usize) !void {
        if (self.pos >= need) return;
        const used = self.cap - self.pos;
        const new_cap = @max(self.cap * 2, used + need + 64);
        const new_st = try self.allocator.alloc(u8, new_cap);
        const new_pos = new_cap - used;
        @memcpy(new_st[new_pos..][0..used], self.storage[self.pos..]);
        self.allocator.free(self.storage);
        self.storage = new_st;
        self.pos = new_pos;
        self.cap = new_cap;
    }

    fn prep(self: *Builder, alignment: u32, additional: u32) !void {
        while ((self.size() + additional) % alignment != 0) {
            try self.grow(1);
            self.pos -= 1;
            self.storage[self.pos] = 0;
        }
    }

    fn pushBytes(self: *Builder, bytes: []const u8) !void {
        try self.grow(bytes.len);
        self.pos -= bytes.len;
        @memcpy(self.storage[self.pos..][0..bytes.len], bytes);
    }

    fn pushU8(self: *Builder, v: u8) !void {
        try self.grow(1);
        self.pos -= 1;
        self.storage[self.pos] = v;
    }

    fn pushI16(self: *Builder, v: i16) !void {
        try self.prep(2, 0);
        var tmp: [2]u8 = undefined;
        std.mem.writeInt(i16, &tmp, v, .little);
        try self.pushBytes(&tmp);
    }

    fn pushI32(self: *Builder, v: i32) !void {
        try self.prep(4, 0);
        var tmp: [4]u8 = undefined;
        std.mem.writeInt(i32, &tmp, v, .little);
        try self.pushBytes(&tmp);
    }

    fn pushU32(self: *Builder, v: u32) !void {
        try self.pushI32(@bitCast(v));
    }

    fn pushI64(self: *Builder, v: i64) !void {
        try self.prep(8, 0);
        var tmp: [8]u8 = undefined;
        std.mem.writeInt(i64, &tmp, v, .little);
        try self.pushBytes(&tmp);
    }

    fn startObject(self: *Builder) void {
        self.vtable = @splat(0);
        self.nested_end = self.size();
    }

    fn refer(self: *Builder, dest: u32) !void {
        try self.prep(4, 0);
        const rel: u32 = self.size() - dest + 4;
        try self.pushU32(rel);
    }

    fn slot(self: *Builder, id: u16) void {
        self.vtable[id] = self.size();
    }

    fn endObject(self: *Builder, nfields: u16) !u32 {
        try self.pushI32(0);
        const obj_from_end = self.size();
        const obj_size: u16 = @intCast(obj_from_end - self.nested_end);

        var i: u16 = 0;
        while (i < nfields) : (i += 1) {
            if (self.vtable[i] != 0) {
                self.vtable[i] = obj_from_end - self.vtable[i];
            }
        }

        var f = nfields;
        while (f > 0) {
            f -= 1;
            try self.pushI16(@intCast(self.vtable[f]));
        }
        try self.pushI16(@intCast(obj_size));
        const vt_size: u16 = 4 + nfields * 2;
        try self.pushI16(@intCast(vt_size));

        const obj_index = self.cap - self.pos - obj_from_end;
        const soff: i32 = -@as(i32, @intCast(obj_index));
        std.mem.writeInt(i32, self.storage[self.pos + obj_index ..][0..4], soff, .little);
        return obj_from_end;
    }

    fn createString(self: *Builder, s: []const u8) !u32 {
        try self.pushU8(0);
        var i = s.len;
        while (i > 0) {
            i -= 1;
            try self.pushU8(s[i]);
        }
        try self.prep(4, 0);
        try self.pushU32(@intCast(s.len));
        return self.size();
    }

    fn createOffVector(self: *Builder, offs: []const u32) !u32 {
        var i = offs.len;
        while (i > 0) {
            i -= 1;
            try self.refer(offs[i]);
        }
        try self.prep(4, 0);
        try self.pushU32(@intCast(offs.len));
        return self.size();
    }

    fn createStructVec(self: *Builder, comptime T: type, items: []const T) !u32 {
        const bytes = std.mem.sliceAsBytes(items);
        try self.prep(@alignOf(T), @intCast(bytes.len));
        try self.pushBytes(bytes);
        try self.prep(4, 0);
        try self.pushU32(@intCast(items.len));
        return self.size();
    }

    fn finish(self: *Builder, root: u32) []u8 {
        self.prep(4, 0) catch {};
        const rel: u32 = self.size() - root + 4;
        var tmp: [4]u8 = undefined;
        std.mem.writeInt(u32, &tmp, rel, .little);
        self.pushBytes(&tmp) catch {};
        return self.storage[self.pos..];
    }
};

fn writeEmpty(b: *Builder) !u32 {
    b.startObject();
    return b.endObject(0);
}

fn writeIntType(b: *Builder, bit_width: i32) !u32 {
    b.startObject();
    try b.pushU8(1);
    b.slot(1);
    try b.pushI32(bit_width);
    b.slot(0);
    return b.endObject(2);
}

fn writeTimestampType(b: *Builder, tz: ?[]const u8) !u32 {
    var tz_off: ?u32 = null;
    if (tz) |s| tz_off = try b.createString(s);
    b.startObject();
    if (tz_off) |o| {
        try b.refer(o);
        b.slot(1);
    }
    try b.pushI16(2);
    b.slot(0);
    return b.endObject(2);
}

fn writeDecimalType(b: *Builder, precision: i32, scale: i32) !u32 {
    b.startObject();
    try b.pushI32(128);
    b.slot(2);
    try b.pushI32(scale);
    b.slot(1);
    try b.pushI32(precision);
    b.slot(0);
    return b.endObject(3);
}

fn writeFixedSize(b: *Builder, n: i32) !u32 {
    b.startObject();
    try b.pushI32(n);
    b.slot(0);
    return b.endObject(1);
}

fn writeFloatType(b: *Builder, precision: i16) !u32 {
    b.startObject();
    try b.pushI16(precision);
    b.slot(0);
    return b.endObject(1);
}

fn writeField(b: *Builder, col: Column) !u32 {
    const name = try b.createString(col.name);
    var type_off: u32 = 0;
    var type_id: u8 = 1;
    switch (col.data_type) {
        .boolean => {
            type_id = 6;
            type_off = try writeEmpty(b);
        },
        .int32 => {
            type_id = 2;
            type_off = try writeIntType(b, 32);
        },
        .int64 => {
            type_id = 2;
            type_off = try writeIntType(b, 64);
        },
        .float32 => {
            type_id = 3;
            type_off = try writeFloatType(b, 1);
        },
        .float64 => {
            type_id = 3;
            type_off = try writeFloatType(b, 2);
        },
        .utf8 => {
            type_id = 5;
            type_off = try writeEmpty(b);
        },
        .timestamp => {
            type_id = 10;
            type_off = try writeTimestampType(b, null);
        },
        .timestamptz => {
            type_id = 10;
            type_off = try writeTimestampType(b, "UTC");
        },
        .uuid => {
            type_id = 15;
            type_off = try writeFixedSize(b, 16);
        },
        .decimal128 => {
            type_id = 7;
            type_off = try writeDecimalType(b, col.decimal_precision, col.decimal_scale);
        },
    }
    b.startObject();
    if (type_off != 0) {
        try b.refer(type_off);
        b.slot(3);
    }
    try b.pushU8(type_id);
    b.slot(2);
    try b.pushU8(1);
    b.slot(1);
    try b.refer(name);
    b.slot(0);
    return b.endObject(7);
}

fn writeSchema(b: *Builder, batch: Batch) !u32 {
    const fields = try b.allocator.alloc(u32, batch.columns.len);
    defer b.allocator.free(fields);
    for (batch.columns, 0..) |col, i| {
        fields[i] = try writeField(b, col);
    }
    const vec = try b.createOffVector(fields);
    b.startObject();
    try b.refer(vec);
    b.slot(1);
    return b.endObject(4);
}

fn writeRecordBatch(b: *Builder, length: i64, nodes: []const FieldNode, buffers: []const IpcBuffer) !u32 {
    const nodes_off = try b.createStructVec(FieldNode, nodes);
    const bufs_off = try b.createStructVec(IpcBuffer, buffers);
    b.startObject();
    try b.refer(bufs_off);
    b.slot(2);
    try b.refer(nodes_off);
    b.slot(1);
    try b.pushI64(length);
    b.slot(0);
    return b.endObject(5);
}

fn writeMessage(b: *Builder, header_type: u8, header: u32, body_len: i64) !u32 {
    b.startObject();
    try b.pushI64(body_len);
    b.slot(3);
    try b.refer(header);
    b.slot(2);
    try b.pushU8(header_type);
    b.slot(1);
    try b.pushI16(4);
    b.slot(0);
    return b.endObject(5);
}

test "ipc roundtrip int64" {
    const gpa = std.testing.allocator;
    const vals = try gpa.alloc(i64, 3);
    defer gpa.free(vals);
    vals[0] = 1;
    vals[1] = 2;
    vals[2] = 40;
    const cols = try gpa.alloc(Column, 1);
    defer gpa.free(cols);
    cols[0] = .{ .name = "n", .data_type = .int64, .len = 3, .i64s = vals };
    const batch: Batch = .{ .columns = cols, .len = 3 };
    const enc = try encodeBatch(gpa, batch);
    defer enc.deinit(gpa);
    const got = try firstI64s(gpa, enc.header, enc.body);
    defer gpa.free(got);
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expectEqual(@as(i64, 1), got[0]);
    try std.testing.expectEqual(@as(i64, 40), got[2]);
}
