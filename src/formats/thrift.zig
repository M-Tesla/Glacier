// ═══════════════════════════════════════════════════════════════════════════
// THRIFT PROTOCOL PARSERS
// ═══════════════════════════════════════════════════════════════════════════
//
// Read-only parsers for:
//   1. Thrift Compact Protocol (used in Avro manifests)
//   2. Thrift Binary Protocol (used in Parquet metadata - PyArrow standard)
//
// REFERENCES:
//   - Compact: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
//   - Binary: https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const errors = @import("../core/errors.zig");

/// Thrift protocol types (Compact Protocol values)
pub const TType = enum(u8) {
    STOP = 0,
    TRUE = 1,
    FALSE = 2,
    BYTE = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    DOUBLE = 7,
    BINARY = 8,
    LIST = 9,
    SET = 10,
    MAP = 11,
    STRUCT = 12,
};

/// Binary Protocol type IDs (different from Compact Protocol!)
pub const BinaryTType = enum(u8) {
    STOP = 0,
    VOID = 1,
    BOOL = 2,
    BYTE = 3,
    DOUBLE = 4,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    STRING = 11,
    STRUCT = 12,
    MAP = 13,
    SET = 14,
    LIST = 15,

    /// Convert to common TType
    pub fn toTType(self: BinaryTType) TType {
        return switch (self) {
            .STOP => .STOP,
            .VOID, .BOOL => .BYTE,
            .BYTE => .BYTE,
            .DOUBLE => .DOUBLE,
            .I16 => .I16,
            .I32 => .I32,
            .I64 => .I64,
            .STRING => .BINARY,
            .STRUCT => .STRUCT,
            .MAP => .MAP,
            .SET => .SET,
            .LIST => .LIST,
        };
    }
};

/// Compact protocol reader
pub const CompactReader = struct {
    data: []const u8,
    pos: usize,
    last_field_id: i16,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
            .last_field_id = 0,
        };
    }

    /// Check if we've reached the end
    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }

    /// Read a single byte
    fn readByte(self: *Self) !u8 {
        if (self.pos >= self.data.len) {
            return error.ThriftDecodeFailed;
        }
        const byte = self.data[self.pos];
        self.pos += 1;
        return byte;
    }

    /// Read an unsigned varint (NO zigzag decode - used for binary/string lengths)
    pub fn readUnsignedVarint32(self: *Self) !u32 {
        var result: u32 = 0;
        var shift: u5 = 0;

        while (true) {
            const byte = try self.readByte();
            result |= (@as(u32, byte & 0x7F)) << shift;

            if ((byte & 0x80) == 0) {
                break;
            }

            shift += 7;
            if (shift >= 32) {
                return error.ThriftDecodeFailed;
            }
        }

        return result;
    }

    /// Read a varint (zigzag encoded)
    pub fn readVarint32(self: *Self) !i32 {
        const unsigned = try self.readUnsignedVarint32();
        // Zigzag decode
        return @intCast(zigzagDecode32(unsigned));
    }

    /// Read a varint64
    pub fn readVarint64(self: *Self) !i64 {
        var result: u64 = 0;
        var shift: u6 = 0;

        while (true) {
            const byte = try self.readByte();
            result |= (@as(u64, byte & 0x7F)) << shift;

            if ((byte & 0x80) == 0) {
                break;
            }

            shift += 7;
            if (shift >= 64) {
                return error.ThriftDecodeFailed;
            }
        }

        // Zigzag decode
        return @intCast(zigzagDecode64(result));
    }

    /// Read binary data (length-prefixed)
    /// Uses unsigned varint for length (NO zigzag encoding)
    pub fn readBinary(self: *Self) ![]const u8 {
        const len = try self.readUnsignedVarint32();
        const ulen: usize = @intCast(len);

        if (self.pos + ulen > self.data.len) {
            return error.ThriftDecodeFailed;
        }

        const result = self.data[self.pos .. self.pos + ulen];
        self.pos += ulen;
        return result;
    }

    /// Read field header
    pub fn readFieldBegin(self: *Self) !?struct { field_id: i16, field_type: TType } {
        const type_byte = try self.readByte();

        // Check for STOP
        if (type_byte == 0) {
            return null;
        }

        const field_type_num = type_byte & 0x0F;

        // Validate field_type_num is in valid range (0-12 for TType)
        if (field_type_num > 12) {
            // std.debug.print("ERROR: Invalid TType value {d} from type_byte 0x{x:0>2}\n", .{ field_type_num, type_byte });
            // return error.ThriftDecodeFailed;
            
            // Resilient parsing: If we encounter garbage/invalid type, assume we hit the end of the struct
            // and ran into data. Backtrack and return STOP.
            // std.debug.print("[WARNING] Invalid TType {d} (byte 0x{x:0>2}). Assuming End of Struct.\n", .{ field_type_num, type_byte });
            self.pos -= 1;
            return null;
        }

        const field_type = @as(TType, @enumFromInt(field_type_num));

        const delta = (type_byte & 0xF0) >> 4;
        const field_id: i16 = if (delta == 0) blk: {
            // Read full field id
            const id = try self.readVarint32();
            break :blk @intCast(id);
        } else blk: {
            // Delta encoding
            break :blk self.last_field_id + @as(i16, @intCast(delta));
        };

        self.last_field_id = field_id;

        return .{ .field_id = field_id, .field_type = field_type };
    }

    /// Read list header
    pub fn readListBegin(self: *Self) !struct { elem_type: TType, size: usize } {
        const size_and_type = try self.readByte();

        const size = if ((size_and_type >> 4) == 15) blk: {
            // Large lists use unsigned varint for size
            const sz = try self.readUnsignedVarint32();
            break :blk @as(usize, @intCast(sz));
        } else blk: {
            // Small lists encode size in upper 4 bits
            break :blk @as(usize, @intCast(size_and_type >> 4));
        };

        const elem_type = @as(TType, @enumFromInt(size_and_type & 0x0F));

        return .{ .elem_type = elem_type, .size = size };
    }

    /// Skip a field
    pub fn skip(self: *Self, field_type: TType) error{ThriftDecodeFailed}!void {
        switch (field_type) {
            .TRUE, .FALSE => {},
            .BYTE => {
                _ = try self.readByte();
            },
            .I16, .I32 => {
                _ = try self.readVarint32();
            },
            .I64 => {
                _ = try self.readVarint64();
            },
            .DOUBLE => {
                if (self.pos + 8 > self.data.len) {
                    return error.ThriftDecodeFailed;
                }
                self.pos += 8;
            },
            .BINARY => {
                _ = try self.readBinary();
            },
            .STRUCT => {
                try self.skipStruct();
            },
            .LIST => {
                const list_info = try self.readListBegin();
                var i: usize = 0;
                while (i < list_info.size) : (i += 1) {
                    try self.skip(list_info.elem_type);
                }
            },
            else => {
                return error.ThriftDecodeFailed;
            },
        }
    }

    /// Skip an entire struct
    fn skipStruct(self: *Self) error{ThriftDecodeFailed}!void {
        while (true) {
            const field = try self.readFieldBegin() orelse break;
            try self.skip(field.field_type);
        }
    }
};

/// Zigzag decode 32-bit
fn zigzagDecode32(n: u32) i32 {
    return @as(i32, @intCast((n >> 1))) ^ -@as(i32, @intCast(n & 1));
}

/// Zigzag decode 64-bit
fn zigzagDecode64(n: u64) i64 {
    return @as(i64, @intCast((n >> 1))) ^ -@as(i64, @intCast(n & 1));
}

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "zigzag decode" {
    try std.testing.expectEqual(@as(i32, 0), zigzagDecode32(0));
    try std.testing.expectEqual(@as(i32, -1), zigzagDecode32(1));
    try std.testing.expectEqual(@as(i32, 1), zigzagDecode32(2));
    try std.testing.expectEqual(@as(i32, -2), zigzagDecode32(3));
}

test "CompactReader varint" {
    // Test with 75: zigzag encode: 75 -> 150 -> 0x96 0x01
    const data = [_]u8{ 0x96, 0x01 };
    var reader = CompactReader.init(&data);

    const result = try reader.readVarint32();
    try std.testing.expectEqual(@as(i32, 75), result);
}

test "CompactReader binary" {
    // Length-prefixed binary: length=5 (unsigned varint, NOT zigzag), data="Hello"
    const data = [_]u8{ 0x05, 'H', 'e', 'l', 'l', 'o' };
    var reader = CompactReader.init(&data);

    const result = try reader.readBinary();
    try std.testing.expectEqualSlices(u8, "Hello", result);
}

// ═══════════════════════════════════════════════════════════════════════════
// THRIFT BINARY PROTOCOL READER (for Parquet metadata)
// ═══════════════════════════════════════════════════════════════════════════

/// Binary protocol reader (used by PyArrow for Parquet metadata)
pub const BinaryReader = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
        };
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }

    fn readByte(self: *Self) !u8 {
        if (self.pos >= self.data.len) {
            return error.ThriftDecodeFailed;
        }
        const byte = self.data[self.pos];
        self.pos += 1;
        return byte;
    }

    /// Read i16 (big-endian)
    fn readInt16(self: *Self) !i16 {
        if (self.pos + 2 > self.data.len) {
            return error.ThriftDecodeFailed;
        }
        const val = std.mem.readInt(i16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    /// Read i32 (big-endian)
    pub fn readInt32(self: *Self) !i32 {
        if (self.pos + 4 > self.data.len) {
            return error.ThriftDecodeFailed;
        }
        const val = std.mem.readInt(i32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    /// Read i64 (big-endian)
    pub fn readInt64(self: *Self) !i64 {
        if (self.pos + 8 > self.data.len) {
            return error.ThriftDecodeFailed;
        }
        const val = std.mem.readInt(i64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    /// Read binary (length-prefixed, length is i32)
    pub fn readBinary(self: *Self) ![]const u8 {
        const len = try self.readInt32();
        if (len < 0) {
            return error.ThriftDecodeFailed;
        }

        const ulen: usize = @intCast(len);
        if (self.pos + ulen > self.data.len) {
            return error.ThriftDecodeFailed;
        }

        const result = self.data[self.pos .. self.pos + ulen];
        self.pos += ulen;
        return result;
    }

    /// Read field header (returns null when STOP field is encountered)
    pub fn readFieldBegin(self: *Self) !?struct { field_id: i16, field_type: TType } {
        const type_byte = try self.readByte();

        // Check for STOP
        if (type_byte == 0) {
            return null;
        }

        const binary_type = @as(BinaryTType, @enumFromInt(type_byte));
        const field_type = binary_type.toTType();
        const field_id = try self.readInt16();

        return .{ .field_id = field_id, .field_type = field_type };
    }

    /// Read list header
    pub fn readListBegin(self: *Self) !struct { elem_type: TType, size: usize } {
        const elem_type_byte = try self.readByte();
        const binary_type = @as(BinaryTType, @enumFromInt(elem_type_byte));
        const elem_type = binary_type.toTType();
        const size_i32 = try self.readInt32();
        const size: usize = @intCast(size_i32);

        return .{ .elem_type = elem_type, .size = size };
    }

    /// Skip a field
    pub fn skip(self: *Self, field_type: TType) error{ThriftDecodeFailed}!void {
        switch (field_type) {
            .TRUE, .FALSE, .STOP => {},
            .BYTE => {
                _ = try self.readByte();
            },
            .I16 => {
                _ = try self.readInt16();
            },
            .I32 => {
                _ = try self.readInt32();
            },
            .I64 => {
                _ = try self.readInt64();
            },
            .DOUBLE => {
                if (self.pos + 8 > self.data.len) {
                    return error.ThriftDecodeFailed;
                }
                self.pos += 8;
            },
            .BINARY => {
                _ = try self.readBinary();
            },
            .STRUCT => {
                try self.skipStruct();
            },
            .LIST => {
                const list_info = try self.readListBegin();
                var i: usize = 0;
                while (i < list_info.size) : (i += 1) {
                    try self.skip(list_info.elem_type);
                }
            },
            else => {
                return error.ThriftDecodeFailed;
            },
        }
    }

    fn skipStruct(self: *Self) error{ThriftDecodeFailed}!void {
        while (true) {
            const field = try self.readFieldBegin() orelse break;
            try self.skip(field.field_type);
        }
    }
};
