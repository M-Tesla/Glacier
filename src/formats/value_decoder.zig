// ═══════════════════════════════════════════════════════════════════════════
// PARQUET VALUE DECODERS
// ═══════════════════════════════════════════════════════════════════════════
//
// Value decoders read actual column data from Parquet pages.
// Different encodings require different decoding strategies:
//
//   PLAIN:              Uncompressed raw values (little-endian)
//   DICTIONARY:         Index into dictionary (RLE/Bit-Packing)
//   DELTA_BINARY_PACKED: Delta encoding for sorted data (timestamps, IDs)
//   RLE:                Run-length encoding (deprecated for values, use for levels)
//
// PARQUET TYPES:
//   - BOOLEAN (1 bit per value)
//   - INT32 (4 bytes, little-endian)
//   - INT64 (8 bytes, little-endian)
//   - INT96 (12 bytes, deprecated timestamp)
//   - FLOAT (4 bytes, IEEE 754)
//   - DOUBLE (8 bytes, IEEE 754)
//   - BYTE_ARRAY (length-prefixed)
//   - FIXED_LEN_BYTE_ARRAY (fixed size)
//
// REFERENCES:
//   - https://github.com/apache/parquet-format/blob/master/Encodings.md
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const encoding = @import("encoding.zig");

// ═══════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

/// Decode a batch of PLAIN encoded INT64 values
pub fn decodePlainInt64(
    allocator: std.mem.Allocator,
    data: []const u8,
    num_values: usize,
    _: bool // required (ignored for PLAIN which doesn't interleave nulls)
) ![]i64 {
    var values = try allocator.alloc(i64, num_values);
    errdefer allocator.free(values);

    var decoder = PlainInt64Decoder.init(data);
    for (0..num_values) |i| {
        if (decoder.isAtEnd()) return error.UnexpectedEndOfData; // Should satisfy count
        values[i] = try decoder.readValue();
    }
    return values;
}

/// Decode a batch of PLAIN encoded INT32 values
pub fn decodePlainInt32(
    allocator: std.mem.Allocator,
    data: []const u8,
    num_values: usize,
    _: bool
) ![]i32 {
    var values = try allocator.alloc(i32, num_values);
    errdefer allocator.free(values);

    var decoder = PlainInt32Decoder.init(data);
    for (0..num_values) |i| {
        if (decoder.isAtEnd()) return error.UnexpectedEndOfData;
        values[i] = try decoder.readValue();
    }
    return values;
}

/// Decode a batch of PLAIN encoded BYTE_ARRAY (String) values
/// Allocates deep copies of strings because Parquet data buffer is temporary
pub fn decodePlainByteArray(
    allocator: std.mem.Allocator,
    data: []const u8,
    num_values: usize,
    _: bool
) ![][]u8 {
    var values = try allocator.alloc([]u8, num_values);
    errdefer allocator.free(values);

    var decoder = PlainByteArrayDecoder.init(data);
    for (0..num_values) |i| {
        if (decoder.isAtEnd()) return error.UnexpectedEndOfData;
        const slice = try decoder.readValue();
        // Deep copy
        values[i] = try allocator.dupe(u8, slice);
    }
    return values;
}

// ═══════════════════════════════════════════════════════════════════════════
// PLAIN ENCODING DECODERS
// ═══════════════════════════════════════════════════════════════════════════

/// PLAIN decoder for BOOLEAN type (bit-packed)
pub const PlainBooleanDecoder = struct {
    data: []const u8,
    pos: usize,
    bit_offset: u3,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
            .bit_offset = 0,
        };
    }

    pub fn readValue(self: *Self) !bool {
        if (self.pos >= self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const byte = self.data[self.pos];
        const bit = (byte >> self.bit_offset) & 1;

        self.bit_offset +%= 1;
        if (self.bit_offset == 0) {
            self.pos += 1;
        }

        return bit != 0;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};

/// PLAIN decoder for INT32
pub const PlainInt32Decoder = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
        };
    }

    pub fn readValue(self: *Self) !i32 {
        if (self.pos + 4 > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        // Read 4 bytes in little-endian
        const bytes = self.data[self.pos .. self.pos + 4];
        const value = std.mem.readInt(i32, bytes[0..4], .little);
        self.pos += 4;

        return value;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};

/// PLAIN decoder for INT64
pub const PlainInt64Decoder = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
        };
    }

    pub fn readValue(self: *Self) !i64 {
        if (self.pos + 8 > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        // Read 8 bytes in little-endian
        const bytes = self.data[self.pos .. self.pos + 8];
        const value = std.mem.readInt(i64, bytes[0..8], .little);
        self.pos += 8;

        return value;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};

/// PLAIN decoder for FLOAT (IEEE 754 single precision)
pub const PlainFloatDecoder = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
        };
    }

    pub fn readValue(self: *Self) !f32 {
        if (self.pos + 4 > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        // Read 4 bytes as u32, then bitcast to f32
        const bytes = self.data[self.pos .. self.pos + 4];
        const bits = std.mem.readInt(u32, bytes[0..4], .little);
        const value: f32 = @bitCast(bits);
        self.pos += 4;

        return value;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};

/// PLAIN decoder for DOUBLE (IEEE 754 double precision)
pub const PlainDoubleDecoder = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
        };
    }

    pub fn readValue(self: *Self) !f64 {
        if (self.pos + 8 > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        // Read 8 bytes as u64, then bitcast to f64
        const bytes = self.data[self.pos .. self.pos + 8];
        const bits = std.mem.readInt(u64, bytes[0..8], .little);
        const value: f64 = @bitCast(bits);
        self.pos += 8;

        return value;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};





/// PLAIN decoder for BYTE_ARRAY (length-prefixed byte arrays)
pub const PlainByteArrayDecoder = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
        };
    }

    /// Read next byte array
    /// Returns a slice pointing into the original data (zero-copy)
    pub fn readValue(self: *Self) ![]const u8 {
        // Read 4-byte length prefix (little-endian)
        if (self.pos + 4 > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const length_bytes = self.data[self.pos .. self.pos + 4];
        const length = std.mem.readInt(u32, length_bytes[0..4], .little);
        self.pos += 4;

        // Read the actual bytes
        if (self.pos + length > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const value = self.data[self.pos .. self.pos + length];
        self.pos += length;

        return value;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};


/// Decode dictionary indices from a Data Page using RLE/Bit-Packing
/// Reads the bit-width byte, then decodes the values.
pub fn decodeDictionaryIndices(
    allocator: std.mem.Allocator,
    data: []const u8,
    num_values: usize,
) ![]i64 {
    if (data.len < 1) return error.UnexpectedEndOfData;

    // First byte is bit width
    const bit_width = data[0];
    const rle_data = data[1..];

    var decoder = encoding.RLEDecoder.init(rle_data, @intCast(bit_width));
    return decoder.decodeAll(allocator, num_values);
}

/// PLAIN decoder for FIXED_LEN_BYTE_ARRAY
pub const PlainFixedLenByteArrayDecoder = struct {
    data: []const u8,
    pos: usize,
    type_length: u32,

    const Self = @This();

    pub fn init(data: []const u8, type_length: u32) Self {
        return .{
            .data = data,
            .pos = 0,
            .type_length = type_length,
        };
    }

    pub fn readValue(self: *Self) ![]const u8 {
        if (self.pos + self.type_length > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        const value = self.data[self.pos .. self.pos + self.type_length];
        self.pos += self.type_length;

        return value;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// DICTIONARY ENCODING
// ═══════════════════════════════════════════════════════════════════════════
//
// Dictionary encoding stores unique values once, then uses indices.
// Used for low-cardinality columns (e.g., enums, categories).
//
// Format:
//   - Dictionary page: Contains all unique values (PLAIN encoded)
//   - Data pages: Contain indices into dictionary (RLE/Bit-Packing)
//
// ═══════════════════════════════════════════════════════════════════════════

/// Dictionary decoder
/// T is the value type from the dictionary
pub fn DictionaryDecoder(comptime T: type) type {
    return struct {
        dictionary: []const T,
        index_decoder: encoding.RLEDecoder,

        const Self = @This();

        pub fn init(dictionary: []const T, index_data: []const u8, bit_width: u5) Self {
            return .{
                .dictionary = dictionary,
                .index_decoder = encoding.RLEDecoder.init(index_data, bit_width),
            };
        }

        pub fn readValue(self: *Self) !T {
            const index = try self.index_decoder.readValue();

            if (index >= self.dictionary.len) {
                return error.InvalidDictionaryIndex;
            }

            return self.dictionary[index];
        }

        pub fn isAtEnd(self: *const Self) bool {
            return self.index_decoder.isAtEnd();
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "PlainBooleanDecoder" {
    // 8 boolean values: [true, false, true, true, false, false, true, false]
    // Packed as: 0b01001101 = 0x4D (LSB first)
    const data = [_]u8{0x4D};

    var decoder = PlainBooleanDecoder.init(&data);

    try std.testing.expectEqual(true, try decoder.readValue());
    try std.testing.expectEqual(false, try decoder.readValue());
    try std.testing.expectEqual(true, try decoder.readValue());
    try std.testing.expectEqual(true, try decoder.readValue());
    try std.testing.expectEqual(false, try decoder.readValue());
    try std.testing.expectEqual(false, try decoder.readValue());
    try std.testing.expectEqual(true, try decoder.readValue());
    try std.testing.expectEqual(false, try decoder.readValue());
}

test "PlainInt32Decoder" {
    // Two INT32 values: 100, -200 (little-endian)
    const data = [_]u8{
        0x64, 0x00, 0x00, 0x00, // 100
        0x38, 0xFF, 0xFF, 0xFF, // -200
    };

    var decoder = PlainInt32Decoder.init(&data);

    try std.testing.expectEqual(@as(i32, 100), try decoder.readValue());
    try std.testing.expectEqual(@as(i32, -200), try decoder.readValue());
}

test "PlainInt64Decoder" {
    // One INT64 value: 1234567890123 (little-endian)
    const data = [_]u8{
        0xCB, 0x04, 0xFB, 0x71, 0x1F, 0x01, 0x00, 0x00,
    };

    var decoder = PlainInt64Decoder.init(&data);

    try std.testing.expectEqual(@as(i64, 1234567890123), try decoder.readValue());
}

test "PlainFloatDecoder" {
    // Float value: 3.14159 (IEEE 754, little-endian)
    const data = [_]u8{
        0xD0, 0x0F, 0x49, 0x40, // 3.14159
    };

    var decoder = PlainFloatDecoder.init(&data);

    const value = try decoder.readValue();
    const expected: f32 = 3.14159;
    const diff = @abs(value - expected);
    try std.testing.expect(diff < 0.0001);
}

test "PlainDoubleDecoder" {
    // Double value: 2.718281828 (IEEE 754, little-endian)
    const data = [_]u8{
        0x90, 0xF7, 0xAA, 0x95, 0x09, 0xBF, 0x05, 0x40,
    };

    var decoder = PlainDoubleDecoder.init(&data);

    const value = try decoder.readValue();
    const expected: f64 = 2.718281828;
    const diff = @abs(value - expected);
    try std.testing.expect(diff < 0.00001); // Relaxed tolerance for double
}

test "PlainByteArrayDecoder" {
    // Two byte arrays: "hello" (5 bytes), "world" (5 bytes)
    const data = [_]u8{
        0x05, 0x00, 0x00, 0x00, // length = 5
        'h',  'e',  'l',  'l',
        'o',
        0x05, 0x00, 0x00, 0x00, // length = 5
        'w',  'o',  'r',  'l',
        'd',
    };

    var decoder = PlainByteArrayDecoder.init(&data);

    const val1 = try decoder.readValue();
    try std.testing.expectEqualStrings("hello", val1);

    const val2 = try decoder.readValue();
    try std.testing.expectEqualStrings("world", val2);
}

test "PlainFixedLenByteArrayDecoder" {
    // Fixed-length arrays of 3 bytes each
    const data = [_]u8{
        'a', 'b', 'c',
        'd', 'e', 'f',
    };

    var decoder = PlainFixedLenByteArrayDecoder.init(&data, 3);

    const val1 = try decoder.readValue();
    try std.testing.expectEqualStrings("abc", val1);

    const val2 = try decoder.readValue();
    try std.testing.expectEqualStrings("def", val2);
}

test "DictionaryDecoder - i32" {
    // Dictionary: [10, 20, 30, 40]
    const dictionary = [_]i32{ 10, 20, 30, 40 };

    // Simple test: Use RLE runs instead of bit-packing
    // Index sequence: 0, 1, 2, 3
    // 4 separate RLE runs of length 1 each
    // RLE run format: (count << 1) | 0 = count * 2
    const index_data = [_]u8{
        0x02, 0x00, // run of 1x value 0
        0x02, 0x01, // run of 1x value 1
        0x02, 0x02, // run of 1x value 2
        0x02, 0x03, // run of 1x value 3
    };

    var decoder = DictionaryDecoder(i32).init(&dictionary, &index_data, 2);

    try std.testing.expectEqual(@as(i32, 10), try decoder.readValue()); // index 0
    try std.testing.expectEqual(@as(i32, 20), try decoder.readValue()); // index 1
    try std.testing.expectEqual(@as(i32, 30), try decoder.readValue()); // index 2
    try std.testing.expectEqual(@as(i32, 40), try decoder.readValue()); // index 3
}

test "decodePlainInt64" {
    const allocator = std.testing.allocator;
    const data = [_]u8{
        0xCB, 0x04, 0xFB, 0x71, 0x1F, 0x01, 0x00, 0x00, // 1234567890123
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1
    };
    
    const values = try decodePlainInt64(allocator, &data, 2, false);
    defer allocator.free(values);
    
    try std.testing.expectEqual(@as(usize, 2), values.len);
    try std.testing.expectEqual(@as(i64, 1234567890123), values[0]);
    try std.testing.expectEqual(@as(i64, 1), values[1]);
}
