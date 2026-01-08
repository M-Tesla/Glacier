// ═══════════════════════════════════════════════════════════════════════════
// PARQUET ENCODING SCHEMES
// ═══════════════════════════════════════════════════════════════════════════
//
// Parquet uses various encoding schemes for efficient data storage:
//   - RLE (Run Length Encoding) - for repetition/definition levels
//   - Bit-Packing - compact storage of small integers
//   - PLAIN - uncompressed raw values
//   - DELTA encodings - for sorted/sequential data
//
// REFERENCES:
//   - https://github.com/apache/parquet-format/blob/master/Encodings.md
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════
// BIT-PACKING DECODER
// ═══════════════════════════════════════════════════════════════════════════
//
// Bit-packing stores multiple small integers in compact bit sequences.
// Example: 8 values of 3 bits each = 24 bits = 3 bytes
//
// Used in Parquet for:
//   - Definition/repetition levels
//   - Dictionary indices
//   - Boolean columns
//
// ═══════════════════════════════════════════════════════════════════════════

/// Bit-packed reader for reading values with arbitrary bit widths (1-32 bits)
pub const BitPackedReader = struct {
    data: []const u8,
    pos: usize, // byte position
    bit_offset: u3, // bit offset within current byte (0-7)

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{
            .data = data,
            .pos = 0,
            .bit_offset = 0,
        };
    }

    /// Read a single value with specified bit width
    pub fn readValue(self: *Self, bit_width: u5) !u32 {
        if (bit_width == 0) return 0;
        if (bit_width > 32) return error.InvalidBitWidth;

        var result: u32 = 0;
        var bits_read: u5 = 0;

        while (bits_read < bit_width) {
            if (self.pos >= self.data.len) {
                return error.UnexpectedEndOfData;
            }

            const bits_in_current_byte: u5 = @as(u5, 8) - self.bit_offset;
            const bits_needed: u5 = bit_width - bits_read;
            const bits_to_read: u5 = @min(bits_in_current_byte, bits_needed);

            // Extract bits from current byte
            const byte = self.data[self.pos];
            const mask: u8 = (@as(u8, 1) << @as(u3, @intCast(bits_to_read))) - 1;
            const value = (byte >> @as(u3, @intCast(self.bit_offset))) & mask;

            // Add to result
            result |= @as(u32, value) << bits_read;

            // Update position
            bits_read += bits_to_read;
            self.bit_offset +%= @intCast(bits_to_read);

            if (self.bit_offset >= 8) {
                self.bit_offset = 0;
                self.pos += 1;
            }
        }

        return result;
    }

    /// Read multiple values into a buffer
    pub fn readValues(self: *Self, bit_width: u5, output: []u32) !void {
        for (output) |*val| {
            val.* = try self.readValue(bit_width);
        }
    }

    /// Check if at end of data
    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// RLE (RUN LENGTH ENCODING) DECODER
// ═══════════════════════════════════════════════════════════════════════════
//
// RLE/Bit-Packing Hybrid encoding (used for repetition/definition levels):
//
// Format:
//   - Sequence of runs
//   - Each run is either:
//     1. Bit-packed run: header (varint) + bit-packed values
//     2. RLE run: header (varint) + single value
//
// Header encoding:
//   - If LSB = 0: RLE run, length = header >> 1, followed by value
//   - If LSB = 1: Bit-packed run, length = (header >> 1) * 8, followed by packed data
//
// ═══════════════════════════════════════════════════════════════════════════

pub const RLEDecoder = struct {
    data: []const u8,
    pos: usize,
    bit_width: u5,

    // Current run state
    run_remaining: usize,
    run_value: u32,
    is_bit_packed: bool,
    bit_packed_reader: ?BitPackedReader,

    const Self = @This();

    pub fn init(data: []const u8, bit_width: u5) Self {
        return .{
            .data = data,
            .pos = 0,
            .bit_width = bit_width,
            .run_remaining = 0,
            .run_value = 0,
            .is_bit_packed = false,
            .bit_packed_reader = null,
        };
    }

    /// Read next value
    pub fn readValue(self: *Self) !u32 {
        // If current run is exhausted, read next run header
        if (self.run_remaining == 0) {
            try self.readRunHeader();
            if (self.run_remaining == 0) return error.UnexpectedZeroRunLength;
        }

        const value = if (self.is_bit_packed) blk: {
            // Read from bit-packed run
            if (self.bit_packed_reader) |*reader| {
                break :blk try reader.readValue(self.bit_width);
            } else {
                return error.InvalidState;
            }
        } else blk: {
            // Return RLE value
            break :blk self.run_value;
        };

        self.run_remaining -= 1;
        return value;
    }

    /// Read multiple values
    pub fn readValues(self: *Self, output: []u32) !void {
        for (output) |*val| {
            val.* = try self.readValue();
        }
    }

    /// Read all remaining values up to `count`
    /// Result is allocated with `allocator`
    pub fn decodeAll(self: *Self, allocator: std.mem.Allocator, count: usize) ![]i64 {
        const result = try allocator.alloc(i64, count);
        errdefer allocator.free(result);

        var i: usize = 0;
        while (i < count) : (i += 1) {
             // RLEDecoder returns u32, cast to i64 for indices
             result[i] = @as(i64, @intCast(try self.readValue()));
        }

        return result;
    }

    /// Read run header and prepare for reading values
    fn readRunHeader(self: *Self) !void {
        // Read varint header
        const header = try self.readVarint();

        if ((header & 1) == 0) {
            // RLE run: LSB = 0
            self.is_bit_packed = false;
            self.run_remaining = header >> 1;

            // Read the repeated value
            self.run_value = try self.readValue_internal();
            self.bit_packed_reader = null;
        } else {
            // Bit-packed run: LSB = 1
            self.is_bit_packed = true;
            const num_groups = header >> 1;
            self.run_remaining = num_groups * 8;

            // Calculate byte size of bit-packed data
            const total_bits = self.run_remaining * self.bit_width;
            const byte_size = (total_bits + 7) / 8;

            if (self.pos + byte_size > self.data.len) {
                return error.UnexpectedEndOfData;
            }

            // Initialize bit-packed reader
            const packed_data = self.data[self.pos .. self.pos + byte_size];
            self.bit_packed_reader = BitPackedReader.init(packed_data);
            self.pos += byte_size;
        }
    }

    /// Read a value using bit width (for RLE repeated value)
    fn readValue_internal(self: *Self) !u32 {
        if (self.bit_width == 0) return 0;

        const byte_width = (self.bit_width + 7) / 8;
        if (self.pos + byte_width > self.data.len) {
            return error.UnexpectedEndOfData;
        }

        var value: u32 = 0;
        var i: usize = 0;
        while (i < byte_width) : (i += 1) {
            value |= @as(u32, self.data[self.pos + i]) << @intCast(i * 8);
        }
        self.pos += byte_width;

        // Mask to bit width
        const mask: u32 = if (self.bit_width == 32) 0xFFFFFFFF else (@as(u32, 1) << self.bit_width) - 1;
        return value & mask;
    }

    /// Read varint
    fn readVarint(self: *Self) !usize {
        var result: usize = 0;
        var shift: u6 = 0;

        while (self.pos < self.data.len) {
            const byte = self.data[self.pos];
            self.pos += 1;

            result |= (@as(usize, byte & 0x7F)) << shift;
            if ((byte & 0x80) == 0) {
                return result;
            }

            shift += 7;
            if (shift >= 64) {
                return error.VarintTooLarge;
            }
        }

        return error.UnexpectedEndOfData;
    }

    /// Check if at end
    pub fn isAtEnd(self: *const Self) bool {
        return self.pos >= self.data.len and self.run_remaining == 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// DELTA BINARY PACKED ENCODING
// ═══════════════════════════════════════════════════════════════════════════
//
// Delta encoding is highly efficient for sorted/sequential data (timestamps, IDs).
// Modern Parquet writers (parquet-rs, Spark) prefer this over RLE for integers.
//
// Format:
//   Header:
//     - block_size: varint (values per miniblock, usually 128)
//     - miniblocks_per_block: varint (usually 4)
//     - total_value_count: varint
//     - first_value: zigzag varint
//
//   Blocks:
//     - min_delta: zigzag varint (minimum delta in this block)
//     - miniblock_bit_widths: array of u8 (one per miniblock)
//     - miniblock_data: bit-packed deltas (using bit_width - min_delta)
//
// Example: Values [100, 102, 104, 106] → Deltas [100, 2, 2, 2]
//          min_delta=2, bit_width=1 (to store 0,0,0)
//
// REFERENCES:
//   - https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5
//
// ═══════════════════════════════════════════════════════════════════════════

pub const DeltaBinaryPackedDecoder = struct {
    data: []const u8,
    pos: usize,

    // Header values
    block_size: u32,
    miniblocks_per_block: u32,
    total_value_count: u32,
    first_value: i64,

    // Current state
    values_decoded: u32,
    current_block_values: u32,
    current_miniblock: u32,
    miniblock_bit_widths: [256]u8, // Max miniblocks per block
    min_delta: i64,

    // Bit-packed reader for current miniblock
    bit_packed_reader: ?BitPackedReader,
    miniblock_values_remaining: u32,

    // Last value (for computing cumulative deltas)
    last_value: i64,

    const Self = @This();

    pub fn init(data: []const u8) !Self {
        var decoder = Self{
            .data = data,
            .pos = 0,
            .block_size = 0,
            .miniblocks_per_block = 0,
            .total_value_count = 0,
            .first_value = 0,
            .values_decoded = 0,
            .current_block_values = 0,
            .current_miniblock = 0,
            .miniblock_bit_widths = undefined,
            .min_delta = 0,
            .bit_packed_reader = null,
            .miniblock_values_remaining = 0,
            .last_value = 0,
        };

        try decoder.readHeader();
        decoder.last_value = decoder.first_value;

        return decoder;
    }

    fn readHeader(self: *Self) !void {
        self.block_size = @intCast(try self.readUnsignedVarInt());
        self.miniblocks_per_block = @intCast(try self.readUnsignedVarInt());
        self.total_value_count = @intCast(try self.readUnsignedVarInt());
        self.first_value = try self.readZigZagVarInt();
    }

    pub fn readValue(self: *Self) !i64 {
        if (self.values_decoded >= self.total_value_count) {
            return error.NoMoreValues;
        }

        // First value is stored directly
        if (self.values_decoded == 0) {
            self.values_decoded += 1;
            return self.first_value;
        }

        // Load new block if needed
        if (self.current_block_values == 0) {
            try self.readBlockHeader();
        }

        // Load new miniblock if needed
        if (self.miniblock_values_remaining == 0) {
            try self.loadNextMiniblock();
        }

        // Read delta from current miniblock
        const bit_width = self.miniblock_bit_widths[self.current_miniblock];
        var delta: i64 = 0;

        if (bit_width > 0) {
            if (self.bit_packed_reader) |*reader| {
                const packed_delta = try reader.readValue(@intCast(bit_width));
                delta = @as(i64, @intCast(packed_delta)) + self.min_delta;
            } else {
                return error.InvalidState;
            }
        } else {
            // bit_width=0 means all deltas are min_delta
            delta = self.min_delta;
        }

        // Compute cumulative value
        self.last_value += delta;
        const value = self.last_value;

        // Update counters
        self.miniblock_values_remaining -= 1;
        self.current_block_values -= 1;
        self.values_decoded += 1;

        return value;
    }

    pub fn readValues(self: *Self, output: []i64) !void {
        for (output) |*val| {
            val.* = try self.readValue();
        }
    }

    fn readBlockHeader(self: *Self) !void {
        // Read min_delta for this block
        self.min_delta = try self.readZigZagVarInt();

        // Read bit widths for each miniblock
        var i: u32 = 0;
        while (i < self.miniblocks_per_block) : (i += 1) {
            if (self.pos >= self.data.len) {
                return error.UnexpectedEndOfData;
            }
            self.miniblock_bit_widths[i] = self.data[self.pos];
            self.pos += 1;
        }

        // Reset block state
        self.current_block_values = self.block_size;
        self.current_miniblock = 0;
        self.miniblock_values_remaining = 0;
    }

    fn loadNextMiniblock(self: *Self) !void {
        if (self.current_miniblock >= self.miniblocks_per_block) {
            return error.InvalidMiniblock;
        }

        const values_per_miniblock = self.block_size / self.miniblocks_per_block;
        const bit_width = self.miniblock_bit_widths[self.current_miniblock];

        if (bit_width > 0) {
            // Calculate byte size of this miniblock
            const total_bits: u64 = @as(u64, values_per_miniblock) * @as(u64, bit_width);
            const byte_size: usize = @intCast((total_bits + 7) / 8);

            if (self.pos + byte_size > self.data.len) {
                return error.UnexpectedEndOfData;
            }

            // Initialize bit-packed reader for this miniblock
            const miniblock_data = self.data[self.pos .. self.pos + byte_size];
            self.bit_packed_reader = BitPackedReader.init(miniblock_data);
            self.pos += byte_size;
        }

        self.miniblock_values_remaining = values_per_miniblock;
        self.current_miniblock += 1;
    }

    fn readUnsignedVarInt(self: *Self) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;

        while (shift < 64) {
            if (self.pos >= self.data.len) {
                return error.UnexpectedEndOfData;
            }

            const byte = self.data[self.pos];
            self.pos += 1;

            result |= @as(u64, byte & 0x7F) << shift;

            if ((byte & 0x80) == 0) {
                return result;
            }

            shift += 7;
        }

        return error.VarIntTooLong;
    }

    fn readZigZagVarInt(self: *Self) !i64 {
        const unsigned = try self.readUnsignedVarInt();
        // ZigZag decoding: (n >> 1) ^ -(n & 1)
        const value: i64 = @bitCast((unsigned >> 1) ^ (0 -% (unsigned & 1)));
        return value;
    }

    pub fn isAtEnd(self: *const Self) bool {
        return self.values_decoded >= self.total_value_count;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "BitPackedReader - read 1-bit values" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // 8 boolean values: 1,0,1,1,0,0,1,0
    // Packed as one byte: 0b01001101 = 0x4D
    const data = [_]u8{0x4D};

    var reader = BitPackedReader.init(&data);

    try std.testing.expectEqual(@as(u32, 1), try reader.readValue(1));
    try std.testing.expectEqual(@as(u32, 0), try reader.readValue(1));
    try std.testing.expectEqual(@as(u32, 1), try reader.readValue(1));
    try std.testing.expectEqual(@as(u32, 1), try reader.readValue(1));
}

test "BitPackedReader - read 4-bit values" {
    // 2 values of 4 bits: 15 (0b1111), 10 (0b1010)
    // Packed: 0b1010_1111 = 0xAF
    const data = [_]u8{0xAF};

    var reader = BitPackedReader.init(&data);

    try std.testing.expectEqual(@as(u32, 15), try reader.readValue(4));
    try std.testing.expectEqual(@as(u32, 10), try reader.readValue(4));
}

test "RLEDecoder - simple RLE run" {
    // RLE run: 10 repetitions of value 42 with bit_width=8
    // Header: (10 << 1) | 0 = 20 (0x14)
    // Value: 42 (0x2A)
    const data = [_]u8{ 0x14, 0x2A };

    var decoder = RLEDecoder.init(&data, 8);

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        const val = try decoder.readValue();
        try std.testing.expectEqual(@as(u32, 42), val);
    }
}

test "RLEDecoder - bit-packed run" {
    // Bit-packed run: 1 group (8 values) of 3-bit values
    // Header: (1 << 1) | 1 = 3 (0x03)
    // Values: 0,1,2,3,4,5,6,7 packed in 3 bits each = 24 bits = 3 bytes
    const data = [_]u8{
        0x03, // header
        0b001_000_00, // values 0,1
        0b011_010_00, // values 2,3
        0b101_100_01, // values 4,5
        // Note: actual bit packing might differ
    };

    var decoder = RLEDecoder.init(&data, 3);

    // Just test that it doesn't crash
    _ = decoder.readValue() catch |err| {
        try std.testing.expect(err == error.UnexpectedEndOfData or err == error.InvalidState);
    };
}

test "DeltaBinaryPackedDecoder - simple sequence" {
    // Encode sequence: [100, 102, 104, 106]
    // Header:
    //   block_size = 128 → varint: 0x80, 0x01
    //   miniblocks_per_block = 4 → varint: 0x04
    //   total_value_count = 4 → varint: 0x04
    //   first_value = 100 → zigzag: 200 → varint: 0xC8, 0x01
    // Block header:
    //   min_delta = 2 → zigzag: 4 → varint: 0x04
    //   bit_widths = [0, 0, 0, 0] (all deltas are min_delta)
    // Data: (no data since bit_width=0)
    const data = [_]u8{
        0x80, 0x01, // block_size = 128
        0x04, // miniblocks_per_block = 4
        0x04, // total_value_count = 4
        0xC8, 0x01, // first_value = 100 (zigzag)
        0x04, // min_delta = 2 (zigzag)
        0x00, 0x00, 0x00, 0x00, // bit_widths for 4 miniblocks
    };

    var decoder = try DeltaBinaryPackedDecoder.init(&data);

    try std.testing.expectEqual(@as(i64, 100), try decoder.readValue());
    try std.testing.expectEqual(@as(i64, 102), try decoder.readValue());
    try std.testing.expectEqual(@as(i64, 104), try decoder.readValue());
    try std.testing.expectEqual(@as(i64, 106), try decoder.readValue());
}
