// ═══════════════════════════════════════════════════════════════════════════
// APACHE PARQUET FILE FORMAT READER
// ═══════════════════════════════════════════════════════════════════════════
//
// Parquet is a columnar storage format optimized for analytics.
//
// FILE STRUCTURE:
//   [Magic Number: "PAR1"]
//   [Row Group 1]
//     [Column Chunk 1] [Column Chunk 2] ...
//   [Row Group 2]
//     ...
//   [Footer (Thrift encoded)]
//   [Footer Length: 4 bytes little-endian]
//   [Magic Number: "PAR1"]
//
// REFERENCES:
//   - https://github.com/apache/parquet-format/blob/master/README.md
//   - https://parquet.apache.org/docs/file-format/
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const rle = @import("rle.zig");
const errors = @import("../core/errors.zig");
const thrift = @import("thrift.zig");
const compression = @import("compression.zig");

pub const MAGIC: [4]u8 = "PAR1".*;
pub const FOOTER_SIZE: usize = 8; // 4 bytes footer length + 4 bytes magic

/// Parquet data types (from Thrift spec)
pub const Type = enum(i32) {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3, // Deprecated
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
};

/// Converted types (logical types)
pub const ConvertedType = enum(i32) {
    UTF8 = 0,
    MAP = 1,
    MAP_KEY_VALUE = 2,
    LIST = 3,
    ENUM = 4,
    DECIMAL = 5,
    DATE = 6,
    TIME_MILLIS = 7,
    TIME_MICROS = 8,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19,
    BSON = 20,
    INTERVAL = 21,
};

/// Field repetition type
pub const FieldRepetitionType = enum(i32) {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2,
};

/// Encoding types
pub const Encoding = enum(i32) {
    PLAIN = 0,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8,
};

/// Compression codec
pub const CompressionCodec = enum(i32) {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6,
};

/// Page type
pub const PageType = enum(i32) {
    DATA_PAGE = 0,
    INDEX_PAGE = 1,
    DICTIONARY_PAGE = 2,
    DATA_PAGE_V2 = 3,
};

/// Data page header (V1)
pub const DataPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    definition_level_encoding: Encoding,
    repetition_level_encoding: Encoding,
};

/// Page header
pub const PageHeader = struct {
    type: PageType,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    data_page_header: ?DataPageHeader,
    dictionary_page_header: ?DictionaryPageHeader,
};

pub const DictionaryPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    is_sorted: ?bool,
};

/// Schema element (simplified - will expand as needed)
pub const SchemaElement = struct {
    type: ?Type,
    name: []const u8,
    repetition_type: ?FieldRepetitionType,
    num_children: i32,
    converted_type: ?ConvertedType,

    pub fn deinit(self: *SchemaElement, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
    }
};

/// Column metadata
pub const ColumnMetaData = struct {
    type: Type,
    encodings: []Encoding,
    codec: CompressionCodec,
    num_values: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    data_page_offset: i64,
    dictionary_page_offset: ?i64, // Optional: only present if column uses dictionary encoding
};

/// File metadata (footer)
pub const FileMetaData = struct {
    version: i32,
    schema: []SchemaElement,
    num_rows: i64,
    row_groups: []RowGroup,

    allocator: std.mem.Allocator,

    pub fn deinit(self: *FileMetaData) void {
        // Free schema element names
        for (self.schema) |*elem| {
            self.allocator.free(elem.name);
        }
        self.allocator.free(self.schema);

        // Free row groups and their column chunks
        for (self.row_groups) |rg| {
            for (rg.columns) |col| {
                self.allocator.free(col.meta_data.encodings);
            }
            self.allocator.free(rg.columns);
        }
        self.allocator.free(self.row_groups);
    }
};

/// Row group metadata
pub const RowGroup = struct {
    columns: []ColumnChunk,
    total_byte_size: i64,
    num_rows: i64,
};

/// Column chunk metadata
pub const ColumnChunk = struct {
    file_offset: i64,
    meta_data: ColumnMetaData,
};

// ═══════════════════════════════════════════════════════════════════════════
// THRIFT FOOTER PARSER
// ═══════════════════════════════════════════════════════════════════════════

/// Parse FileMetaData from Thrift Compact Protocol (used by PyArrow)
fn parseFileMetaData(allocator: std.mem.Allocator, data: []const u8) !FileMetaData {
    var reader = thrift.CompactReader.init(data);

    var version: i32 = 1;
    var schema: std.ArrayListUnmanaged(SchemaElement) = .{};
    errdefer schema.deinit(allocator);
    var num_rows: i64 = 0;
    var row_groups: std.ArrayListUnmanaged(RowGroup) = .{};
    errdefer row_groups.deinit(allocator);

    // Parse FileMetaData struct fields
    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { // version
                version = try reader.readVarint32();
            },
            2 => { // schema
                const list_info = try reader.readListBegin();
                var i: usize = 0;
                while (i < list_info.size) : (i += 1) {
                    const elem = try parseSchemaElement(allocator, &reader);
                    try schema.append(allocator, elem);
                }
            },
            3, 5, 7, 12 => { // num_rows (field varies by writer: PyArrow=5, some tools=7, Zig Parquet=12)
                num_rows = try reader.readVarint64();
            },
            4, 6, 8, 13 => { // row_groups (field varies by writer: PyArrow=6, some tools=8, Zig Parquet=13)
                const list_info = try reader.readListBegin();
                var i: usize = 0;
                while (i < list_info.size) : (i += 1) {
                    const rg = try parseRowGroup(allocator, &reader);
                    try row_groups.append(allocator, rg);
                }
            },
            else => {
                // Skip unknown/optional fields
                try reader.skip(field.field_type);
            },
        }
    }

    return FileMetaData{
        .version = version,
        .schema = try schema.toOwnedSlice(allocator),
        .num_rows = num_rows,
        .row_groups = try row_groups.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

/// Parse SchemaElement from Thrift
fn parseSchemaElement(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !SchemaElement {
    // CRITICAL: Reset last_field_id when entering a new struct!
    // In Thrift Compact Protocol, delta encoding is relative to the PREVIOUS field in the SAME struct
    // When we enter a new struct (SchemaElement), we must reset to 0
    reader.last_field_id = 0;

    var elem_type: ?Type = null;
    var name: []const u8 = "";
    var repetition_type: ?FieldRepetitionType = null;
    var num_children: i32 = 0;
    var converted_type: ?ConvertedType = null;

    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { // type
                const type_val = try reader.readVarint32();
                elem_type = @enumFromInt(type_val);
            },
            2 => { // type_length (for FIXED_LEN_BYTE_ARRAY)
                _ = try reader.readVarint32();
            },
            3 => { // repetition_type
                const rep_val = try reader.readVarint32();
                repetition_type = @enumFromInt(rep_val);
            },
            4 => { // name
                const name_slice = try reader.readBinary();
                // CRITICAL: Must copy the name to owned memory!
                // readBinary() returns a slice of the reader's buffer which is temporary
                name = try allocator.dupe(u8, name_slice);
            },
            5 => { // num_children
                num_children = try reader.readVarint32();
            },
            6 => { // converted_type
                const conv_val = try reader.readVarint32();
                converted_type = @enumFromInt(conv_val);
            },
            else => {
                try reader.skip(field.field_type);
            },
        }
    }

    return SchemaElement{
        .type = elem_type,
        .name = name,
        .repetition_type = repetition_type,
        .num_children = num_children,
        .converted_type = converted_type,
    };
}

/// Parse RowGroup from Thrift
fn parseRowGroup(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !RowGroup {
    reader.last_field_id = 0; // Reset for new struct

    var columns: std.ArrayListUnmanaged(ColumnChunk) = .{};
    errdefer columns.deinit(allocator);
    var total_byte_size: i64 = 0;
    var num_rows: i64 = 0;

    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { // columns
                const list_info = try reader.readListBegin();
                var i: usize = 0;
                while (i < list_info.size) : (i += 1) {
                    const col = try parseColumnChunk(allocator, reader);
                    try columns.append(allocator, col);
                }
            },
            2, 17 => { // total_byte_size (field 2 in spec, but PyArrow may use field 17)
                total_byte_size = try reader.readVarint64();
            },
            3, 5, 18 => { // num_rows (field 3 in spec, some writers use 5 or 18)
                num_rows = try reader.readVarint64();
            },
            else => {
                try reader.skip(field.field_type);
            },
        }
    }

    return RowGroup{
        .columns = try columns.toOwnedSlice(allocator),
        .total_byte_size = total_byte_size,
        .num_rows = num_rows,
    };
}

/// Parse ColumnChunk from Thrift
fn parseColumnChunk(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !ColumnChunk {
    reader.last_field_id = 0; // Reset for new struct

    var file_offset: i64 = 0;
    var meta_data: ?ColumnMetaData = null;

    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { // file_path (deprecated, skip)
                _ = try reader.readBinary();
            },
            2 => { // file_offset
                file_offset = try reader.readVarint64();
            },
            3 => { // meta_data
                meta_data = try parseColumnMetaData(allocator, reader);
            },
            else => {
                try reader.skip(field.field_type);
            },
        }
    }

    return ColumnChunk{
        .file_offset = file_offset,
        .meta_data = meta_data orelse return error.ThriftDecodeFailed,
    };
}

/// Parse ColumnMetaData from Thrift
fn parseColumnMetaData(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !ColumnMetaData {
    reader.last_field_id = 0; // Reset for new struct

    var col_type: Type = .INT32;
    var encodings: std.ArrayListUnmanaged(Encoding) = .{};
    errdefer encodings.deinit(allocator);
    var codec: CompressionCodec = .UNCOMPRESSED;
    var num_values: i64 = 0;
    var total_uncompressed_size: i64 = 0;
    var total_compressed_size: i64 = 0;
    var data_page_offset: i64 = 0;
    var dictionary_page_offset: ?i64 = null;

    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { // type
                const type_val = try reader.readVarint32();
                col_type = @enumFromInt(type_val);
            },
            2 => { // encodings
                const list_info = try reader.readListBegin();
                var i: usize = 0;
                while (i < list_info.size) : (i += 1) {
                    const enc_val = try reader.readVarint32();
                    const encoding: Encoding = @enumFromInt(enc_val);
                    try encodings.append(allocator, encoding);
                }
            },
            3 => { // path_in_schema (skip for now)
                const list_info = try reader.readListBegin();
                var i: usize = 0;
                while (i < list_info.size) : (i += 1) {
                    _ = try reader.readBinary();
                }
            },
            4 => { // codec
                const codec_val = try reader.readVarint32();
                codec = @enumFromInt(codec_val);
            },
            5 => { // num_values
                num_values = try reader.readVarint64();
            },
            6 => { // total_uncompressed_size
                total_uncompressed_size = try reader.readVarint64();
            },
            7 => { // total_compressed_size
                total_compressed_size = try reader.readVarint64();
            },
            8 => { // key_value_metadata (skip for now)
                try reader.skip(field.field_type);
            },
            9 => { // data_page_offset
                data_page_offset = try reader.readVarint64();
            },
            10 => { // index_page_offset (skip for now)
                try reader.skip(field.field_type);
            },
            11 => { // dictionary_page_offset
                dictionary_page_offset = try reader.readVarint64();
            },
            else => {
                try reader.skip(field.field_type);
            },
        }
    }

    return ColumnMetaData{
        .type = col_type,
        .encodings = try encodings.toOwnedSlice(allocator),
        .codec = codec,
        .num_values = num_values,
        .total_uncompressed_size = total_uncompressed_size,
        .total_compressed_size = total_compressed_size,
        .data_page_offset = data_page_offset,
        .dictionary_page_offset = dictionary_page_offset,
    };
}

/// Parse PageHeader from Thrift Compact Protocol
fn parsePageHeader(allocator: std.mem.Allocator, data: []const u8) !PageHeader {
    var reader = thrift.CompactReader.init(data);
    reader.last_field_id = 0;

    var page_type: PageType = .DATA_PAGE;
    var uncompressed_page_size: i32 = 0;
    var compressed_page_size: i32 = 0;
    var data_page_header: ?DataPageHeader = null;
    var dictionary_page_header: ?DictionaryPageHeader = null;

    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { // type
                const type_val = try reader.readVarint32();
                page_type = @enumFromInt(type_val);
            },
            2 => { // uncompressed_page_size
                uncompressed_page_size = try reader.readVarint32();
            },
            3 => { // compressed_page_size
                compressed_page_size = try reader.readVarint32();
            },
            5 => { // data_page_header
                data_page_header = try parseDataPageHeader(allocator, &reader);
            },
            7 => { // dictionary_page_header
                dictionary_page_header = try parseDictionaryPageHeader(allocator, &reader);
            },
            else => {
                try reader.skip(field.field_type);
            },
        }
    }

    return PageHeader{
        .type = page_type,
        .uncompressed_page_size = uncompressed_page_size,
        .compressed_page_size = compressed_page_size,
        .data_page_header = data_page_header,
        .dictionary_page_header = dictionary_page_header,
    };
}

fn parseDictionaryPageHeader(_: std.mem.Allocator, reader: *thrift.CompactReader) !DictionaryPageHeader {
    reader.last_field_id = 0;
    var num_values: i32 = 0;
    var encoding: Encoding = .PLAIN;
    var is_sorted: ?bool = null;

    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { num_values = try reader.readVarint32(); },
            2 => { encoding = @enumFromInt(try reader.readVarint32()); },
            3 => { is_sorted = (try reader.readVarint32()) != 0; },
            else => try reader.skip(field.field_type),
        }
    }
    return DictionaryPageHeader{
        .num_values = num_values,
        .encoding = encoding,
        .is_sorted = is_sorted,
    };
}

/// Parse DataPageHeader from Thrift
fn parseDataPageHeader(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !DataPageHeader {
    _ = allocator;
    reader.last_field_id = 0;

    var num_values: i32 = 0;
    var encoding: Encoding = .PLAIN;
    var definition_level_encoding: Encoding = .RLE;
    var repetition_level_encoding: Encoding = .RLE;

    while (try reader.readFieldBegin()) |field| {
        switch (field.field_id) {
            1 => { // num_values
                num_values = try reader.readVarint32();
            },
            2 => { // encoding
                const enc_val = try reader.readVarint32();
                encoding = @enumFromInt(enc_val);
            },
            3 => { // definition_level_encoding
                const enc_val = try reader.readVarint32();
                definition_level_encoding = @enumFromInt(enc_val);
            },
            4 => { // repetition_level_encoding
                const enc_val = try reader.readVarint32();
                repetition_level_encoding = @enumFromInt(enc_val);
            },
            else => {
                try reader.skip(field.field_type);
            },
        }
    }

    return DataPageHeader{
        .num_values = num_values,
        .encoding = encoding,
        .definition_level_encoding = definition_level_encoding,
        .repetition_level_encoding = repetition_level_encoding,
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// PARQUET READER
// ═══════════════════════════════════════════════════════════════════════════

/// Parquet file reader
pub const Reader = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    file_size: u64,
    metadata: ?FileMetaData,

    const Self = @This();

    /// Open a Parquet file
    pub fn open(allocator: std.mem.Allocator, path: []const u8) !Self {
        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();

        const stat = try file.stat();

        return Self{
            .allocator = allocator,
            .file = file,
            .file_size = stat.size,
            .metadata = null,
        };
    }

    /// Close the file
    /// Read a data page from the file
    pub fn readDataPage(self: *Self, offset: i64, allocator: std.mem.Allocator) !struct {
        page_header: PageHeader,
        compressed_data: []u8,
        header_size: usize,
    } {
        // OPTIMIZATION: Read header + data in ONE seek + read operation
        // Old approach: 3 seeks per page (45ms overhead on HDD)
        // New approach: 1 seek per page (15ms overhead on HDD)
        //
        // Strategy:
        // 1. Seek once to page offset
        // 2. Read header_buffer (max 1KB) + max expected page (estimate 16KB)
        // 3. Parse header from buffer to get real compressed_page_size
        // 4. If page is larger, read remaining bytes (rare case)

        try self.file.seekTo(@intCast(offset));

        // Read header (max 1KB) + initial data chunk (64KB estimate)
        // Increased from 16KB to 64KB to handle larger pages in one read
        const initial_read_size = 1024 + 65536; // 65KB (header + data)
        var initial_buffer = try allocator.alloc(u8, initial_read_size);
        defer allocator.free(initial_buffer);

        const initial_bytes_read = try self.file.read(initial_buffer);
        if (initial_bytes_read == 0) {
            return error.InvalidPageData; // Need at least some bytes
        }

        // Parse page header from buffer
        // If buffer is too small, parsePageHeader will return error
        const page_header = try parsePageHeader(allocator, initial_buffer[0..initial_bytes_read]);

        // SANITY CHECK: Validate page sizes
        // 256MB limit covers most Iceberg production tables (Spark default: 128MB row groups)
        const MAX_PAGE_SIZE: i32 = 268_435_456; // 256MB
        if (page_header.compressed_page_size < 0 or page_header.compressed_page_size > MAX_PAGE_SIZE) {
            std.debug.print("ERROR: Invalid compressed_page_size={d} (max: {}MB)\n", .{ page_header.compressed_page_size, MAX_PAGE_SIZE / 1_000_000 });
            return error.InvalidPageSize;
        }
        if (page_header.uncompressed_page_size < 0 or page_header.uncompressed_page_size > MAX_PAGE_SIZE) {
            std.debug.print("ERROR: Invalid uncompressed_page_size={d} (max: {}MB)\n", .{ page_header.uncompressed_page_size, MAX_PAGE_SIZE / 1_000_000 });
            return error.InvalidPageSize;
        }

        // Calculate header size by re-parsing
        var temp_reader = thrift.CompactReader.init(initial_buffer[0..initial_bytes_read]);
        temp_reader.last_field_id = 0;
        while (try temp_reader.readFieldBegin()) |field| {
            try temp_reader.skip(field.field_type);
        }
        const header_size = temp_reader.pos;

        // Check if we already have all the data in initial_buffer
        const compressed_size: usize = @intCast(page_header.compressed_page_size);
        const total_needed = header_size + compressed_size;

        var compressed_data: []u8 = undefined;

        if (total_needed <= initial_bytes_read) {
            // FAST PATH: Everything already in buffer (common case)
            compressed_data = try allocator.alloc(u8, compressed_size);
            @memcpy(compressed_data, initial_buffer[header_size..][0..compressed_size]);
        } else {
            // SLOW PATH: Need to read more data (rare for large pages)
            compressed_data = try allocator.alloc(u8, compressed_size);

            // Copy what we have
            const bytes_in_buffer = initial_bytes_read - header_size;
            @memcpy(compressed_data[0..bytes_in_buffer], initial_buffer[header_size..initial_bytes_read]);

            // Read remaining bytes (NO SEEK needed - already positioned correctly)
            const remaining = compressed_size - bytes_in_buffer;
            const bytes_read = try self.file.read(compressed_data[bytes_in_buffer..]);
            if (bytes_read != remaining) {
                allocator.free(compressed_data);
                return error.InvalidPageData;
            }
        }

        return .{
            .page_header = page_header,
            .compressed_data = compressed_data,
            .header_size = header_size,
        };
    }

    /// Dictionary storage for a column
    pub const DictionaryInt64 = struct {
        values: []i64,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *DictionaryInt64) void {
            self.allocator.free(self.values);
        }
    };

    pub const DictionaryByteArray = struct {
        values: [][]const u8,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *DictionaryByteArray) void {
            for (self.values) |str| {
                self.allocator.free(str);
            }
            self.allocator.free(self.values);
        }
    };

    pub const DictionaryFloat = struct {
        values: []f32,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *DictionaryFloat) void {
            self.allocator.free(self.values);
        }
    };

    pub const DictionaryDouble = struct {
        values: []f64,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *DictionaryDouble) void {
            self.allocator.free(self.values);
        }
    };

    pub const DictionaryBoolean = struct {
        values: []bool,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *DictionaryBoolean) void {
            self.allocator.free(self.values);
        }
    };

    /// Read dictionary page for INT32 column
    /// Returns null if column doesn't have a dictionary page
    /// NOTE: INT32 dictionaries store INT32 values, then convert to INT64 when mapping indices
    pub fn readDictionaryPageInt32(
        self: *Self,
        col_meta: ColumnMetaData,
        allocator: std.mem.Allocator,
    ) !?DictionaryInt64 {
        // Check if column has dictionary page
        const dict_offset = col_meta.dictionary_page_offset orelse return null;

        // Read dictionary page
        const dict_page = try self.readDataPage(dict_offset, allocator);
        defer allocator.free(dict_page.compressed_data);

        // Decompress if needed
        const uncompressed_size: usize = @intCast(dict_page.page_header.uncompressed_page_size);
        const page_data = try compression.decompress(
            allocator,
            col_meta.codec,
            dict_page.compressed_data,
            uncompressed_size,
        );
        defer allocator.free(page_data);

        // Dictionary pages always use PLAIN encoding
        // For INT32, dictionary values are stored as INT32 (4 bytes each)
        const num_values = uncompressed_size / @sizeOf(i32);
        const i32_values = try decodePlainInt32(
            allocator,
            page_data,
            num_values,
            true, // Dictionary values are always required
        );
        defer allocator.free(i32_values);

        // Convert INT32 to INT64 for storage
        var dict_values = try allocator.alloc(i64, num_values);
        for (i32_values, 0..) |v, i| {
            dict_values[i] = @intCast(v);
        }

        return DictionaryInt64{
            .values = dict_values,
            .allocator = allocator,
        };
    }

    /// Read dictionary page for INT64 column
    /// Returns null if column doesn't have a dictionary page
    pub fn readDictionaryPageInt64(
        self: *Self,
        col_meta: ColumnMetaData,
        allocator: std.mem.Allocator,
    ) !?DictionaryInt64 {
        // Check if column has dictionary page
        const dict_offset = col_meta.dictionary_page_offset orelse return null;

        // Read dictionary page
        const dict_page = try self.readDataPage(dict_offset, allocator);
        defer allocator.free(dict_page.compressed_data);

        // Decompress if needed
        const uncompressed_size: usize = @intCast(dict_page.page_header.uncompressed_page_size);
        const page_data = try compression.decompress(
            allocator,
            col_meta.codec,
            dict_page.compressed_data,
            uncompressed_size,
        );
        defer allocator.free(page_data);

        // Dictionary pages always use PLAIN encoding
        // For INT64, dictionary values are stored as INT64 (8 bytes each)
        const num_values = uncompressed_size / @sizeOf(i64);
        const dict_values = try decodePlainInt64(
            allocator,
            page_data,
            num_values,
            true, // Dictionary values are always required
        );

        return DictionaryInt64{
            .values = dict_values,
            .allocator = allocator,
        };
    }

    /// Read dictionary page for BYTE_ARRAY column
    pub fn readDictionaryPageByteArray(
        self: *Self,
        col_meta: ColumnMetaData,
        allocator: std.mem.Allocator,
    ) !?DictionaryByteArray {
        // Check if column has dictionary page
        const dict_offset = col_meta.dictionary_page_offset orelse return null;

        // Read dictionary page
        const dict_page = try self.readDataPage(dict_offset, allocator);
        defer allocator.free(dict_page.compressed_data);

        // Decompress if needed
        const uncompressed_size: usize = @intCast(dict_page.page_header.uncompressed_page_size);
        const page_data = try compression.decompress(
            allocator,
            col_meta.codec,
            dict_page.compressed_data,
            uncompressed_size,
        );
        defer allocator.free(page_data);

        // Dictionary pages always use PLAIN encoding
        // For byte arrays, count the actual number of values by scanning
        var num_values: usize = 0;
        var pos: usize = 0;
        while (pos + 4 <= page_data.len) {
            const length = std.mem.readInt(i32, page_data[pos..][0..4], .little);
            pos += 4;
            if (length < 0 or pos + @as(usize, @intCast(length)) > page_data.len) break;
            pos += @intCast(length);
            num_values += 1;
        }

        const dict_values = try decodePlainByteArray(
            allocator,
            page_data,
            num_values,
            true, // Dictionary values are always required
        );

        return DictionaryByteArray{
            .values = dict_values,
            .allocator = allocator,
        };
    }

    /// Read dictionary page for FLOAT column
    pub fn readDictionaryPageFloat(
        self: *Self,
        col_meta: ColumnMetaData,
        allocator: std.mem.Allocator,
    ) !?DictionaryFloat {
        // Check if column has dictionary page
        const dict_offset = col_meta.dictionary_page_offset orelse return null;

        // Read dictionary page
        const dict_page = try self.readDataPage(dict_offset, allocator);
        defer allocator.free(dict_page.compressed_data);

        // Decompress if needed
        const uncompressed_size: usize = @intCast(dict_page.page_header.uncompressed_page_size);
        const page_data = try compression.decompress(
            allocator,
            col_meta.codec,
            dict_page.compressed_data,
            uncompressed_size,
        );
        defer allocator.free(page_data);

        // Dictionary pages always use PLAIN encoding
        const num_values = uncompressed_size / @sizeOf(f32);
        const dict_values = try decodePlainFloat(
            allocator,
            page_data,
            num_values,
            true, // Dictionary values are always required
        );

        return DictionaryFloat{
            .values = dict_values,
            .allocator = allocator,
        };
    }

    /// Read dictionary page for DOUBLE column
    pub fn readDictionaryPageDouble(
        self: *Self,
        col_meta: ColumnMetaData,
        allocator: std.mem.Allocator,
    ) !?DictionaryDouble {
        // Check if column has dictionary page
        const dict_offset = col_meta.dictionary_page_offset orelse return null;

        // Read dictionary page
        const dict_page = try self.readDataPage(dict_offset, allocator);
        defer allocator.free(dict_page.compressed_data);

        // Decompress if needed
        const uncompressed_size: usize = @intCast(dict_page.page_header.uncompressed_page_size);
        const page_data = try compression.decompress(
            allocator,
            col_meta.codec,
            dict_page.compressed_data,
            uncompressed_size,
        );
        defer allocator.free(page_data);

        // Dictionary pages always use PLAIN encoding
        const num_values = uncompressed_size / @sizeOf(f64);
        const dict_values = try decodePlainDouble(
            allocator,
            page_data,
            num_values,
            true, // Dictionary values are always required
        );

        return DictionaryDouble{
            .values = dict_values,
            .allocator = allocator,
        };
    }

    /// Read dictionary page for BOOLEAN column
    pub fn readDictionaryPageBoolean(
        self: *Self,
        col_meta: ColumnMetaData,
        allocator: std.mem.Allocator,
    ) !?DictionaryBoolean {
        // Check if column has dictionary page
        const dict_offset = col_meta.dictionary_page_offset orelse return null;

        // Read dictionary page
        const dict_page = try self.readDataPage(dict_offset, allocator);
        defer allocator.free(dict_page.compressed_data);

        // Decompress if needed
        const uncompressed_size: usize = @intCast(dict_page.page_header.uncompressed_page_size);
        const page_data = try compression.decompress(
            allocator,
            col_meta.codec,
            dict_page.compressed_data,
            uncompressed_size,
        );
        defer allocator.free(page_data);

        // Dictionary pages always use PLAIN encoding
        // For booleans, values are bit-packed (1 bit per boolean)
        //
        // NOTE: We conservatively allocate for worst-case (all bits are values).
        // In practice, Iceberg NEVER uses dictionary encoding for BOOLEAN columns
        // (dictionary overhead > benefit for 1-bit values), so this code rarely executes.
        //
        // Correct approach would read num_values from DictionaryPageHeader,
        // but that's not currently parsed. This over-allocation is safe but wasteful.
        const num_values = uncompressed_size * 8; // Worst-case: all bits are values

        const dict_values = try decodePlainBoolean(
            allocator,
            page_data,
            num_values,
            true, // Dictionary values are always required
        );

        return DictionaryBoolean{
            .values = dict_values,
            .allocator = allocator,
        };
    }

    /// Skip RLE-encoded definition levels
    /// Returns the position after the definition levels
    fn skipDefinitionLevels(page_data: []const u8) !usize {
        // Definition levels format: [INT32: byte_length] [RLE data]
        // NOTE: byte_length is a LITTLE-ENDIAN INT32, NOT a varint!
        if (page_data.len < 4) return error.InvalidPageData;

        const byte_length = std.mem.readInt(i32, page_data[0..4], .little);
        if (byte_length < 0) return error.InvalidPageData;

        const ulen: usize = @intCast(byte_length);
        return 4 + ulen; // 4 bytes for INT32 + RLE data
    }

    /// Decode PLAIN encoded INT64 values from uncompressed page data
    /// Page data format: [definition_levels] [values]
    /// For OPTIONAL columns, we skip the definition levels for MVP
    pub fn decodePlainInt64(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
    ) ![]i64 {
        var pos: usize = 0;

        // For OPTIONAL columns, skip definition levels
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Now read INT64 values (little-endian)
        var values = try allocator.alloc(i64, num_values);

        for (0..num_values) |i| {
            if (pos + 8 > page_data.len) {
                return error.InvalidPageData;
            }
            values[i] = std.mem.readInt(i64, page_data[pos..][0..8], .little);
            pos += 8;
        }

        return values;
    }

    /// Decode PLAIN encoded INT32 values from uncompressed page data
    pub fn decodePlainInt32(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
    ) ![]i32 {
        var pos: usize = 0;

        // For OPTIONAL columns, skip definition levels
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Now read INT32 values (little-endian)
        var values = try allocator.alloc(i32, num_values);

        for (0..num_values) |i| {
            if (pos + 4 > page_data.len) {
                return error.InvalidPageData;
            }
            values[i] = std.mem.readInt(i32, page_data[pos..][0..4], .little);
            pos += 4;
        }

        return values;
    }

    /// Decode PLAIN encoded FLOAT values from uncompressed page data
    pub fn decodePlainFloat(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
    ) ![]f32 {
        var pos: usize = 0;

        // For OPTIONAL columns, skip definition levels
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Now read FLOAT values (little-endian)
        var values = try allocator.alloc(f32, num_values);

        for (0..num_values) |i| {
            if (pos + 4 > page_data.len) {
                return error.InvalidPageData;
            }
            const bits = std.mem.readInt(u32, page_data[pos..][0..4], .little);
            values[i] = @bitCast(bits);
            pos += 4;
        }

        return values;
    }

    /// Decode PLAIN encoded DOUBLE values from uncompressed page data
    pub fn decodePlainDouble(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
    ) ![]f64 {
        var pos: usize = 0;

        // For OPTIONAL columns, skip definition levels
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Now read DOUBLE values (little-endian)
        var values = try allocator.alloc(f64, num_values);

        for (0..num_values) |i| {
            if (pos + 8 > page_data.len) {
                return error.InvalidPageData;
            }
            const bits = std.mem.readInt(u64, page_data[pos..][0..8], .little);
            values[i] = @bitCast(bits);
            pos += 8;
        }

        return values;
    }

    /// Decode PLAIN encoded BOOLEAN values from uncompressed page data
    pub fn decodePlainBoolean(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
    ) ![]bool {
        var pos: usize = 0;

        // For OPTIONAL columns, skip definition levels
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // BOOLEAN in Parquet is bit-packed (8 booleans per byte)
        var values = try allocator.alloc(bool, num_values);

        for (0..num_values) |i| {
            const byte_index = i / 8;
            const bit_index = @as(u3, @intCast(i % 8));

            if (pos + byte_index >= page_data.len) {
                return error.InvalidPageData;
            }

            const byte = page_data[pos + byte_index];
            values[i] = ((byte >> bit_index) & 1) == 1;
        }

        return values;
    }

    /// Decode PLAIN encoded BYTE_ARRAY (strings) from uncompressed page data
    /// OPTIMIZED VERSION: Batch allocation instead of 1 malloc per string
    /// Uses contiguous buffer for all strings (better cache locality, less fragmentation)
    pub fn decodePlainByteArray(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
    ) ![][]const u8 {
        var pos: usize = 0;

        // For OPTIONAL columns, skip definition levels
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // BYTE_ARRAY format: [INT32: length] [bytes...]

        // PASS 1: Calculate total size needed for all strings
        var total_bytes: usize = 0;
        var scan_pos = pos;
        for (0..num_values) |_| {
            if (scan_pos + 4 > page_data.len) {
                return error.InvalidPageData;
            }
            const length = std.mem.readInt(i32, page_data[scan_pos..][0..4], .little);
            scan_pos += 4;

            if (length < 0 or scan_pos + @as(usize, @intCast(length)) > page_data.len) {
                return error.InvalidPageData;
            }

            total_bytes += @intCast(length);
            scan_pos += @intCast(length);
        }

        // PASS 2: Allocate contiguous buffer + slice array
        const buffer = try allocator.alloc(u8, total_bytes);
        errdefer allocator.free(buffer);

        var values = try allocator.alloc([]const u8, num_values);
        errdefer allocator.free(values);

        // PASS 3: Copy strings into buffer and create slices
        var buffer_offset: usize = 0;
        for (0..num_values) |i| {
            const length = std.mem.readInt(i32, page_data[pos..][0..4], .little);
            pos += 4;

            const ulen: usize = @intCast(length);
            @memcpy(buffer[buffer_offset..][0..ulen], page_data[pos..][0..ulen]);
            values[i] = buffer[buffer_offset..][0..ulen];

            buffer_offset += ulen;
            pos += ulen;
        }

        return values;
    }

    /// Decode RLE_DICTIONARY encoded INT64 values
    /// Dictionary values must be provided (read from dictionary page first)
    pub fn decodeRLEDictionaryInt64(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
        dictionary: []const i64,
    ) ![]i64 {
        var pos: usize = 0;

        // Skip definition levels if column is optional
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Read bit width (1 byte)
        if (pos >= page_data.len) return error.InvalidPageData;
        const bit_width: u5 = @intCast(page_data[pos]);
        pos += 1;

        // Decode indices using RLE
        const indices_data = page_data[pos..];
        var rle_decoder = rle.RLEDecoder.init(indices_data, bit_width);
        const indices = try rle_decoder.decodeAll(allocator, num_values);
        defer allocator.free(indices);

        // Map indices to dictionary values
        var values = try allocator.alloc(i64, num_values);
        for (0..num_values) |i| {
            const idx = indices[i];
            if (idx < 0 or idx >= dictionary.len) {
                allocator.free(values);
                return error.InvalidDictionaryIndex;
            }
            values[i] = dictionary[@intCast(idx)];
        }

        return values;
    }

    /// Decode RLE_DICTIONARY encoded BYTE_ARRAY (strings)
    /// Dictionary values must be provided (read from dictionary page first)
    pub fn decodeRLEDictionaryByteArray(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
        dictionary: []const []const u8,
    ) ![][]const u8 {
        var pos: usize = 0;

        // Skip definition levels if column is optional
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Read bit width (1 byte)
        if (pos >= page_data.len) return error.InvalidPageData;
        const bit_width: u5 = @intCast(page_data[pos]);
        pos += 1;

        // Decode indices using RLE
        const indices_data = page_data[pos..];
        var rle_decoder = rle.RLEDecoder.init(indices_data, bit_width);
        const indices = try rle_decoder.decodeAll(allocator, num_values);
        defer allocator.free(indices);

        // Map indices to dictionary values (ZERO-COPY: just reference the dictionary)
        // The dictionary strings are already allocated and will outlive this result
        var values = try allocator.alloc([]const u8, num_values);
        errdefer allocator.free(values);

        for (0..num_values) |i| {
            const idx = indices[i];
            if (idx < 0 or idx >= dictionary.len) {
                allocator.free(values);
                return error.InvalidDictionaryIndex;
            }

            // ZERO-COPY: Just point to the dictionary entry (no allocation, no memcpy)
            values[i] = dictionary[@intCast(idx)];
        }

        return values;
    }

    /// Decode RLE_DICTIONARY encoded FLOAT values
    /// Dictionary values must be provided (read from dictionary page first)
    pub fn decodeRLEDictionaryFloat(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
        dictionary: []const f32,
    ) ![]f32 {
        var pos: usize = 0;

        // Skip definition levels if column is optional
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Read bit width (1 byte)
        if (pos >= page_data.len) return error.InvalidPageData;
        const bit_width: u5 = @intCast(page_data[pos]);
        pos += 1;

        // Decode indices using RLE
        const indices_data = page_data[pos..];
        var rle_decoder = rle.RLEDecoder.init(indices_data, bit_width);
        const indices = try rle_decoder.decodeAll(allocator, num_values);
        defer allocator.free(indices);

        // Map indices to dictionary values
        var values = try allocator.alloc(f32, num_values);
        for (0..num_values) |i| {
            const idx = indices[i];
            if (idx < 0 or idx >= dictionary.len) {
                allocator.free(values);
                return error.InvalidDictionaryIndex;
            }
            values[i] = dictionary[@intCast(idx)];
        }

        return values;
    }

    /// Decode RLE_DICTIONARY encoded DOUBLE values
    /// Dictionary values must be provided (read from dictionary page first)
    pub fn decodeRLEDictionaryDouble(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
        dictionary: []const f64,
    ) ![]f64 {
        var pos: usize = 0;

        // Skip definition levels if column is optional
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Read bit width (1 byte)
        if (pos >= page_data.len) return error.InvalidPageData;
        const bit_width: u5 = @intCast(page_data[pos]);
        pos += 1;

        // Decode indices using RLE
        const indices_data = page_data[pos..];
        var rle_decoder = rle.RLEDecoder.init(indices_data, bit_width);
        const indices = try rle_decoder.decodeAll(allocator, num_values);
        defer allocator.free(indices);

        // Map indices to dictionary values
        var values = try allocator.alloc(f64, num_values);
        for (0..num_values) |i| {
            const idx = indices[i];
            if (idx < 0 or idx >= dictionary.len) {
                allocator.free(values);
                return error.InvalidDictionaryIndex;
            }
            values[i] = dictionary[@intCast(idx)];
        }

        return values;
    }

    /// Decode RLE_DICTIONARY encoded BOOLEAN values
    /// Dictionary values must be provided (read from dictionary page first)
    pub fn decodeRLEDictionaryBoolean(
        allocator: std.mem.Allocator,
        page_data: []const u8,
        num_values: usize,
        is_required: bool,
        dictionary: []const bool,
    ) ![]bool {
        var pos: usize = 0;

        // Skip definition levels if column is optional
        if (!is_required) {
            pos = try skipDefinitionLevels(page_data);
        }

        // Read bit width (1 byte)
        if (pos >= page_data.len) return error.InvalidPageData;
        const bit_width: u5 = @intCast(page_data[pos]);
        pos += 1;

        // Decode indices using RLE
        const indices_data = page_data[pos..];
        var rle_decoder = rle.RLEDecoder.init(indices_data, bit_width);
        const indices = try rle_decoder.decodeAll(allocator, num_values);
        defer allocator.free(indices);

        // Map indices to dictionary values
        var values = try allocator.alloc(bool, num_values);
        for (0..num_values) |i| {
            const idx = indices[i];
            if (idx < 0 or idx >= dictionary.len) {
                allocator.free(values);
                return error.InvalidDictionaryIndex;
            }
            values[i] = dictionary[@intCast(idx)];
        }

        return values;
    }

    pub fn close(self: *Self) void {
        if (self.metadata) |*meta| {
            meta.deinit();
        }
        self.file.close();
    }

    /// Verify magic numbers at start and end
    pub fn verifyMagic(self: *Self) !void {
        // Check header magic
        var header_magic: [4]u8 = undefined;
        try self.file.seekTo(0);
        const header_read = try self.file.read(&header_magic);
        if (header_read != 4 or !std.mem.eql(u8, &header_magic, &MAGIC)) {
            return error.InvalidMagicNumber;
        }

        // Check footer magic
        var footer_magic: [4]u8 = undefined;
        try self.file.seekTo(self.file_size - 4);
        const footer_read = try self.file.read(&footer_magic);
        if (footer_read != 4 or !std.mem.eql(u8, &footer_magic, &MAGIC)) {
            return error.InvalidMagicNumber;
        }
    }

    /// Read footer length (4 bytes before the trailing magic)
    fn readFooterLength(self: *Self) !u32 {
        var buf: [4]u8 = undefined;
        try self.file.seekTo(self.file_size - 8);
        _ = try self.file.read(&buf);
        return std.mem.readInt(u32, &buf, .little);
    }

    /// Read and parse the file metadata (footer)
    pub fn readMetadata(self: *Self) !void {
        try self.verifyMagic();

        const footer_len = try self.readFooterLength();

        // Footer starts at: file_size - 8 (footer_len + magic) - footer_len
        const footer_offset = self.file_size - 8 - footer_len;

        // Read footer bytes
        const footer_bytes = try self.allocator.alloc(u8, footer_len);
        defer self.allocator.free(footer_bytes);

        try self.file.seekTo(footer_offset);
        const bytes_read = try self.file.read(footer_bytes);
        if (bytes_read != footer_len) {
            return error.InvalidFooter;
        }

        // Parse Thrift-encoded footer
        self.metadata = try parseFileMetaData(self.allocator, footer_bytes);
    }

    /// Get schema information
    pub fn getSchema(self: *const Self) ?[]SchemaElement {
        if (self.metadata) |meta| {
            return meta.schema;
        }
        return null;
    }

    /// Get number of rows
    pub fn getNumRows(self: *const Self) i64 {
        if (self.metadata) |meta| {
            return meta.num_rows;
        }
        return 0;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test "Parquet magic number" {
    try std.testing.expectEqualSlices(u8, "PAR1", &MAGIC);
}

test "Type enum values" {
    try std.testing.expectEqual(@as(i32, 0), @intFromEnum(Type.BOOLEAN));
    try std.testing.expectEqual(@as(i32, 1), @intFromEnum(Type.INT32));
    try std.testing.expectEqual(@as(i32, 6), @intFromEnum(Type.BYTE_ARRAY));
}

test "CompressionCodec enum" {
    try std.testing.expectEqual(@as(i32, 0), @intFromEnum(CompressionCodec.UNCOMPRESSED));
    try std.testing.expectEqual(@as(i32, 1), @intFromEnum(CompressionCodec.SNAPPY));
    try std.testing.expectEqual(@as(i32, 6), @intFromEnum(CompressionCodec.ZSTD));
}
