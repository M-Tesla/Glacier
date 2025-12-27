// ═══════════════════════════════════════════════════════════════════════════
// GLACIER REPL - Interactive SQL Shell
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const glacier = @import("glacier");

// Display mode for query results
const DisplayMode = union(enum) {
    default: void, // Show 10 rows (deprecated - use LIMIT instead)
    full: void, // Show all rows
};

// Data source types
const DataSourceType = enum {
    iceberg_table, // Directory with metadata/
    parquet_file, // .parquet file
    avro_file, // .avro file
};

// Connection to a data source
const Connection = struct {
    name: []const u8,
    path: []const u8,
    source_type: DataSourceType,
    metadata: ?ConnectionMetadata,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, name: []const u8, path: []const u8) !Self {
        const owned_name = try allocator.dupe(u8, name);
        errdefer allocator.free(owned_name);

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const source_type = try detectDataSource(path);

        return .{
            .name = owned_name,
            .path = owned_path,
            .source_type = source_type,
            .metadata = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.name);
        self.allocator.free(self.path);
        if (self.metadata) |*meta| {
            meta.deinit();
        }
    }

    pub fn loadMetadata(self: *Self) !void {
        switch (self.source_type) {
            .iceberg_table => {
                self.metadata = try loadIcebergMetadata(self.allocator, self.path);
            },
            .parquet_file => {
                self.metadata = try loadParquetMetadata(self.allocator, self.path);
            },
            .avro_file => {
                self.metadata = .{ .avro = .{} };
            },
        }
    }

    pub fn printInfo(self: *const Self) void {
        std.debug.print("\nConnected to: {s}\n", .{self.name});
        std.debug.print("  Type: ", .{});
        switch (self.source_type) {
            .iceberg_table => std.debug.print("Iceberg Table", .{}),
            .parquet_file => std.debug.print("Parquet File", .{}),
            .avro_file => std.debug.print("Avro File", .{}),
        }
        std.debug.print("\n  Path: {s}\n", .{self.path});

        if (self.metadata) |meta| {
            switch (meta) {
                .iceberg => |ice| {
                    std.debug.print("  Format Version: v{d}\n", .{ice.format_version});
                    std.debug.print("  Schema: {d} fields\n", .{ice.field_count});
                    if (ice.record_count) |rc| {
                        std.debug.print("  Records: {d}\n", .{rc});
                    }
                },
                .parquet => |pq| {
                    std.debug.print("  Row Groups: {d}\n", .{pq.row_group_count});
                    std.debug.print("  Total Rows: {d}\n", .{pq.total_rows});
                    std.debug.print("  Columns: {d}\n", .{pq.column_count});
                },
                .avro => {},
            }
        }
        std.debug.print("\n", .{});
    }
};

const ConnectionMetadata = union(enum) {
    iceberg: IcebergMetadata,
    parquet: ParquetMetadata,
    avro: AvroMetadata,

    pub fn deinit(self: *ConnectionMetadata) void {
        switch (self.*) {
            .iceberg => |*meta| meta.deinit(),
            .parquet => {},
            .avro => {},
        }
    }
};

const IcebergMetadata = struct {
    format_version: i32,
    field_count: usize,
    record_count: ?i64,
    // NO table_metadata pointer - we don't keep it in memory!

    pub fn deinit(self: *IcebergMetadata) void {
        _ = self; // Nothing to free - all data is stack-allocated primitives
    }
};

const ParquetMetadata = struct {
    row_group_count: usize,
    total_rows: i64,
    column_count: usize,
};

const AvroMetadata = struct {};

// ═══════════════════════════════════════════════════════════════════════════
// ICEBERG BOUNDS CHECK - Predicate Pushdown Optimization
// ═══════════════════════════════════════════════════════════════════════════

/// Find field_id for a column name in Iceberg schema
fn getFieldIdByName(schema: *const glacier.iceberg.Schema, column_name: []const u8) ?i32 {
    for (schema.fields) |field| {
        if (std.mem.eql(u8, field.name, column_name)) {
            return field.id;
        }
    }
    return null;
}

/// Parse int64 from Iceberg bounds format (little-endian ByteBuffer)
/// Iceberg stores bounds as Map<FieldId, ByteBuffer> where ByteBuffer is raw bytes
fn parseInt64Bound(bound_bytes: []const u8) ?i64 {
    // Iceberg int64 bounds are 8 bytes, little-endian
    if (bound_bytes.len < 8) return null;

    // Read 8 bytes as little-endian int64
    const value = std.mem.readInt(i64, bound_bytes[0..8], .little);
    return value;
}

/// Parse float/double from Iceberg bounds format (little-endian)
fn parseDoubleBound(bound_bytes: []const u8) ?f64 {
    if (bound_bytes.len == 8) {
        // Try as double (f64)
        return std.mem.bytesAsValue(f64, bound_bytes[0..8]).*;
    } else if (bound_bytes.len == 4) {
        // Try as float (f32), convert to f64
        const f32_val = std.mem.bytesAsValue(f32, bound_bytes[0..4]).*;
        return @floatCast(f32_val);
    }
    return null;
}

/// Evaluate int64 predicate for file pruning
fn evaluateInt64Predicate(op: glacier.sql.BinaryOp, lower: i64, upper: i64, value: i64) bool {
    return switch (op) {
        .gt => upper <= value, // Skip if max ≤ threshold
        .ge => upper < value, // Skip if max < threshold
        .lt => lower >= value, // Skip if min ≥ threshold
        .le => lower > value, // Skip if min > threshold
        .eq => value < lower or value > upper, // Skip if not in range
        .ne => lower == upper and lower == value, // Skip if all values are X
        else => false,
    };
}

/// Evaluate double predicate for file pruning
fn evaluateDoublePredicate(op: glacier.sql.BinaryOp, lower: f64, upper: f64, value: f64) bool {
    return switch (op) {
        .gt => upper <= value,
        .ge => upper < value,
        .lt => lower >= value,
        .le => lower > value,
        .eq => value < lower or value > upper,
        .ne => lower == upper and lower == value,
        else => false,
    };
}

/// Check if a file can be skipped based on WHERE clause and Iceberg bounds
/// Generic implementation supporting any numeric column (int64, float, double)
/// Returns true if file can be SKIPPED (doesn't match predicate)
fn canSkipFileByBounds(
    where_clause: *const glacier.sql.Expr,
    lower_bounds: ?glacier.avro.BoundsMap,
    upper_bounds: ?glacier.avro.BoundsMap,
    schema: *const glacier.iceberg.Schema,
) bool {
    // No bounds = can't skip (must read file)
    if (lower_bounds == null or upper_bounds == null) return false;

    const lower_map = lower_bounds.?;
    const upper_map = upper_bounds.?;

    // Evaluate WHERE clause against bounds
    // Only handle simple binary comparisons: column OP value
    switch (where_clause.*) {
        .binary => |bin| {
            // Extract column name from WHERE clause
            if (bin.left.* != .column) return false;
            const column_name = bin.left.column;

            // Lookup field_id for this column in schema
            const field_id = getFieldIdByName(schema, column_name) orelse return false;

            // Get bounds bytes for this field_id
            const lower_bytes = lower_map.get(field_id) orelse return false;
            const upper_bytes = upper_map.get(field_id) orelse return false;

            if (bin.right.* != .number) return false;

            // Try parsing as int64 first (most common)
            if (parseInt64Bound(lower_bytes)) |lower_val| {
                if (parseInt64Bound(upper_bytes)) |upper_val| {
                    const compare_value: i64 = @intFromFloat(bin.right.number);
                    return evaluateInt64Predicate(bin.op, lower_val, upper_val, compare_value);
                }
            }

            // Try parsing as double (FLOAT/DOUBLE columns)
            if (parseDoubleBound(lower_bytes)) |lower_val| {
                if (parseDoubleBound(upper_bytes)) |upper_val| {
                    const compare_value = bin.right.number;
                    return evaluateDoublePredicate(bin.op, lower_val, upper_val, compare_value);
                }
            }

            return false; // Couldn't parse bounds as numeric type
        },
        else => {},
    }

    return false; // Can't determine - must read file
}

/// Load Iceberg table metadata - LIGHTWEIGHT with arena allocator for temp data
fn loadIcebergMetadata(allocator: std.mem.Allocator, table_path: []const u8) !ConnectionMetadata {
    // Use arena allocator for ALL temporary allocations - FREED at end
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit(); // FREE EVERYTHING automatically

    const temp_alloc = arena.allocator();

    // Find latest metadata file - use simple path concatenation
    var path_buf: [4096]u8 = undefined;
    const metadata_path = try std.fmt.bufPrint(&path_buf, "{s}\\metadata", .{table_path});

    var metadata_dir = try std.fs.cwd().openDir(metadata_path, .{ .iterate = true });
    defer metadata_dir.close();

    var latest_version: i32 = -1;
    var latest_file: ?[]const u8 = null;

    var iter = metadata_dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".metadata.json")) continue;

        if (std.mem.startsWith(u8, entry.name, "v")) {
            const version_str = entry.name[1..];
            const dot_idx = std.mem.indexOf(u8, version_str, ".") orelse continue;
            const version = std.fmt.parseInt(i32, version_str[0..dot_idx], 10) catch continue;

            if (version > latest_version) {
                latest_version = version;
                latest_file = try temp_alloc.dupe(u8, entry.name);
            }
        }
    }

    if (latest_file == null) return error.NoMetadataFound;

    // Read and parse metadata JSON (using temp arena)
    const json_file = try metadata_dir.openFile(latest_file.?, .{});
    defer json_file.close();

    const json_content = try json_file.readToEndAlloc(temp_alloc, 10 * 1024 * 1024);

    // Parse ONLY to extract the minimal data we need, then DISCARD
    var table_meta = try glacier.iceberg.TableMetadata.parseFromJson(temp_alloc, json_content);
    defer table_meta.deinit(); // CLEAN UP parsed metadata immediately

    const current_schema = table_meta.getCurrentSchema();
    const field_count = if (current_schema) |s| s.fields.len else 0;

    // Extract record count from snapshot summary
    var record_count: ?i64 = null;
    if (table_meta.getCurrentSnapshot()) |snapshot| {
        if (snapshot.summary) |summary| {
            record_count = summary.added_records;
        }
    }

    // Return ONLY the minimal extracted data - NO HEAP ALLOCATIONS KEPT
    return .{
        .iceberg = .{
            .format_version = table_meta.format_version,
            .field_count = field_count,
            .record_count = record_count,
        },
    };
}

/// Load Parquet file metadata - LIGHTWEIGHT, read and CLEAN immediately
fn loadParquetMetadata(allocator: std.mem.Allocator, file_path: []const u8) !ConnectionMetadata {
    // Use arena for temporary reader allocations
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit(); // FREE all reader allocations

    const temp_alloc = arena.allocator();

    var reader = try glacier.parquet.Reader.open(temp_alloc, file_path);
    defer reader.close();

    try reader.readMetadata();
    const metadata = reader.metadata.?;

    // Extract data BEFORE arena cleanup
    // Use FileMetaData.num_rows first, fall back to summing row groups if needed
    var total_rows: i64 = metadata.num_rows;
    if (total_rows == 0) {
        // Fallback: sum row groups (some files don't have global num_rows)
        for (metadata.row_groups) |rg| {
            total_rows += rg.num_rows;
        }
    }

    const column_count = if (metadata.row_groups.len > 0)
        metadata.row_groups[0].columns.len
    else
        0;

    const row_group_count = metadata.row_groups.len;

    // Return ONLY the extracted numbers - reader and metadata will be FREED
    return .{
        .parquet = .{
            .row_group_count = row_group_count,
            .total_rows = total_rows,
            .column_count = column_count,
        },
    };
}

/// Evaluate WHERE clause expression for a single row
/// Returns true if row passes the filter, false otherwise
fn evaluateWhere(expr: *const glacier.sql.Expr, column_values: std.StringHashMap(ColumnValue)) bool {
    switch (expr.*) {
        .column => |col_name| {
            // Column reference - get value
            if (column_values.get(col_name)) |val| {
                return switch (val) {
                    .bool => |b| b,
                    else => false, // Non-boolean column used as predicate
                };
            }
            return false; // Column not found
        },

        .boolean => |b| return b,

        .binary => |bin| {
            const left_val = getExprValue(bin.left, column_values);
            const right_val = getExprValue(bin.right, column_values);

            return switch (bin.op) {
                .eq => compareValues(left_val, right_val, .eq),
                .ne => compareValues(left_val, right_val, .ne),
                .lt => compareValues(left_val, right_val, .lt),
                .le => compareValues(left_val, right_val, .le),
                .gt => compareValues(left_val, right_val, .gt),
                .ge => compareValues(left_val, right_val, .ge),
                .and_op => evaluateWhere(bin.left, column_values) and evaluateWhere(bin.right, column_values),
                .or_op => evaluateWhere(bin.left, column_values) or evaluateWhere(bin.right, column_values),
                else => false, // Arithmetic ops not supported in WHERE for now
            };
        },

        .unary => |un| {
            switch (un.op) {
                .not => return !evaluateWhere(un.expr, column_values),
                else => return false,
            }
        },

        else => return false, // Unsupported expression type
    }
}

/// Column value types for WHERE evaluation
const ColumnValue = union(enum) {
    int: i64,
    float: f64,
    string: []const u8,
    bool: bool,
    null_val: void,
};

/// Get value from expression
fn getExprValue(expr: *const glacier.sql.Expr, column_values: std.StringHashMap(ColumnValue)) ColumnValue {
    switch (expr.*) {
        .number => |n| {
            // Try to convert to int if it's a whole number
            if (@floor(n) == n) {
                return .{ .int = @intFromFloat(n) };
            }
            return .{ .float = n };
        },
        .string => |s| return .{ .string = s },
        .boolean => |b| return .{ .bool = b },
        .null_literal => return .{ .null_val = {} },
        .column => |col_name| {
            if (column_values.get(col_name)) |val| {
                return val;
            }
            return .{ .null_val = {} };
        },
        else => return .{ .null_val = {} },
    }
}

/// Compare two values based on operator
fn compareValues(left: ColumnValue, right: ColumnValue, op: glacier.sql.BinaryOp) bool {
    // NULL handling: NULL compared to anything is false (SQL semantics)
    if (left == .null_val or right == .null_val) return false;

    // Type coercion and comparison
    switch (left) {
        .int => |l| {
            switch (right) {
                .int => |r| return compareInts(l, r, op),
                .float => |r| return compareFloats(@floatFromInt(l), r, op),
                else => return false,
            }
        },
        .float => |l| {
            switch (right) {
                .int => |r| return compareFloats(l, @floatFromInt(r), op),
                .float => |r| return compareFloats(l, r, op),
                else => return false,
            }
        },
        .string => |l| {
            switch (right) {
                .string => |r| return compareStrings(l, r, op),
                else => return false,
            }
        },
        .bool => |l| {
            switch (right) {
                .bool => |r| return compareBools(l, r, op),
                else => return false,
            }
        },
        .null_val => return false,
    }
}

fn compareInts(a: i64, b: i64, op: glacier.sql.BinaryOp) bool {
    return switch (op) {
        .eq => a == b,
        .ne => a != b,
        .lt => a < b,
        .le => a <= b,
        .gt => a > b,
        .ge => a >= b,
        else => false,
    };
}

fn compareFloats(a: f64, b: f64, op: glacier.sql.BinaryOp) bool {
    return switch (op) {
        .eq => a == b,
        .ne => a != b,
        .lt => a < b,
        .le => a <= b,
        .gt => a > b,
        .ge => a >= b,
        else => false,
    };
}

fn compareStrings(a: []const u8, b: []const u8, op: glacier.sql.BinaryOp) bool {
    const cmp = std.mem.order(u8, a, b);
    return switch (op) {
        .eq => cmp == .eq,
        .ne => cmp != .eq,
        .lt => cmp == .lt,
        .le => cmp != .gt,
        .gt => cmp == .gt,
        .ge => cmp != .lt,
        else => false,
    };
}

fn compareBools(a: bool, b: bool, op: glacier.sql.BinaryOp) bool {
    return switch (op) {
        .eq => a == b,
        .ne => a != b,
        else => false,
    };
}

/// Detect data source type (Iceberg table, Parquet file, or Avro file)
/// EXTREMELY LIGHTWEIGHT: extension check → stat → magic number (minimal syscalls)
fn detectDataSource(path: []const u8) !DataSourceType {
    // Fast path: check extension first (zero syscalls)
    if (std.mem.endsWith(u8, path, ".parquet")) {
        return .parquet_file;
    }
    if (std.mem.endsWith(u8, path, ".avro")) {
        return .avro_file;
    }

    // Check if it's a directory (Iceberg table)
    const stat = std.fs.cwd().statFile(path) catch |err| {
        // If statFile fails with IsDir, it's a directory
        if (err == error.IsDir) {
            // Verify it has metadata/ subdirectory
            var dir = std.fs.cwd().openDir(path, .{}) catch return error.NotIcebergTable;
            defer dir.close();

            dir.access("metadata", .{}) catch return error.NotIcebergTable;
            return .iceberg_table;
        }
        return err;
    };

    if (stat.kind == .directory) {
        // It's a directory - verify Iceberg structure
        var dir = std.fs.cwd().openDir(path, .{}) catch return error.NotIcebergTable;
        defer dir.close();

        dir.access("metadata", .{}) catch return error.NotIcebergTable;
        return .iceberg_table;
    }

    // File without extension - read magic number (4 bytes)
    const file = std.fs.cwd().openFile(path, .{}) catch return error.UnknownFormat;
    defer file.close();

    var magic: [4]u8 = undefined;
    const n = file.read(&magic) catch return error.UnknownFormat;
    if (n < 4) return error.FileTooSmall;

    // Parquet: "PAR1"
    if (std.mem.eql(u8, &magic, "PAR1")) {
        return .parquet_file;
    }

    // Avro: "Obj\x01"
    if (magic[0] == 'O' and magic[1] == 'b' and magic[2] == 'j' and magic[3] == 0x01) {
        return .avro_file;
    }

    return error.UnknownFormat;
}

/// Execute query on Iceberg table
fn loadIcebergDataFiles(allocator: std.mem.Allocator, table_path: []const u8, query: ?*const glacier.sql.Query) ![][]const u8 {
    // Find latest metadata file
    const metadata_path = try std.fs.path.join(allocator, &[_][]const u8{ table_path, "metadata" });
    defer allocator.free(metadata_path);

    var metadata_dir = try std.fs.cwd().openDir(metadata_path, .{ .iterate = true });
    defer metadata_dir.close();

    // Find v*.metadata.json (highest version = current)
    var latest_version: i32 = -1;
    var latest_file: ?[]const u8 = null;
    defer if (latest_file) |f| allocator.free(f);

    var iter = metadata_dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".metadata.json")) continue;

        // Parse version: v1.metadata.json -> 1
        if (std.mem.startsWith(u8, entry.name, "v")) {
            const version_str = entry.name[1..];
            const dot_idx = std.mem.indexOf(u8, version_str, ".") orelse continue;
            const version = std.fmt.parseInt(i32, version_str[0..dot_idx], 10) catch continue;

            if (version > latest_version) {
                if (latest_file) |f| allocator.free(f);
                latest_version = version;
                latest_file = try allocator.dupe(u8, entry.name);
            }
        }
    }

    if (latest_file == null) {
        return error.NoMetadataFound;
    }

    // Read and parse metadata JSON
    const json_path = try std.fs.path.join(allocator, &[_][]const u8{ metadata_path, latest_file.? });
    defer allocator.free(json_path);

    const json_file = try std.fs.cwd().openFile(json_path, .{});
    defer json_file.close();

    const json_content = try json_file.readToEndAlloc(allocator, 10 * 1024 * 1024);
    defer allocator.free(json_content);

    var table_metadata = try glacier.iceberg.TableMetadata.parseFromJson(allocator, json_content);
    defer table_metadata.deinit();

    const current_schema = table_metadata.getCurrentSchema();

    // Get current snapshot
    const current_snapshot = table_metadata.getCurrentSnapshot();
    if (current_snapshot == null) {
        return &[_][]const u8{}; // Empty table
    }

    // Read manifest list (Avro file)
    var manifest_list_path = current_snapshot.?.manifest_list;

    // Strip file:// URI prefix if present
    if (std.mem.startsWith(u8, manifest_list_path, "file://")) {
        manifest_list_path = manifest_list_path[7..];
        if (manifest_list_path.len > 0 and manifest_list_path[0] == '/' and
            manifest_list_path.len > 2 and manifest_list_path[2] == ':')
        {
            manifest_list_path = manifest_list_path[1..];
        }
    }

    // Check if manifest list is remote
    const is_remote = std.mem.startsWith(u8, manifest_list_path, "s3://") or
        std.mem.startsWith(u8, manifest_list_path, "http://") or
        std.mem.startsWith(u8, manifest_list_path, "https://");

    if (is_remote) {
        return error.RemoteManifestNotSupported;
    }

    // Check if path is absolute
    const is_absolute = std.fs.path.isAbsolute(manifest_list_path);
    const starts_with_table_name = std.mem.startsWith(u8, manifest_list_path, table_path);

    const manifest_path_to_read = if (is_absolute or starts_with_table_name)
        manifest_list_path
    else
        try std.fs.path.join(allocator, &[_][]const u8{ table_path, manifest_list_path });

    defer if (!is_absolute and !starts_with_table_name) allocator.free(manifest_path_to_read);

    // Read manifest list Avro file to get data file entries
    const data_file_entries = try glacier.avro.readManifestFile(allocator, manifest_path_to_read);
    defer {
        for (data_file_entries) |*entry| {
            var mut_entry = entry.*;
            mut_entry.deinit();
        }
        allocator.free(data_file_entries);
    }

    // Filter for ADDED and EXISTING files only
    var data_files: std.ArrayListUnmanaged([]const u8) = .{};
    errdefer {
        for (data_files.items) |file| allocator.free(file);
        data_files.deinit(allocator);
    }

    for (data_file_entries) |entry| {
        if (entry.status == .added or entry.status == .existing) {
            var raw_path = entry.file_path;

            // Strip file:// URI prefix if present
            if (std.mem.startsWith(u8, raw_path, "file://")) {
                raw_path = raw_path[7..];
                if (raw_path.len > 0 and raw_path[0] == '/' and
                    raw_path.len > 2 and raw_path[2] == ':')
                {
                    raw_path = raw_path[1..];
                }
            }

            // Check if it's a remote path
            const is_remote_path = std.mem.startsWith(u8, raw_path, "s3://") or
                std.mem.startsWith(u8, raw_path, "http://") or
                std.mem.startsWith(u8, raw_path, "https://");

            if (is_remote_path) {
                continue;
            }

            // Resolve relative paths from table root
            const is_absolute_path = std.mem.startsWith(u8, raw_path, "/") or
                std.mem.indexOf(u8, raw_path, ":/") != null;

            const file_path = if (is_absolute_path)
                try allocator.dupe(u8, raw_path)
            else
                try std.fs.path.join(allocator, &[_][]const u8{ table_path, raw_path });

            // PREDICATE PUSHDOWN: Skip files that can't match WHERE clause
            if (query) |q| {
                if (q.where_clause) |where_clause| {
                    if (current_schema) |schema| {
                        if (canSkipFileByBounds(where_clause, entry.lower_bounds, entry.upper_bounds, schema)) {
                            allocator.free(file_path);
                            continue;
                        }
                    }
                }
            }

            try data_files.append(allocator, file_path);
        }
    }

    return try data_files.toOwnedSlice(allocator);
}

fn executeIcebergQuery(allocator: std.mem.Allocator, table_path: []const u8, query: *const glacier.sql.Query, mode: DisplayMode) !void {
    std.debug.print("\n", .{});
    std.debug.print("Reading Iceberg metadata...\n", .{});

    // Find latest metadata file
    const metadata_path = try std.fs.path.join(allocator, &[_][]const u8{ table_path, "metadata" });
    defer allocator.free(metadata_path);

    var metadata_dir = try std.fs.cwd().openDir(metadata_path, .{ .iterate = true });
    defer metadata_dir.close();

    // Find v*.metadata.json (highest version = current)
    var latest_version: i32 = -1;
    var latest_file: ?[]const u8 = null;
    defer if (latest_file) |f| allocator.free(f);

    var iter = metadata_dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".metadata.json")) continue;

        // Parse version: v1.metadata.json -> 1
        if (std.mem.startsWith(u8, entry.name, "v")) {
            const version_str = entry.name[1..];
            const dot_idx = std.mem.indexOf(u8, version_str, ".") orelse continue;
            const version = std.fmt.parseInt(i32, version_str[0..dot_idx], 10) catch continue;

            if (version > latest_version) {
                if (latest_file) |f| allocator.free(f);
                latest_version = version;
                latest_file = try allocator.dupe(u8, entry.name);
            }
        }
    }

    if (latest_file == null) {
        std.debug.print("[ERROR] No metadata files found in {s}\n\n", .{metadata_path});
        return error.NoMetadataFound;
    }

    // Read and parse metadata JSON
    const json_path = try std.fs.path.join(allocator, &[_][]const u8{ metadata_path, latest_file.? });
    defer allocator.free(json_path);

    const json_file = try std.fs.cwd().openFile(json_path, .{});
    defer json_file.close();

    const json_content = try json_file.readToEndAlloc(allocator, 10 * 1024 * 1024);
    defer allocator.free(json_content);

    var table_metadata = try glacier.iceberg.TableMetadata.parseFromJson(allocator, json_content);
    defer table_metadata.deinit();

    std.debug.print("Iceberg Table (v{d})\n", .{table_metadata.format_version});
    std.debug.print("  Location: {s}\n", .{table_metadata.location});
    std.debug.print("  UUID: {s}\n", .{table_metadata.table_uuid});

    const current_schema = table_metadata.getCurrentSchema();
    if (current_schema) |schema| {
        std.debug.print("  Schema: {d} fields\n", .{schema.fields.len});
    }

    // Get current snapshot
    const current_snapshot = table_metadata.getCurrentSnapshot();
    if (current_snapshot == null) {
        std.debug.print("\n[INFO] Table has no snapshots (empty table)\n\n", .{});
        return;
    }

    std.debug.print("  Snapshot ID: {d}\n", .{current_snapshot.?.snapshot_id});
    std.debug.print("  Manifest List: {s}\n", .{current_snapshot.?.manifest_list});

    // Read manifest list (Avro file)
    var manifest_list_path = current_snapshot.?.manifest_list;

    // Strip file:// URI prefix if present
    if (std.mem.startsWith(u8, manifest_list_path, "file://")) {
        manifest_list_path = manifest_list_path[7..]; // Skip "file://"
        // Windows paths may have extra slash: file:///C:/path -> /C:/path, strip leading /
        if (manifest_list_path.len > 0 and manifest_list_path[0] == '/' and
            manifest_list_path.len > 2 and manifest_list_path[2] == ':')
        {
            manifest_list_path = manifest_list_path[1..]; // Skip leading /
        }
    }

    // Check if manifest list is remote
    const is_remote = std.mem.startsWith(u8, manifest_list_path, "s3://") or
        std.mem.startsWith(u8, manifest_list_path, "http://") or
        std.mem.startsWith(u8, manifest_list_path, "https://");

    if (is_remote) {
        std.debug.print("\n[WARNING] Remote manifest lists not yet supported: {s}\n\n", .{manifest_list_path});
        return error.RemoteManifestNotSupported;
    }

    // Check if path is absolute (Windows: C:/ or Linux: /)
    const is_absolute = std.fs.path.isAbsolute(manifest_list_path);

    // Check if manifest path already starts with table name (generated by some Iceberg writers)
    const starts_with_table_name = std.mem.startsWith(u8, manifest_list_path, table_path);

    const manifest_path_to_read = if (is_absolute or starts_with_table_name)
        manifest_list_path
    else
        try std.fs.path.join(allocator, &[_][]const u8{ table_path, manifest_list_path });

    defer if (!is_absolute and !starts_with_table_name) allocator.free(manifest_path_to_read);

    std.debug.print("\n", .{});
    std.debug.print("Reading manifest list: {s}\n", .{manifest_path_to_read});

    // Read manifest list Avro file to get data file entries
    const data_file_entries = glacier.avro.readManifestFile(allocator, manifest_path_to_read) catch |err| {
        std.debug.print("[ERROR] Failed to read manifest list: {s}\n\n", .{@errorName(err)});
        return err;
    };
    defer {
        for (data_file_entries) |*entry| {
            var mut_entry = entry.*;
            mut_entry.deinit();
        }
        allocator.free(data_file_entries);
    }

    std.debug.print("  Found {d} data file entries\n", .{data_file_entries.len});

    // Filter for ADDED and EXISTING files only
    var data_files: std.ArrayListUnmanaged([]const u8) = .{};
    defer {
        for (data_files.items) |file| allocator.free(file);
        data_files.deinit(allocator);
    }

    var total_records: i64 = 0;
    for (data_file_entries) |entry| {
        if (entry.status == .added or entry.status == .existing) {
            // Resolve file path (handle file://, s3://, http://, absolute, relative)
            var raw_path = entry.file_path;

            // Strip file:// URI prefix if present
            if (std.mem.startsWith(u8, raw_path, "file://")) {
                raw_path = raw_path[7..];
                // Windows: file:///C:/path -> /C:/path, strip leading /
                if (raw_path.len > 0 and raw_path[0] == '/' and
                    raw_path.len > 2 and raw_path[2] == ':')
                {
                    raw_path = raw_path[1..];
                }
            }

            // Check if it's a remote path
            const is_remote_path = std.mem.startsWith(u8, raw_path, "s3://") or
                std.mem.startsWith(u8, raw_path, "http://") or
                std.mem.startsWith(u8, raw_path, "https://");

            if (is_remote_path) {
                std.debug.print("    WARNING: Remote data file not supported: {s}\n", .{raw_path});
                continue;
            }

            // Resolve relative paths from table root
            const is_absolute_path = std.mem.startsWith(u8, raw_path, "/") or
                std.mem.indexOf(u8, raw_path, ":/") != null;

            const file_path = if (is_absolute_path)
                try allocator.dupe(u8, raw_path)
            else
                try std.fs.path.join(allocator, &[_][]const u8{ table_path, raw_path });

            // PREDICATE PUSHDOWN: Skip files that can't match WHERE clause
            if (query.where_clause) |where_clause| {
                if (current_schema) |schema| {
                    if (canSkipFileByBounds(where_clause, entry.lower_bounds, entry.upper_bounds, schema)) {
                        std.debug.print("    >> SKIP: {s} (bounds don't match WHERE)\n", .{raw_path});
                        allocator.free(file_path); // Don't add to list
                        continue;
                    }
                }
            }

            try data_files.append(allocator, file_path);
            total_records += entry.record_count;
        }
    }

    std.debug.print("  Active data files: {d}\n", .{data_files.items.len});
    std.debug.print("  Total records: {d}\n", .{total_records});
    std.debug.print("\n", .{});

    if (data_files.items.len == 0) {
        std.debug.print("[INFO] No data files to read (empty table)\n\n", .{});
        return;
    }

    // Execute query on each Parquet data file
    std.debug.print("Executing query on data files:\n", .{});
    std.debug.print("---------------------------------------------------------\n", .{});

    for (data_files.items, 0..) |file_path, idx| {
        // Check if file exists
        std.fs.cwd().access(file_path, .{}) catch |err| {
            std.debug.print("WARNING: File not found [{d}/{d}]: {s}\n", .{ idx + 1, data_files.items.len, @errorName(err) });
            continue;
        };

        // Open and execute query on this Parquet file
        var reader = glacier.parquet.Reader.open(allocator, file_path) catch |err| {
            std.debug.print("WARNING: Failed to open [{d}/{d}]: {s}\n", .{ idx + 1, data_files.items.len, @errorName(err) });
            continue;
        };
        defer reader.close();

        try reader.readMetadata();
        const metadata = reader.metadata.?;

        if (metadata.row_groups.len == 0) continue;

        // Execute query on each row group in this file
        const is_select_star = query.select_columns.len == 1 and std.mem.eql(u8, query.select_columns[0], "*");

        for (metadata.row_groups) |rg| {
            if (is_select_star) {
                _ = try readAllColumnsWithMode(allocator, &reader, metadata, rg, mode, query.where_clause, query.order_by, query.limit);
            } else {
                _ = try readSpecificColumnsWithMode(allocator, &reader, metadata, rg, query.select_columns, mode, query.where_clause);
            }
        }
    }

    std.debug.print("\n", .{});
}

/// Execute query on Avro file
/// Avro schema field definition
const AvroField = struct {
    name: []const u8,
    field_type: []const u8, // "int", "long", "string", "double", "float", "boolean"
};

/// Parse Avro schema JSON from metadata
fn parseAvroSchema(allocator: std.mem.Allocator, schema_json: []const u8) ![]AvroField {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, schema_json, .{});
    defer parsed.deinit();

    const root = parsed.value;
    if (root != .object) return error.InvalidSchema;

    const obj = root.object;
    const fields_value = obj.get("fields") orelse return error.MissingFields;
    if (fields_value != .array) return error.InvalidFieldsArray;

    const fields_array = fields_value.array.items;
    var fields = try std.ArrayList(AvroField).initCapacity(allocator, fields_array.len);
    errdefer fields.deinit(allocator);

    for (fields_array) |field_value| {
        if (field_value != .object) continue;
        const field_obj = field_value.object;

        const name = if (field_obj.get("name")) |n| n.string else continue;
        const type_value = field_obj.get("type") orelse continue;

        // Type can be string or object/array (for unions, complex types)
        const type_str = switch (type_value) {
            .string => |s| s,
            .object => |o| if (o.get("type")) |t| t.string else "unknown",
            .array => |arr| blk: {
                // Union type like ["null", "string"] - get non-null type
                for (arr.items) |item| {
                    if (item == .string and !std.mem.eql(u8, item.string, "null")) {
                        break :blk item.string;
                    }
                }
                break :blk "unknown";
            },
            else => "unknown",
        };

        const name_copy = try allocator.dupe(u8, name);
        const type_copy = try allocator.dupe(u8, type_str);

        try fields.append(allocator, .{
            .name = name_copy,
            .field_type = type_copy,
        });
    }

    return try fields.toOwnedSlice(allocator);
}

/// Read an Avro value based on type
fn readAvroValue(reader: *glacier.avro_reader.AvroReader, field_type: []const u8) !DynamicColumnValue {
    if (std.mem.eql(u8, field_type, "int")) {
        const val = try reader.readInt();
        return .{ .int32 = val };
    } else if (std.mem.eql(u8, field_type, "long")) {
        const val = try reader.readLong();
        return .{ .int64 = val };
    } else if (std.mem.eql(u8, field_type, "string")) {
        const val = try reader.readString();
        return .{ .byte_array = val };
    } else if (std.mem.eql(u8, field_type, "double")) {
        // Read as bytes and interpret as double (little-endian)
        if (reader.pos + 8 > reader.data.len) return error.UnexpectedEOF;
        const bytes = reader.data[reader.pos..][0..8];
        reader.pos += 8;
        const int_val = std.mem.readInt(u64, bytes, .little);
        const val: f64 = @bitCast(int_val);
        return .{ .double = val };
    } else if (std.mem.eql(u8, field_type, "float")) {
        // Read as bytes and interpret as float (little-endian)
        if (reader.pos + 4 > reader.data.len) return error.UnexpectedEOF;
        const bytes = reader.data[reader.pos..][0..4];
        reader.pos += 4;
        const int_val = std.mem.readInt(u32, bytes, .little);
        const val: f32 = @bitCast(int_val);
        return .{ .float = val };
    } else if (std.mem.eql(u8, field_type, "boolean")) {
        const val = try reader.readBoolean();
        return .{ .boolean = val };
    } else {
        // Unknown type - skip
        _ = try reader.readString();
        return .{ .null_val = {} };
    }
}

fn executeAvroQuery(allocator: std.mem.Allocator, file_path: []const u8, query: *const glacier.sql.Query, _: DisplayMode) !void {
    std.debug.print("\n", .{});
    std.debug.print("🔧 Detected: Avro File\n\n", .{});

    // Read file
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    const file_size = (try file.stat()).size;
    const file_data = try allocator.alloc(u8, file_size);
    defer allocator.free(file_data);

    _ = try file.read(file_data);

    // Parse Avro header
    var reader = glacier.avro_reader.AvroReader.init(allocator, file_data);
    var header = try reader.readHeader();
    defer header.deinit();

    // Extract schema
    const schema_bytes = header.metadata.get("avro.schema") orelse return error.MissingSchema;
    const fields = try parseAvroSchema(allocator, schema_bytes);
    defer {
        for (fields) |field| {
            allocator.free(field.name);
            allocator.free(field.field_type);
        }
        allocator.free(fields);
    }

    // Determine which columns to select
    const is_select_star = query.select_columns.len == 1 and std.mem.eql(u8, query.select_columns[0], "*");
    const selected_fields = if (is_select_star) fields else blk: {
        var selected = std.ArrayList(AvroField){};
        for (query.select_columns) |col_name| {
            for (fields) |field| {
                if (std.mem.eql(u8, field.name, col_name)) {
                    try selected.append(allocator, field);
                    break;
                }
            }
        }
        break :blk try selected.toOwnedSlice(allocator);
    };
    defer if (!is_select_star) allocator.free(selected_fields);

    // Create column structure
    var columns = try std.ArrayList(ColumnData).initCapacity(allocator, selected_fields.len);
    defer {
        for (columns.items) |col| {
            allocator.free(col.values);
        }
        columns.deinit(allocator);
    }

    for (selected_fields) |field| {
        try columns.append(allocator, .{
            .name = field.name,
            .values = &[_]DynamicColumnValue{}, // Will be filled
        });
    }

    // Read all records
    var all_records = std.ArrayList([]DynamicColumnValue){};
    defer {
        for (all_records.items) |record| {
            allocator.free(record);
        }
        all_records.deinit(allocator);
    }

    while (try reader.readDataBlock(header.sync_marker)) |block| {
        var block_reader = glacier.avro_reader.AvroReader.init(allocator, block.data);

        var i: i64 = 0;
        while (i < block.count) : (i += 1) {
            var record = try allocator.alloc(DynamicColumnValue, fields.len);

            // Read all fields in order
            for (fields, 0..) |field, idx| {
                record[idx] = try readAvroValue(&block_reader, field.field_type);
            }

            try all_records.append(allocator, record);
        }
    }

    // Now reorganize data into column format
    for (columns.items) |*col| {
        // Find field index in original fields array
        var field_idx: usize = 0;
        for (fields, 0..) |field, idx| {
            if (std.mem.eql(u8, field.name, col.name)) {
                field_idx = idx;
                break;
            }
        }

        var col_values = try allocator.alloc(DynamicColumnValue, all_records.items.len);
        for (all_records.items, 0..) |record, row_idx| {
            col_values[row_idx] = record[field_idx];
        }
        col.values = col_values;
    }

    // Apply WHERE clause filtering
    var matched_rows = std.ArrayList(usize){};
    defer matched_rows.deinit(allocator);

    for (0..all_records.items.len) |i| {
        if (query.where_clause) |where_expr| {
            const passes = evaluateWhereDynamic(where_expr, columns.items, i);
            if (!passes) continue;
        }
        try matched_rows.append(allocator, i);
    }

    // Display using existing table display logic
    try displayDynamicTable(columns.items, matched_rows.items, all_records.items.len);
}

/// Prompt user for data source connection
fn promptConnection(allocator: std.mem.Allocator) !?Connection {
    const stdin_file = std.fs.File.stdin();
    var input_buffer: [1024]u8 = undefined;

    std.debug.print("\nWelcome to Glacier!\n\n", .{});
    std.debug.print("Select data source:\n", .{});
    std.debug.print("  1) Local Iceberg Table (directory)\n", .{});
    std.debug.print("  2) Local Parquet File\n", .{});
    std.debug.print("  3) Local Avro File\n", .{});
    std.debug.print("  4) Skip - Use FROM clause in queries\n", .{});
    std.debug.print("\nEnter option (1-4): ", .{});

    // Read option
    const option_line = readLineSync(stdin_file, &input_buffer) catch return null;
    const option = std.mem.trim(u8, option_line, &std.ascii.whitespace);

    if (std.mem.eql(u8, option, "4")) {
        std.debug.print("\nNo default connection. Use full paths in FROM clauses.\n", .{});
        return null;
    }

    // Read path
    std.debug.print("Enter path: ", .{});
    const path_line = readLineSync(stdin_file, &input_buffer) catch return null;
    const path = std.mem.trim(u8, path_line, &std.ascii.whitespace);

    if (path.len == 0) {
        std.debug.print("Error: Path cannot be empty\n", .{});
        return null;
    }

    // Detect and connect
    std.debug.print("\nConnecting to data source...\n", .{});

    var connection = Connection.init(allocator, "default", path) catch |err| {
        std.debug.print("Error connecting: {s}\n", .{@errorName(err)});
        return null;
    };
    errdefer connection.deinit();

    std.debug.print("Loading metadata...\n", .{});
    connection.loadMetadata() catch |err| {
        std.debug.print("Error loading metadata: {s}\n", .{@errorName(err)});
        connection.deinit();
        return null;
    };

    connection.printInfo();

    // Clear screen after connection info (keep it clean!)
    std.debug.print("\n\n", .{});

    return connection;
}

/// Read a line from stdin synchronously
fn readLineSync(file: std.fs.File, buffer: []u8) ![]const u8 {
    var pos: usize = 0;
    while (pos < buffer.len) {
        const n = try file.read(buffer[pos .. pos + 1]);
        if (n == 0) return error.EndOfStream;
        if (buffer[pos] == '\n') break;
        if (buffer[pos] == '\r') continue; // Skip CR
        pos += 1;
    }
    return buffer[0..pos];
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Print banner
    printBanner();

    var input_buffer: [4096]u8 = undefined;
    const stdin_file = std.fs.File.stdin();

    // Prompt for connection
    var connection: ?Connection = try promptConnection(allocator);
    defer if (connection) |*conn| conn.deinit();

    while (true) {
        // Print prompt with connection name
        if (connection) |conn| {
            std.debug.print("glacier({s})> ", .{conn.name});
        } else {
            std.debug.print("glacier> ", .{});
        }

        // Read line (simple version - read until we hit newline manually)
        var pos: usize = 0;
        while (pos < input_buffer.len) {
            const n = stdin_file.read(input_buffer[pos .. pos + 1]) catch break;
            if (n == 0) return; // EOF
            if (input_buffer[pos] == '\n') break;
            pos += 1;
        }

        if (pos == 0) continue;
        const line = input_buffer[0..pos];
        const trimmed = std.mem.trim(u8, line, &std.ascii.whitespace);
        if (trimmed.len == 0) continue;

        // Handle special commands
        if (std.mem.eql(u8, trimmed, "\\q") or std.ascii.eqlIgnoreCase(trimmed, "exit")) {
            std.debug.print("Bye!\n", .{});
            break;
        }

        if (std.ascii.eqlIgnoreCase(trimmed, "help")) {
            printHelp();
            continue;
        }

        if (std.mem.eql(u8, trimmed, "\\d") or std.mem.eql(u8, trimmed, "\\tables")) {
            if (connection) |conn| {
                conn.printInfo();
            } else {
                std.debug.print("No active connection. Use queries with FROM clause.\n", .{});
            }
            std.debug.print("\n", .{});
            continue;
        }

        if (std.mem.eql(u8, trimmed, "\\schema")) {
            if (connection) |conn| {
                if (conn.metadata) |meta| {
                    switch (meta) {
                        .iceberg => {
                            std.debug.print("\n\\schema command requires reloading metadata.\n", .{});
                            std.debug.print("Use \\refresh to reload, or run queries directly.\n", .{});
                            std.debug.print("Schema has {d} fields.\n\n", .{meta.iceberg.field_count});
                        },
                        .parquet => {
                            std.debug.print("\nParquet file schema:\n", .{});
                            std.debug.print("  {d} columns\n", .{meta.parquet.column_count});
                            std.debug.print("  {d} row groups\n", .{meta.parquet.row_group_count});
                            std.debug.print("  {d} total rows\n\n", .{meta.parquet.total_rows});
                        },
                        .avro => {
                            std.debug.print("Schema information not yet implemented for Avro files.\n\n", .{});
                        },
                    }
                } else {
                    std.debug.print("No metadata loaded for current connection.\n\n", .{});
                }
            } else {
                std.debug.print("No active connection.\n\n", .{});
            }
            continue;
        }

        if (std.mem.eql(u8, trimmed, "\\refresh")) {
            if (connection) |*conn| {
                std.debug.print("Refreshing metadata...\n", .{});
                conn.loadMetadata() catch |err| {
                    std.debug.print("Error refreshing metadata: {s}\n\n", .{@errorName(err)});
                    continue;
                };
                conn.printInfo();
                std.debug.print("\n", .{});
            } else {
                std.debug.print("No active connection.\n\n", .{});
            }
            continue;
        }

        // Parse display mode commands (head, tail, full)
        var display_mode: DisplayMode = .{ .default = {} };
        var actual_sql = trimmed;

        // DEPRECATED: head/tail commands removed in v0.8.2
        // Use SQL standard: SELECT * ORDER BY col LIMIT N (head) or ORDER BY col DESC LIMIT N (tail)
        if (std.ascii.startsWithIgnoreCase(trimmed, "head ") or std.ascii.startsWithIgnoreCase(trimmed, "tail ")) {
            std.debug.print("Error: head/tail commands are deprecated in v0.8.2-alpha\n", .{});
            std.debug.print("Use SQL standard instead:\n", .{});
            std.debug.print("  - For first N rows:  SELECT * LIMIT N\n", .{});
            std.debug.print("  - For last N rows:   SELECT * ORDER BY col DESC LIMIT N\n\n", .{});
            continue;
        } else if (std.ascii.startsWithIgnoreCase(trimmed, "full ")) {
            // Extract SQL: "full SELECT ..."
            actual_sql = std.mem.trim(u8, trimmed[5..], " \t");
            display_mode = .{ .full = {} };
        }

        // Try to execute SQL query
        if (std.ascii.startsWithIgnoreCase(actual_sql, "SELECT")) {
            executeQueryWithMode(allocator, actual_sql, display_mode, if (connection) |*conn| conn else null) catch |err| {
                std.debug.print("Error: {s}\n\n", .{@errorName(err)});
            };
            continue;
        }

        std.debug.print("Unknown command. Type 'help' for available commands.\n\n", .{});
    }
}

fn printBanner() void {
    std.debug.print("\n", .{});

    // ASCII Art Logo
    std.debug.print("           _____ _               _\n", .{});
    std.debug.print("          / ____| |             (_)\n", .{});
    std.debug.print("         | |  __| | __ _  ___ _  ___ _ __\n", .{});
    std.debug.print("         | | |_ | |/ _` |/ __| |/ _ \\ '__|\n", .{});
    std.debug.print("         | |__| | | (_| | (__| |  __/ |\n", .{});
    std.debug.print("          \\_____|_|\\__,_|\\___|_|\\___|_|\n", .{});
    std.debug.print("\n", .{});

    // Info box
    std.debug.print("+----------------------------------------------------------+\n", .{});
    std.debug.print("|                                                          |\n", .{});
    std.debug.print("|             Interactive OLAP Query Engine                |\n", .{});
    std.debug.print("|                                                          |\n", .{});
    std.debug.print("|                 Version: 0.8.2-alpha                     |\n", .{});
    std.debug.print("|          Type 'help' for commands, '\\q' to quit         |\n", .{});
    std.debug.print("|                                                          |\n", .{});
    std.debug.print("+----------------------------------------------------------+\n", .{});
    std.debug.print("\n", .{});
}

fn printHelp() void {
    std.debug.print("\n", .{});
    std.debug.print("Available commands:\n", .{});
    std.debug.print("  SELECT ... FROM ... WHERE ...  Execute SQL query\n", .{});
    std.debug.print("  SELECT ... LIMIT N             Return first N rows\n", .{});
    std.debug.print("  SELECT ... ORDER BY col DESC   Order results\n", .{});
    std.debug.print("  full <query>                   Show all rows (no 10-row limit)\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("Connection commands:\n", .{});
    std.debug.print("  \\d, \\tables                    Show current connection info\n", .{});
    std.debug.print("  \\schema                        Show table schema (Iceberg only)\n", .{});
    std.debug.print("  \\refresh                       Reload metadata for current connection\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("General commands:\n", .{});
    std.debug.print("  help                           Show this help message\n", .{});
    std.debug.print("  \\q, exit                       Quit REPL\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("Example queries:\n", .{});
    std.debug.print("  SELECT * LIMIT 10\n", .{});
    std.debug.print("  SELECT * WHERE age > 30\n", .{});
    std.debug.print("  SELECT * ORDER BY id DESC LIMIT 5\n", .{});
    std.debug.print("  SELECT category, COUNT(*) GROUP BY category\n", .{});
    std.debug.print("\n", .{});
}

fn executeQueryWithMode(allocator: std.mem.Allocator, sql: []const u8, mode: DisplayMode, connection: ?*Connection) !void {
    std.debug.print("\n", .{});
    std.debug.print("Executing: {s}\n", .{sql});
    std.debug.print("---------------------------------------------------------\n", .{});

    // Parse SQL using the real parser
    var query = glacier.sql.parseQuery(allocator, sql) catch |err| {
        std.debug.print("[ERROR] SQL Parse Error: {s}\n", .{@errorName(err)});
        std.debug.print("        Check your SQL syntax.\n\n", .{});
        return;
    };
    defer query.deinit();

    // If query has no FROM table and we have an active connection, use connection's path
    if (query.from_table.len == 0 and connection != null) {
        // Free empty string
        allocator.free(query.from_table);
        // Use connection's path
        query.from_table = try allocator.dupe(u8, connection.?.path);
    }

    // Execute the parsed query with display mode
    try executeParquetQueryWithMode(allocator, &query, mode);
}

/// Execute aggregate query on Iceberg table (processes multiple data files)
fn executeIcebergAggregateQuery(allocator: std.mem.Allocator, table_path: []const u8, query: *const glacier.sql.Query) !void {
    // Load Iceberg metadata
    const data_files = try loadIcebergDataFiles(allocator, table_path, query);
    defer {
        for (data_files) |file_path| {
            allocator.free(file_path);
        }
        allocator.free(data_files);
    }

    if (data_files.len == 0) {
        std.debug.print("[INFO] No data files to read (empty table)\n\n", .{});
        return;
    }

    const aggregates = query.aggregates.?;

    // Initialize accumulators
    const accumulators = try allocator.alloc(AggregateAccumulator, aggregates.len);
    defer allocator.free(accumulators);

    for (accumulators, aggregates) |*acc, agg_func| {
        acc.* = AggregateAccumulator.init(agg_func.func_type);
    }

    // Process each data file
    for (data_files) |file_path| {
        var reader = glacier.parquet.Reader.open(allocator, file_path) catch continue;
        defer reader.close();

        try reader.readMetadata();
        const metadata = reader.metadata.?;

        // Process all row groups in this file
        for (metadata.row_groups) |rg| {
            try processRowGroupForAggregates(allocator, &reader, metadata, rg, aggregates, accumulators, query.where_clause);
        }
    }

    // Print results (same as regular aggregate query)
    var headers = try allocator.alloc([]const u8, aggregates.len);
    defer {
        for (headers) |header| {
            allocator.free(header);
        }
        allocator.free(headers);
    }

    for (aggregates, 0..) |agg_func, i| {
        const func_name = switch (agg_func.func_type) {
            .count => "COUNT",
            .sum => "SUM",
            .avg => "AVG",
            .min => "MIN",
            .max => "MAX",
        };
        const col_name = agg_func.column orelse "*";
        headers[i] = try std.fmt.allocPrint(allocator, "{s}({s})", .{ func_name, col_name });
    }

    const widths = try allocator.alloc(usize, aggregates.len);
    defer allocator.free(widths);

    for (headers, accumulators, 0..) |header, acc, i| {
        const result = acc.finalize();
        const result_str = try std.fmt.allocPrint(allocator, "{d:.2}", .{result});
        defer allocator.free(result_str);
        widths[i] = @max(getDisplayWidth(header), getDisplayWidth(result_str)) + 2;
    }

    printTableHeader(headers, widths);

    std.debug.print("|", .{});
    for (accumulators, widths) |acc, width| {
        const result = acc.finalize();
        const result_str = try std.fmt.allocPrint(allocator, "{d:.2}", .{result});
        defer allocator.free(result_str);
        printCentered(result_str, width);
        std.debug.print("|", .{});
    }
    std.debug.print("\n", .{});

    printTableFooter(widths, 1, 1);
    std.debug.print("\n", .{});
}

/// Execute aggregate query (COUNT, SUM, AVG, MIN, MAX)
fn executeAggregateQuery(allocator: std.mem.Allocator, query: *const glacier.sql.Query) !void {
    // Check if GROUP BY is present
    if (query.group_by) |_| {
        return executeGroupByQuery(allocator, query);
    }

    // No GROUP BY: simple aggregation
    const table_name = query.from_table;

    // Check if this is an Iceberg table (directory) or Parquet file
    const is_iceberg = blk: {
        const stat = std.fs.cwd().statFile(table_name) catch |err| {
            if (err == error.IsDir) break :blk true;
            break :blk false;
        };
        _ = stat;
        break :blk false;
    };

    if (is_iceberg) {
        // Process as Iceberg table with multiple files
        return executeIcebergAggregateQuery(allocator, table_name, query);
    }

    // Open Parquet file
    var reader = glacier.parquet.Reader.open(allocator, table_name) catch |err| {
        std.debug.print("\n", .{});
        std.debug.print("[ERROR] Failed to open '{s}'\n", .{table_name});
        std.debug.print("        Error: {s}\n", .{@errorName(err)});
        std.debug.print("\n", .{});
        return err;
    };
    defer reader.close();

    try reader.readMetadata();
    const metadata = reader.metadata.?;

    // Initialize accumulators for each aggregate function
    const aggregates = query.aggregates.?;
    const accumulators = try allocator.alloc(AggregateAccumulator, aggregates.len);
    defer allocator.free(accumulators);

    for (accumulators, aggregates) |*acc, agg_func| {
        acc.* = AggregateAccumulator.init(agg_func.func_type);
    }

    // Process all row groups
    for (metadata.row_groups) |rg| {
        try processRowGroupForAggregates(allocator, &reader, metadata, rg, aggregates, accumulators, query.where_clause);
    }

    // Finalize and print results
    // Build column headers
    var headers = try allocator.alloc([]const u8, aggregates.len);
    defer {
        for (headers) |header| {
            allocator.free(header);
        }
        allocator.free(headers);
    }

    for (aggregates, 0..) |agg_func, i| {
        const func_name = switch (agg_func.func_type) {
            .count => "COUNT",
            .sum => "SUM",
            .avg => "AVG",
            .min => "MIN",
            .max => "MAX",
        };
        const col_name = agg_func.column orelse "*";
        headers[i] = try std.fmt.allocPrint(allocator, "{s}({s})", .{ func_name, col_name });
    }

    // Calculate individual width for EACH column
    const widths = try allocator.alloc(usize, aggregates.len);
    defer allocator.free(widths);

    for (headers, accumulators, 0..) |header, acc, i| {
        const result = acc.finalize();
        const result_str = try std.fmt.allocPrint(allocator, "{d:.2}", .{result});
        defer allocator.free(result_str);
        widths[i] = @max(getDisplayWidth(header), getDisplayWidth(result_str)) + 2;
    }

    // Print header
    printTableHeader(headers, widths);

    // Print results
    std.debug.print("|", .{});
    for (accumulators, widths) |acc, width| {
        const result = acc.finalize();
        const result_str = try std.fmt.allocPrint(allocator, "{d:.2}", .{result});
        defer allocator.free(result_str);
        printCentered(result_str, width);
        std.debug.print("|", .{});
    }
    std.debug.print("\n", .{});

    // Print footer
    printTableFooter(widths, 1, 1);
    std.debug.print("\n", .{});
}

/// Process a single row group for aggregate computation
fn processRowGroupForAggregates(
    allocator: std.mem.Allocator,
    reader: *glacier.parquet.Reader,
    metadata: glacier.parquet.FileMetaData,
    rg: glacier.parquet.RowGroup,
    aggregates: []const glacier.sql.AggregateFunc,
    accumulators: []AggregateAccumulator,
    where_clause: ?*const glacier.sql.Expr,
) !void {
    var string_arena = std.heap.ArenaAllocator.init(allocator);
    defer string_arena.deinit();

    // Create schema mapper to dynamically resolve column names
    var schema_mapper = try SchemaMapper.init(allocator, metadata);
    defer schema_mapper.deinit();

    // Determine which columns we need to read
    var columns_needed = std.StringHashMap(void).init(allocator);
    defer columns_needed.deinit();

    // Add columns needed for aggregates
    for (aggregates) |agg| {
        if (agg.column) |col_name| {
            try columns_needed.put(col_name, {});
        }
    }

    // Add columns needed for WHERE clause
    if (where_clause) |expr| {
        try collectColumnsFromExpr(expr, &columns_needed);
    }

    // Read needed columns dynamically
    var columns: std.ArrayListUnmanaged(ColumnData) = .{};
    defer {
        for (columns.items) |col| {
            allocator.free(col.values);
        }
        columns.deinit(allocator);
    }

    var col_iter = columns_needed.keyIterator();
    while (col_iter.next()) |col_name| {
        const col_idx = schema_mapper.getColumnIndex(col_name.*) orelse return error.UnknownColumn;
        const col_values = try readColumnDynamic(allocator, string_arena.allocator(), reader, metadata, rg, col_idx, null);
        try columns.append(allocator, .{ .name = col_name.*, .values = col_values });
    }

    // If no columns were read (COUNT(*) with no WHERE), read first column to determine row count
    // This handles cases where rg.num_rows is 0 or incorrect
    if (columns.items.len == 0 and schema_mapper.column_names.len > 0) {
        const col_values = try readColumnDynamic(allocator, string_arena.allocator(), reader, metadata, rg, 0, null);
        try columns.append(allocator, .{ .name = schema_mapper.column_names[0], .values = col_values });
    }

    // Determine total rows from actual column data (don't trust rg.num_rows which may be 0)
    const total_rows = if (columns.items.len > 0) columns.items[0].values.len else 0;

    // Process each row
    for (0..total_rows) |i| {
        // Evaluate WHERE clause if present
        if (where_clause) |where_expr| {
            const passes = evaluateWhereDynamic(where_expr, columns.items, i);
            if (!passes) continue;
        }

        // Accumulate aggregates
        for (aggregates, accumulators) |agg_func, *acc| {
            switch (agg_func.func_type) {
                .count => {
                    // COUNT(*) or COUNT(column)
                    acc.accumulateCount();
                },
                .sum, .avg, .min, .max => {
                    // Get the column value
                    const col_name = agg_func.column orelse return error.InvalidAggregate;

                    // Find the column in our read columns
                    var value: f64 = 0;
                    for (columns.items) |col| {
                        if (std.mem.eql(u8, col.name, col_name)) {
                            value = col.values[i].toFloat();
                            break;
                        }
                    }

                    acc.accumulate(value);
                },
            }
        }
    }
}

fn executeQuery(allocator: std.mem.Allocator, sql: []const u8) !void {
    std.debug.print("\n", .{});
    std.debug.print("Executing: {s}\n", .{sql});
    std.debug.print("---------------------------------------------------------\n", .{});

    // Parse SQL using the real parser
    var query = glacier.sql.parseQuery(allocator, sql) catch |err| {
        std.debug.print("[ERROR] SQL Parse Error: {s}\n", .{@errorName(err)});
        std.debug.print("        Check your SQL syntax.\n\n", .{});
        return;
    };
    defer query.deinit();

    // Execute the parsed query
    try executeParquetQuery(allocator, &query);
}

fn executeParquetQueryWithMode(allocator: std.mem.Allocator, query: *const glacier.sql.Query, mode: DisplayMode) !void {

    // Check if this is an aggregate query
    if (query.aggregates) |_| {
        return executeAggregateQuery(allocator, query);
    }

    const table_name = query.from_table;

    // Check if file/directory exists
    std.fs.cwd().access(table_name, .{}) catch {
        std.debug.print("\n", .{});
        std.debug.print("[ERROR] '{s}' not found!\n", .{table_name});
        std.debug.print("\n", .{});
        std.debug.print("Current directory: ", .{});
        var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
        const cwd = std.fs.cwd().realpath(".", &cwd_buf) catch ".";
        std.debug.print("{s}\n\n", .{cwd});
        return;
    };

    // Detect data source type
    const source_type = detectDataSource(table_name) catch |err| {
        std.debug.print("\n", .{});
        std.debug.print("[ERROR] Failed to detect format of '{s}'\n", .{table_name});
        std.debug.print("        Error: {s}\n", .{@errorName(err)});
        std.debug.print("\n", .{});
        return err;
    };

    // Execute based on source type
    switch (source_type) {
        .iceberg_table => {
            std.debug.print("🔧 Detected: Iceberg Table\n", .{});
            return executeIcebergQuery(allocator, table_name, query, mode);
        },
        .avro_file => {
            std.debug.print("🔧 Detected: Avro File\n", .{});
            return executeAvroQuery(allocator, table_name, query, mode);
        },
        .parquet_file => {
            // Continue with existing Parquet logic
        },
    }

    // Open Parquet file
    var reader = glacier.parquet.Reader.open(allocator, table_name) catch |err| {
        std.debug.print("\n", .{});
        std.debug.print("[ERROR] Failed to open '{s}'\n", .{table_name});
        std.debug.print("        Error: {s}\n", .{@errorName(err)});
        std.debug.print("\n", .{});
        std.debug.print("The file might be corrupted or in an unsupported format.\n\n", .{});
        return err;
    };
    defer reader.close();

    try reader.readMetadata();
    const metadata = reader.metadata.?;

    if (metadata.row_groups.len == 0) {
        std.debug.print("[ERROR] No data in file\n\n", .{});
        return;
    }

    // Use FileMetaData.num_rows (more reliable than summing row groups)
    const total_rows: i64 = metadata.num_rows;

    // Print info about multi-row-group files
    if (metadata.row_groups.len > 1) {
        std.debug.print("File: {d} row groups, {d} total rows\n", .{ metadata.row_groups.len, total_rows });
        std.debug.print("---------------------------------------------------------\n", .{});
    }

    // Determine which row groups to read based on LIMIT clause or display mode
    const rows_to_show: usize = if (query.limit) |limit|
        limit // ✅ USE LIMIT from SQL query!
    else switch (mode) {
        .default => 10,  // Default: show 10 rows
        .full => @intCast(total_rows),  // Full: show all rows
    };

    // Determine which columns to read
    const is_select_star = query.select_columns.len == 1 and std.mem.eql(u8, query.select_columns[0], "*");

    // Multi-row-group reading strategy (simplified in v0.8.2)
    switch (mode) {
        .default => {
            // Read row groups from the start until we have enough rows (LIMIT support)
            // Use ACTUAL rows displayed (not metadata) for accurate early termination
            var rows_accumulated: usize = 0;
            for (metadata.row_groups, 0..) |rg, rg_idx| {
                // Early termination: stop if we've shown enough rows
                if (rows_accumulated >= rows_to_show) break;

                // Calculate how many more rows we need
                const rows_needed = rows_to_show - rows_accumulated;

                // Read and display this row group
                const rows_displayed = if (is_select_star)
                    try readAllColumnsWithMode(allocator, &reader, metadata, rg, .{ .default = {} }, query.where_clause, query.order_by, rows_needed)
                else
                    try readSpecificColumnsWithMode(allocator, &reader, metadata, rg, query.select_columns, .{ .default = {} }, query.where_clause);

                rows_accumulated += rows_displayed;

                // Show continuation indicator if there are more row groups AND we need more rows
                if (rg_idx < metadata.row_groups.len - 1 and rows_accumulated < rows_to_show) {
                    std.debug.print("... continuing from row group {d} ...\n", .{rg_idx + 2});
                }
            }
        },
        .full => {
            // Read all row groups
            for (metadata.row_groups, 0..) |rg, rg_idx| {
                if (is_select_star) {
                    _ = try readAllColumnsWithMode(allocator, &reader, metadata, rg, .{ .full = {} }, query.where_clause, query.order_by, query.limit);
                } else {
                    _ = try readSpecificColumnsWithMode(allocator, &reader, metadata, rg, query.select_columns, .{ .full = {} }, query.where_clause);
                }
                if (rg_idx < metadata.row_groups.len - 1) {
                    std.debug.print("... continuing from row group {d} ...\n", .{rg_idx + 2});
                }
            }
        },
    }

    std.debug.print("\n", .{});
}

fn executeParquetQuery(allocator: std.mem.Allocator, query: *const glacier.sql.Query) !void {
    return executeParquetQueryWithMode(allocator, query, .{ .default = {} });
}

/// Display dynamic table with any columns
fn displayDynamicTable(columns: []const ColumnData, matched_rows: []const usize, total_rows: usize) !void {
    if (columns.len == 0) return;

    const allocator = std.heap.page_allocator;

    // Calculate INDIVIDUAL width for EACH column
    const widths = try allocator.alloc(usize, columns.len);
    defer allocator.free(widths);

    // Maximum column width for better UX (similar to DuckDB)
    const MAX_COLUMN_WIDTH: usize = 50;

    for (columns, 0..) |col, col_idx| {
        var max_width: usize = getDisplayWidth(col.name); // Start with column name

        // Find max width from DATA - if data is bigger, it wins
        for (matched_rows) |row_idx| {
            const val_str = try col.values[row_idx].toString(allocator);
            defer allocator.free(val_str);
            max_width = @max(max_width, getDisplayWidth(val_str));
        }

        // Limit to MAX_COLUMN_WIDTH for better readability
        max_width = @min(max_width, MAX_COLUMN_WIDTH);
        widths[col_idx] = max_width + 2; // +2 for padding
    }

    // Build column names array
    var col_names = try allocator.alloc([]const u8, columns.len);
    defer allocator.free(col_names);

    for (columns, 0..) |col, i| {
        col_names[i] = col.name;
    }

    // Print header
    std.debug.print("---------------------------------------------------------\n", .{});
    printTableHeader(col_names, widths);

    // Print rows
    for (matched_rows) |row_idx| {
        std.debug.print("|", .{});
        for (columns, widths) |col, width| {
            const val_str = try col.values[row_idx].toString(allocator);
            defer allocator.free(val_str);

            // Truncate text if it exceeds column width
            const display_str = try truncateText(allocator, val_str, width - 2); // -2 for padding
            defer allocator.free(display_str);

            printCentered(display_str, width);
            std.debug.print("|", .{});
        }
        std.debug.print("\n", .{});
    }

    // Print footer
    printTableFooter(widths, total_rows, matched_rows.len);
}

/// Sort matched_rows based on ORDER BY clauses
fn sortRowsByOrder(
    allocator: std.mem.Allocator,
    columns: []const ColumnData,
    matched_rows: *std.ArrayListUnmanaged(usize),
    order_clauses: []const glacier.sql.OrderByClause,
) !void {
    _ = allocator;

    if (order_clauses.len == 0) return;

    // Find the column to sort by (using first ORDER BY clause)
    const order_clause = order_clauses[0];
    var sort_col_idx: ?usize = null;
    for (columns, 0..) |col, idx| {
        if (std.mem.eql(u8, col.name, order_clause.column)) {
            sort_col_idx = idx;
            break;
        }
    }

    if (sort_col_idx == null) {
        std.debug.print("[WARNING] ORDER BY column '{s}' not found\n", .{order_clause.column});
        return;
    }

    const col_idx = sort_col_idx.?;
    const col_values = columns[col_idx].values;
    const direction = order_clause.direction;

    // Simple bubble sort (good enough for now, can optimize later)
    const rows = matched_rows.items;
    var i: usize = 0;
    while (i < rows.len) : (i += 1) {
        var j: usize = i + 1;
        while (j < rows.len) : (j += 1) {
            const row_i = rows[i];
            const row_j = rows[j];
            const val_i = col_values[row_i];
            const val_j = col_values[row_j];

            const should_swap = switch (direction) {
                .asc => val_i.compareOrder(val_j) == .gt,
                .desc => val_i.compareOrder(val_j) == .lt,
            };

            if (should_swap) {
                rows[i] = row_j;
                rows[j] = row_i;
            }
        }
    }
}

fn readAllColumnsWithMode(allocator: std.mem.Allocator, reader: *glacier.parquet.Reader, metadata: glacier.parquet.FileMetaData, rg: glacier.parquet.RowGroup, mode: DisplayMode, where_clause: ?*const glacier.sql.Expr, order_by: ?[]const glacier.sql.OrderByClause, limit: ?usize) !usize {
    // DYNAMIC VERSION: Read ALL columns from schema (no hard-coding)

    // Create arena for all string allocations in this query
    var string_arena = std.heap.ArenaAllocator.init(allocator);
    defer string_arena.deinit();

    // Get schema mapper for dynamic column resolution
    var schema_mapper = try SchemaMapper.init(allocator, metadata);
    defer schema_mapper.deinit();

    // Read ALL columns dynamically
    const num_columns = rg.columns.len;
    var all_columns = std.ArrayList(ColumnData){};
    defer {
        for (all_columns.items) |col| {
            allocator.free(col.values);
        }
        all_columns.deinit(allocator);
    }

    // Determine max rows to read based on mode and limit
    // OPTIMIZATION: Pass limit to stop reading pages early (for LIMIT 5, only read 5 rows)
    // IMPORTANT: If ORDER BY is present, we MUST read all rows before sorting!
    const max_rows_to_read = if (order_by != null)
        null  // ORDER BY: must read all rows to sort
    else switch (mode) {
        .default => if (limit) |lim| lim else 10,  // Default/LIMIT: use limit or 10
        .full => null,   // Full: read everything
    };

    for (0..num_columns) |col_idx| {
        const col_name = schema_mapper.getColumnName(col_idx) orelse continue;
        const col_values = try readColumnDynamic(allocator, string_arena.allocator(), reader, metadata, rg, col_idx, max_rows_to_read);
        try all_columns.append(allocator, .{ .name = col_name, .values = col_values });
    }

    if (all_columns.items.len == 0) {
        std.debug.print("[ERROR] No columns found in table\n", .{});
        return 0; // No rows displayed
    }

    // Determine actual row count from first column (all columns should have same length)
    const total_rows: usize = if (all_columns.items.len > 0) all_columns.items[0].values.len else 0;

    // Apply WHERE filter
    var matched_rows: std.ArrayListUnmanaged(usize) = .{};
    defer matched_rows.deinit(allocator);

    for (0..total_rows) |i| {
        const passes = if (where_clause) |expr|
            evaluateWhereDynamic(expr, all_columns.items, i)
        else
            true;

        if (passes) {
            try matched_rows.append(allocator, i);
        }
    }

    // Apply ORDER BY if present
    if (order_by) |order_clauses| {
        try sortRowsByOrder(allocator, all_columns.items, &matched_rows, order_clauses);
    }

    // Apply LIMIT if present
    const rows_to_display = if (limit) |lim|
        if (lim < matched_rows.items.len) matched_rows.items[0..lim] else matched_rows.items
    else
        matched_rows.items;

    // Display results using dynamic table formatter
    // Show total from file metadata, not just this row group
    const file_total_rows: usize = @intCast(metadata.num_rows);
    try displayDynamicTable(all_columns.items, rows_to_display, file_total_rows);

    // Return number of rows actually displayed
    return rows_to_display.len;
}

fn readAllColumns(allocator: std.mem.Allocator, reader: *glacier.parquet.Reader, metadata: glacier.parquet.FileMetaData, rg: glacier.parquet.RowGroup) !void {
    _ = try readAllColumnsWithMode(allocator, reader, metadata, rg, .{ .default = {} }, null, null, null);
}

/// Read ALL columns, filter by WHERE, then display only selected columns
fn readAllColumnsAndFilterDisplay(
    allocator: std.mem.Allocator,
    reader: *glacier.parquet.Reader,
    metadata: glacier.parquet.FileMetaData,
    rg: glacier.parquet.RowGroup,
    selected_columns: [][]const u8,
    mode: DisplayMode,
    where_clause: *const glacier.sql.Expr,
) !usize {
    // Use dynamic system instead of hard-coded columns
    var string_arena = std.heap.ArenaAllocator.init(allocator);
    defer string_arena.deinit();

    // Create schema mapper
    var schema_mapper = try SchemaMapper.init(allocator, metadata);
    defer schema_mapper.deinit();

    // Determine which columns we need to read for WHERE evaluation
    var columns_needed = std.StringHashMap(void).init(allocator);
    defer columns_needed.deinit();

    // Add columns referenced in WHERE clause
    try collectColumnsFromExpr(where_clause, &columns_needed);

    // Also need all selected columns for display
    for (selected_columns) |col_name| {
        try columns_needed.put(col_name, {});
    }

    // Read needed columns dynamically
    var all_columns: std.ArrayListUnmanaged(ColumnData) = .{};
    defer {
        for (all_columns.items) |col| {
            allocator.free(col.values);
        }
        all_columns.deinit(allocator);
    }

    var col_iter = columns_needed.keyIterator();
    while (col_iter.next()) |col_name| {
        const col_idx = schema_mapper.getColumnIndex(col_name.*) orelse {
            std.debug.print("[ERROR] Unknown column: {s}\n", .{col_name.*});
            return error.UnknownColumn;
        };

        const col_values = try readColumnDynamic(allocator, string_arena.allocator(), reader, metadata, rg, col_idx, null);
        try all_columns.append(allocator, .{ .name = col_name.*, .values = col_values });
    }

    // Determine total rows
    const total_rows: usize = if (all_columns.items.len > 0) all_columns.items[0].values.len else 0;

    // Filter rows using WHERE clause and collect matching indices
    var matched_rows: std.ArrayListUnmanaged(usize) = .{};
    defer matched_rows.deinit(allocator);

    for (0..total_rows) |i| {
        const passes = evaluateWhereDynamic(where_clause, all_columns.items, i);
        if (passes) {
            try matched_rows.append(allocator, i);
        }
    }

    // Calculate display range based on mode
    const num_matched = matched_rows.items.len;
    const display_count: usize = switch (mode) {
        .default => @min(num_matched, 10),
        .full => num_matched,
    };

    const start_idx: usize = 0;  // Always start from beginning (removed tail support)
    const end_idx = @min(start_idx + display_count, num_matched);

    // Calculate individual width for EACH column
    const widths = try allocator.alloc(usize, selected_columns.len);
    defer allocator.free(widths);

    const MAX_COLUMN_WIDTH: usize = 50;

    for (selected_columns, 0..) |col_name, col_idx| {
        var max_width: usize = getDisplayWidth(col_name);

        // Find max width for this specific column
        for (all_columns.items) |col| {
            if (std.mem.eql(u8, col.name, col_name)) {
                for (start_idx..end_idx) |idx| {
                    const row_idx = matched_rows.items[idx];
                    if (row_idx < col.values.len) {
                        const val_str = try col.values[row_idx].toString(allocator);
                        defer allocator.free(val_str);
                        max_width = @max(max_width, getDisplayWidth(val_str));
                    }
                }
                break;
            }
        }

        max_width = @min(max_width, MAX_COLUMN_WIDTH);
        widths[col_idx] = max_width + 2; // +2 for padding
    }

    // Print header
    printTableHeader(selected_columns, widths);

    // Print matched rows
    for (start_idx..end_idx) |idx| {
        const row_idx = matched_rows.items[idx];

        std.debug.print("|", .{});
        for (selected_columns, widths) |col_name, width| {
            // Find column in all_columns
            var found = false;
            for (all_columns.items) |col| {
                if (std.mem.eql(u8, col.name, col_name)) {
                    const val_str = try col.values[row_idx].toString(allocator);
                    defer allocator.free(val_str);

                    const display_str = try truncateText(allocator, val_str, width - 2);
                    defer allocator.free(display_str);

                    printCentered(display_str, width);
                    std.debug.print("|", .{});
                    found = true;
                    break;
                }
            }
            if (!found) {
                printCentered("NULL", width);
                std.debug.print("|", .{});
            }
        }
        std.debug.print("\n", .{});
    }

    // Show total from file metadata, not just this row group
    const file_total_rows: usize = @intCast(metadata.num_rows);
    printTableFooter(widths, file_total_rows, num_matched);

    // Return number of rows actually displayed
    return num_matched;
}
fn readSpecificColumnsWithMode(allocator: std.mem.Allocator, reader: *glacier.parquet.Reader, metadata: glacier.parquet.FileMetaData, rg: glacier.parquet.RowGroup, column_names: [][]const u8, mode: DisplayMode, where_clause: ?*const glacier.sql.Expr) !usize {
    // OPTIMIZATION: If WHERE clause is present, we need to read ALL columns to evaluate it
    // Then we filter and only display the requested columns
    // This is simpler than tracking which columns are needed for WHERE vs SELECT

    if (where_clause != null) {
        // Read ALL columns, filter by WHERE, then display only selected columns
        return readAllColumnsAndFilterDisplay(allocator, reader, metadata, rg, column_names, mode, where_clause.?);
    }

    // NO WHERE clause: Proceed with optimized read of only selected columns
    // Create arena for all string allocations in this query
    var string_arena = std.heap.ArenaAllocator.init(allocator);
    defer string_arena.deinit();

    // Create schema mapper to dynamically resolve column names
    var schema_mapper = try SchemaMapper.init(allocator, metadata);
    defer schema_mapper.deinit();

    // Get actual row count from first column metadata (rg.num_rows may be 0!)
    const actual_total_rows: usize = if (rg.columns.len > 0)
        @intCast(rg.columns[0].meta_data.num_values)
    else
        0;

    // Calculate number of rows to display based on mode
    const num_rows: usize = switch (mode) {
        .default => @min(actual_total_rows, 10),
        .full => actual_total_rows,
    };

    // OPTIMIZATION: Only read first N rows for default mode
    const max_rows_to_read: ?usize = switch (mode) {
        .default => 10,  // Default: read 10 rows
        .full => null,   // Full: read everything
    };

    // Read all requested columns dynamically
    var all_values: std.ArrayListUnmanaged(ColumnData) = .{};
    defer {
        for (all_values.items) |col| {
            allocator.free(col.values);
        }
        all_values.deinit(allocator);
    }

    for (column_names) |col_name| {
        const col_idx = schema_mapper.getColumnIndex(col_name) orelse {
            std.debug.print("[ERROR] Unknown column: {s}\n", .{col_name});
            return error.UnknownColumn;
        };

        const col_values = try readColumnDynamic(allocator, string_arena.allocator(), reader, metadata, rg, col_idx, max_rows_to_read);
        try all_values.append(allocator, .{ .name = col_name, .values = col_values });
    }

    // Determine actual row count
    var actual_rows: usize = 0;
    for (all_values.items) |col_data| {
        actual_rows = @max(actual_rows, col_data.values.len);
    }

    // Calculate display range (always start from beginning - removed tail support)
    const start_idx: usize = 0;
    const end_idx = @min(start_idx + num_rows, actual_rows);

    // Calculate individual width for EACH column
    const widths = try allocator.alloc(usize, column_names.len);
    defer allocator.free(widths);

    const MAX_COLUMN_WIDTH: usize = 50;

    for (column_names, 0..) |col_name, col_idx| {
        var max_width: usize = getDisplayWidth(col_name);

        // Find max width for this specific column
        for (all_values.items) |col_data| {
            if (std.mem.eql(u8, col_data.name, col_name)) {
                for (start_idx..end_idx) |row_idx| {
                    if (row_idx < col_data.values.len) {
                        const val_str = try col_data.values[row_idx].toString(allocator);
                        defer allocator.free(val_str);
                        max_width = @max(max_width, getDisplayWidth(val_str));
                    }
                }
                break;
            }
        }

        max_width = @min(max_width, MAX_COLUMN_WIDTH);
        widths[col_idx] = max_width + 2; // +2 for padding
    }

    // Print header
    printTableHeader(column_names, widths);

    // Print rows
    for (start_idx..end_idx) |row_idx| {
        std.debug.print("|", .{});
        for (all_values.items, widths) |col_data, width| {
            if (row_idx < col_data.values.len) {
                const val_str = try col_data.values[row_idx].toString(allocator);
                defer allocator.free(val_str);

                const display_str = try truncateText(allocator, val_str, width - 2);
                defer allocator.free(display_str);

                printCentered(display_str, width);
                std.debug.print("|", .{});
            } else {
                printCentered("NULL", width);
                std.debug.print("|", .{});
            }
        }
        std.debug.print("\n", .{});
    }

    // Show total from file metadata, not just this row group
    const file_total_rows: usize = @intCast(metadata.num_rows);
    printTableFooter(widths, file_total_rows, end_idx - start_idx);

    // Return number of rows actually displayed
    return end_idx - start_idx;
}

fn readSpecificColumns(allocator: std.mem.Allocator, reader: *glacier.parquet.Reader, metadata: glacier.parquet.FileMetaData, rg: glacier.parquet.RowGroup, column_names: [][]const u8) !void {
    _ = try readSpecificColumnsWithMode(allocator, reader, metadata, rg, column_names, .{ .default = {} }, null);
}

/// Column data structure used for dynamic column reading
const ColumnData = struct {
    name: []const u8,
    values: []DynamicColumnValue,
};

/// Aggregate accumulator for computing COUNT, SUM, AVG, MIN, MAX
const AggregateAccumulator = struct {
    func_type: glacier.sql.AggregateType,
    count: i64,
    sum: f64,
    min_val: f64,
    max_val: f64,

    fn init(func_type: glacier.sql.AggregateType) AggregateAccumulator {
        return .{
            .func_type = func_type,
            .count = 0,
            .sum = 0,
            .min_val = std.math.inf(f64),
            .max_val = -std.math.inf(f64),
        };
    }

    fn accumulate(self: *AggregateAccumulator, value: f64) void {
        self.count += 1;
        self.sum += value;
        if (value < self.min_val) self.min_val = value;
        if (value > self.max_val) self.max_val = value;
    }

    fn accumulateCount(self: *AggregateAccumulator) void {
        self.count += 1;
    }

    fn finalize(self: *const AggregateAccumulator) f64 {
        return switch (self.func_type) {
            .count => @floatFromInt(self.count),
            .sum => self.sum,
            .avg => if (self.count > 0) self.sum / @as(f64, @floatFromInt(self.count)) else 0,
            .min => if (self.count > 0) self.min_val else 0,
            .max => if (self.count > 0) self.max_val else 0,
        };
    }
};

/// Count the display width of a UTF-8 string (number of visible characters)
fn getDisplayWidth(text: []const u8) usize {
    var count: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const byte = text[i];
        // UTF-8 byte length detection
        const char_len: usize = if ((byte & 0x80) == 0)
            1 // ASCII (0xxxxxxx)
        else if ((byte & 0xE0) == 0xC0)
            2 // 2-byte (110xxxxx)
        else if ((byte & 0xF0) == 0xE0)
            3 // 3-byte (1110xxxx)
        else if ((byte & 0xF8) == 0xF0)
            4 // 4-byte (11110xxx)
        else
            1; // Invalid, treat as 1

        i += char_len;
        count += 1; // Each UTF-8 character counts as 1 display position
    }
    return count;
}

/// Helper to print centered text within a given width
/// Truncate text if needed and add ellipsis
fn truncateText(allocator: std.mem.Allocator, text: []const u8, max_width: usize) ![]const u8 {
    const text_width = getDisplayWidth(text);
    if (text_width <= max_width) {
        return try allocator.dupe(u8, text);
    }

    // Text is too long, truncate and add "..."
    const ellipsis = "...";
    const available = if (max_width > 3) max_width - 3 else 0;

    if (available == 0) {
        return try allocator.dupe(u8, ellipsis);
    }

    // Find the byte position where we should cut
    var byte_count: usize = 0;
    var char_count: usize = 0;

    while (byte_count < text.len and char_count < available) {
        const char_len = std.unicode.utf8ByteSequenceLength(text[byte_count]) catch 1;
        if (byte_count + char_len > text.len) break;
        byte_count += char_len;
        char_count += 1;
    }

    const result = try allocator.alloc(u8, byte_count + ellipsis.len);
    @memcpy(result[0..byte_count], text[0..byte_count]);
    @memcpy(result[byte_count..], ellipsis);
    return result;
}

fn printCentered(text: []const u8, width: usize) void {
    const text_display_width = getDisplayWidth(text);
    const text_len = @min(text_display_width, width);
    const padding = width - text_len;
    const left_pad = padding / 2;
    const right_pad = padding - left_pad;

    for (0..left_pad) |_| std.debug.print(" ", .{});
    std.debug.print("{s}", .{text});
    for (0..right_pad) |_| std.debug.print(" ", .{});
}

/// Print separator line with dynamic widths
fn printTableSeparator(widths: []const usize) void {
    std.debug.print("+", .{});
    for (widths, 0..) |width, i| {
        for (0..width) |_| {
            std.debug.print("-", .{});
        }
        if (i < widths.len - 1) {
            std.debug.print("+", .{});
        }
    }
    std.debug.print("+\n", .{});
}

fn printTableHeader(column_names: [][]const u8, widths: []const usize) void {
    // Print top border
    printTableSeparator(widths);

    // Print column names (centered)
    std.debug.print("|", .{});
    for (column_names, widths) |col_name, width| {
        printCentered(col_name, width);
        std.debug.print("|", .{});
    }
    std.debug.print("\n", .{});

    // Print separator
    printTableSeparator(widths);
}

fn printTableFooter(widths: []const usize, total_rows: usize, shown_rows: usize) void {
    printTableSeparator(widths);
    if (total_rows == shown_rows) {
        std.debug.print("({d} rows)\n", .{total_rows});
    } else {
        std.debug.print("({d} rows total, showing {d})\n", .{ total_rows, shown_rows });
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// GROUP BY STRUCTURES
// ═══════════════════════════════════════════════════════════════════════════

/// Represents a grouping key (one or more column values)
const GroupKey = struct {
    values: [][]const u8, // Array of string values for each GROUP BY column

    pub fn hash(self: GroupKey) u64 {
        var hasher = std.hash.Wyhash.init(0);
        for (self.values) |val| {
            hasher.update(val);
        }
        return hasher.final();
    }

    pub fn eql(self: GroupKey, other: GroupKey) bool {
        if (self.values.len != other.values.len) return false;
        for (self.values, other.values) |a, b| {
            if (!std.mem.eql(u8, a, b)) return false;
        }
        return true;
    }

    pub fn deinit(self: *GroupKey, allocator: std.mem.Allocator) void {
        for (self.values) |val| allocator.free(val);
        allocator.free(self.values);
    }
};

/// Hash context for GroupKey
const GroupKeyContext = struct {
    pub fn hash(_: GroupKeyContext, key: GroupKey) u64 {
        return key.hash();
    }

    pub fn eql(_: GroupKeyContext, a: GroupKey, b: GroupKey) bool {
        return a.eql(b);
    }
};

/// Aggregates for a single group
const GroupedAggregates = struct {
    accumulators: []AggregateAccumulator,

    pub fn init(allocator: std.mem.Allocator, aggregates: []const glacier.sql.AggregateFunc) !GroupedAggregates {
        const accumulators = try allocator.alloc(AggregateAccumulator, aggregates.len);
        for (accumulators, aggregates) |*acc, agg_func| {
            acc.* = AggregateAccumulator.init(agg_func.func_type);
        }
        return .{ .accumulators = accumulators };
    }

    pub fn deinit(self: *GroupedAggregates, allocator: std.mem.Allocator) void {
        allocator.free(self.accumulators);
    }
};

/// Execute GROUP BY query with hash aggregation
/// Execute GROUP BY query on Iceberg table (processes multiple data files)
fn executeIcebergGroupByQuery(allocator: std.mem.Allocator, table_path: []const u8, query: *const glacier.sql.Query) !void {
    const group_by_cols = query.group_by.?;
    const aggregates = query.aggregates.?;

    // Load Iceberg data files
    const data_files = try loadIcebergDataFiles(allocator, table_path, query);
    defer {
        for (data_files) |file_path| {
            allocator.free(file_path);
        }
        allocator.free(data_files);
    }

    if (data_files.len == 0) {
        std.debug.print("[INFO] No data files to read (empty table)\n\n", .{});
        return;
    }

    // Hash map for grouped aggregates: GroupKey -> GroupedAggregates
    var groups = std.HashMap(GroupKey, GroupedAggregates, GroupKeyContext, std.hash_map.default_max_load_percentage).init(allocator);
    defer {
        var iter = groups.iterator();
        while (iter.next()) |entry| {
            var key = entry.key_ptr.*;
            var aggs = entry.value_ptr.*;
            key.deinit(allocator);
            aggs.deinit(allocator);
        }
        groups.deinit();
    }

    // Process each data file
    for (data_files) |file_path| {
        var reader = glacier.parquet.Reader.open(allocator, file_path) catch continue;
        defer reader.close();

        try reader.readMetadata();
        const metadata = reader.metadata.?;

        // Process all row groups in this file
        for (metadata.row_groups) |rg| {
            try processRowGroupForGroupBy(allocator, &reader, metadata, rg, group_by_cols, aggregates, &groups, query.where_clause);
        }
    }

    // Print results
    printGroupByResults(allocator, group_by_cols, aggregates, &groups, query.order_by) catch |err| {
        std.debug.print("Error printing results: {}\n", .{err});
    };
}

fn executeGroupByQuery(allocator: std.mem.Allocator, query: *const glacier.sql.Query) !void {
    const table_name = query.from_table;
    const group_by_cols = query.group_by.?;
    const aggregates = query.aggregates.?;

    // Check if this is an Iceberg table (directory) or Parquet file
    const is_iceberg = blk: {
        const stat = std.fs.cwd().statFile(table_name) catch |err| {
            if (err == error.IsDir) break :blk true;
            break :blk false;
        };
        _ = stat;
        break :blk false;
    };

    if (is_iceberg) {
        return executeIcebergGroupByQuery(allocator, table_name, query);
    }

    // Open Parquet file
    var reader = glacier.parquet.Reader.open(allocator, table_name) catch |err| {
        std.debug.print("\n[ERROR] Failed to open '{s}': {s}\n\n", .{ table_name, @errorName(err) });
        return err;
    };
    defer reader.close();

    try reader.readMetadata();
    const metadata = reader.metadata.?;

    // Hash map for grouped aggregates: GroupKey -> GroupedAggregates
    var groups = std.HashMap(GroupKey, GroupedAggregates, GroupKeyContext, std.hash_map.default_max_load_percentage).init(allocator);
    defer {
        var iter = groups.iterator();
        while (iter.next()) |entry| {
            var key = entry.key_ptr.*;
            var aggs = entry.value_ptr.*;
            key.deinit(allocator);
            aggs.deinit(allocator);
        }
        groups.deinit();
    }

    // Process all row groups
    for (metadata.row_groups) |rg| {
        try processRowGroupForGroupBy(allocator, &reader, metadata, rg, group_by_cols, aggregates, &groups, query.where_clause);
    }

    // Print results
    printGroupByResults(allocator, group_by_cols, aggregates, &groups, query.order_by) catch |err| {
        std.debug.print("Error printing results: {}\n", .{err});
    };
}

/// Helper to evaluate WHERE clause for a single row (used in GROUP BY)
fn evaluateWhereForRow(
    where_expr: *const glacier.sql.Expr,
    id: i64,
    name: []const u8,
    age: i64,
) !bool {
    var column_values = std.StringHashMap(ColumnValue).init(std.heap.page_allocator);
    defer column_values.deinit();

    try column_values.put("id", .{ .int = id });
    try column_values.put("name", .{ .string = name });
    try column_values.put("age", .{ .int = age });

    return evaluateWhere(where_expr, column_values);
}

/// Process a single row group for GROUP BY (GENERIC VERSION)
fn processRowGroupForGroupBy(
    allocator: std.mem.Allocator,
    reader: *glacier.parquet.Reader,
    metadata: glacier.parquet.FileMetaData,
    rg: glacier.parquet.RowGroup,
    group_by_cols: [][]const u8,
    aggregates: []const glacier.sql.AggregateFunc,
    groups: *std.HashMap(GroupKey, GroupedAggregates, GroupKeyContext, std.hash_map.default_max_load_percentage),
    where_clause: ?*const glacier.sql.Expr,
) !void {
    var string_arena = std.heap.ArenaAllocator.init(allocator);
    defer string_arena.deinit();

    // Create schema mapper
    var schema_mapper = try SchemaMapper.init(allocator, metadata);
    defer schema_mapper.deinit();

    // Determine which columns we need to read
    var columns_needed = std.StringHashMap(void).init(allocator);
    defer columns_needed.deinit();

    // Add GROUP BY columns
    for (group_by_cols) |col| {
        try columns_needed.put(col, {});
    }

    // Add aggregate columns
    for (aggregates) |agg| {
        if (agg.column) |col| {
            try columns_needed.put(col, {});
        }
    }

    // For WHERE clause, we need ALL columns (simplified - could be optimized)
    // TODO: Parse WHERE to determine exact columns needed
    if (where_clause != null) {
        for (schema_mapper.column_names) |col| {
            try columns_needed.put(col, {});
        }
    }

    // Read all needed columns dynamically
    var columns: std.ArrayListUnmanaged(ColumnData) = .{};
    defer {
        for (columns.items) |col| {
            allocator.free(col.values);
        }
        columns.deinit(allocator);
    }

    var col_iter = columns_needed.keyIterator();
    while (col_iter.next()) |col_name| {
        const col_idx = schema_mapper.getColumnIndex(col_name.*) orelse return error.UnknownColumn;
        const col_values = try readColumnDynamic(
            allocator,
            string_arena.allocator(),
            reader,
            metadata,
            rg,
            col_idx,
            null,
        );
        try columns.append(allocator, .{ .name = col_name.*, .values = col_values });
    }

    // Determine actual row count from first column (don't trust rg.num_rows which may be 0)
    const total_rows: usize = if (columns.items.len > 0) columns.items[0].values.len else 0;

    // Helper to get column value by name and row index
    const getColumnValue = struct {
        fn call(cols: []const @TypeOf(columns.items[0]), name: []const u8, row: usize) ?DynamicColumnValue {
            for (cols) |col| {
                if (std.mem.eql(u8, col.name, name)) {
                    return col.values[row];
                }
            }
            return null;
        }
    }.call;

    // Process each row
    for (0..total_rows) |i| {
        // Evaluate WHERE clause if present
        if (where_clause) |where_expr| {
            const passes = evaluateWhereDynamic(where_expr, columns.items, i);
            if (!passes) continue;
        }

        // Build group key from GROUP BY columns
        const key_values = try allocator.alloc([]const u8, group_by_cols.len);
        for (group_by_cols, 0..) |col_name, idx| {
            const col_value = getColumnValue(columns.items, col_name, i) orelse return error.UnknownColumn;
            key_values[idx] = try col_value.toString(allocator);
        }

        const group_key = GroupKey{ .values = key_values };

        // Get or create group aggregates
        const gop = try groups.getOrPut(group_key);
        if (!gop.found_existing) {
            gop.value_ptr.* = try GroupedAggregates.init(allocator, aggregates);
        } else {
            // Key already exists, free the duplicate key
            for (key_values) |val| allocator.free(val);
            allocator.free(key_values);
        }

        // Accumulate aggregate values
        for (aggregates, 0..) |agg_func, agg_idx| {
            if (agg_func.func_type == .count) {
                gop.value_ptr.accumulators[agg_idx].accumulate(1.0);
            } else if (agg_func.column) |col_name| {
                const col_value = getColumnValue(columns.items, col_name, i) orelse return error.UnknownColumn;
                const value = col_value.toFloat();
                gop.value_ptr.accumulators[agg_idx].accumulate(value);
            }
        }
    }
}

/// Print GROUP BY results
fn printGroupByResults(
    allocator: std.mem.Allocator,
    group_by_cols: [][]const u8,
    aggregates: []const glacier.sql.AggregateFunc,
    groups: *std.HashMap(GroupKey, GroupedAggregates, GroupKeyContext, std.hash_map.default_max_load_percentage),
    order_by: ?[]glacier.sql.OrderByClause,
) !void {
    _ = order_by; // TODO: Implement ORDER BY for GROUP BY

    const col_count = group_by_cols.len + aggregates.len;

    // Build column headers
    var headers = try allocator.alloc([]const u8, col_count);
    defer {
        for (headers[group_by_cols.len..]) |header| {
            allocator.free(header);
        }
        allocator.free(headers);
    }

    // Copy group by column names
    for (group_by_cols, 0..) |col_name, i| {
        headers[i] = col_name;
    }

    // Create aggregate headers
    for (aggregates, 0..) |agg_func, i| {
        const func_name = switch (agg_func.func_type) {
            .count => "COUNT",
            .sum => "SUM",
            .avg => "AVG",
            .min => "MIN",
            .max => "MAX",
        };
        const col_name = agg_func.column orelse "*";
        headers[group_by_cols.len + i] = try std.fmt.allocPrint(allocator, "{s}({s})", .{ func_name, col_name });
    }

    // Calculate individual width for EACH column
    const widths = try allocator.alloc(usize, col_count);
    defer allocator.free(widths);

    // Initialize with header widths
    for (headers, 0..) |header, i| {
        widths[i] = getDisplayWidth(header);
    }

    // Scan all data to find max width per column
    var iter = groups.iterator();
    while (iter.next()) |entry| {
        // Check group key values
        for (entry.key_ptr.values, 0..) |val, i| {
            widths[i] = @max(widths[i], getDisplayWidth(val));
        }

        // Check aggregate values
        for (entry.value_ptr.accumulators, 0..) |acc, i| {
            const result = acc.finalize();
            const result_str = try std.fmt.allocPrint(allocator, "{d:.2}", .{result});
            defer allocator.free(result_str);
            widths[group_by_cols.len + i] = @max(widths[group_by_cols.len + i], getDisplayWidth(result_str));
        }
    }

    // Add padding to each column
    for (widths) |*w| {
        w.* += 2;
    }

    // Print header
    printTableHeader(headers, widths);

    // Print rows
    iter = groups.iterator();
    var row_count: usize = 0;
    while (iter.next()) |entry| {
        std.debug.print("|", .{});

        // Print group key values
        for (entry.key_ptr.values, 0..) |val, i| {
            printCentered(val, widths[i]);
            std.debug.print("|", .{});
        }

        // Print aggregate values
        for (entry.value_ptr.accumulators, 0..) |acc, i| {
            const result = acc.finalize();
            const result_str = try std.fmt.allocPrint(allocator, "{d:.2}", .{result});
            defer allocator.free(result_str);
            printCentered(result_str, widths[group_by_cols.len + i]);
            std.debug.print("|", .{});
        }
        std.debug.print("\n", .{});
        row_count += 1;
    }

    // Footer
    printTableFooter(widths, row_count, row_count);
    std.debug.print("\n", .{});
}

// ═══════════════════════════════════════════════════════════════════════════
// DYNAMIC SCHEMA SUPPORT
// ═══════════════════════════════════════════════════════════════════════════

/// Column value that can hold any Parquet type
const DynamicColumnValue = union(enum) {
    int64: i64,
    int32: i32,
    float: f32,
    double: f64,
    byte_array: []const u8,
    boolean: bool,
    null_val: void,

    pub fn toString(self: DynamicColumnValue, allocator: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .int64 => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .int32 => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .float => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .double => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
            .byte_array => |v| try allocator.dupe(u8, v),
            .boolean => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
            .null_val => try allocator.dupe(u8, "NULL"),
        };
    }

    pub fn toFloat(self: DynamicColumnValue) f64 {
        return switch (self) {
            .int64 => |v| @floatFromInt(v),
            .int32 => |v| @floatFromInt(v),
            .float => |v| @floatCast(v),
            .double => |v| v,
            .boolean => |v| if (v) 1.0 else 0.0,
            else => 0.0,
        };
    }

    /// Compare for ordering (returns .less, .equal, or .greater)
    pub fn compareOrder(self: DynamicColumnValue, other: DynamicColumnValue) std.math.Order {
        if (self.equals(other)) return .eq;
        if (self.lessThan(other)) return .lt;
        return .gt;
    }

    /// Compare two dynamic values
    pub fn compare(self: DynamicColumnValue, other: DynamicColumnValue, op: glacier.sql.BinaryOp) bool {
        // Handle NULL
        if (self == .null_val or other == .null_val) return false;

        return switch (op) {
            .eq => self.equals(other),
            .ne => !self.equals(other),
            .lt => self.lessThan(other),
            .le => self.lessThan(other) or self.equals(other),
            .gt => other.lessThan(self),
            .ge => other.lessThan(self) or self.equals(other),
            else => false,
        };
    }

    fn equals(self: DynamicColumnValue, other: DynamicColumnValue) bool {
        return switch (self) {
            .int64 => |a| switch (other) {
                .int64 => |b| a == b,
                .int32 => |b| a == b,
                .double => |b| @as(f64, @floatFromInt(a)) == b,
                .float => |b| @as(f64, @floatFromInt(a)) == @as(f64, @floatCast(b)),
                else => false,
            },
            .int32 => |a| switch (other) {
                .int64 => |b| a == b,
                .int32 => |b| a == b,
                .double => |b| @as(f64, @floatFromInt(a)) == b,
                .float => |b| @as(f64, @floatFromInt(a)) == @as(f64, @floatCast(b)),
                else => false,
            },
            .byte_array => |a| switch (other) {
                .byte_array => |b| std.mem.eql(u8, a, b),
                else => false,
            },
            .double => |a| switch (other) {
                .double => |b| a == b,
                .float => |b| a == @as(f64, @floatCast(b)),
                .int64 => |b| a == @as(f64, @floatFromInt(b)),
                .int32 => |b| a == @as(f64, @floatFromInt(b)),
                else => false,
            },
            .float => |a| switch (other) {
                .float => |b| a == b,
                .double => |b| @as(f64, @floatCast(a)) == b,
                .int64 => |b| @as(f64, @floatCast(a)) == @as(f64, @floatFromInt(b)),
                .int32 => |b| @as(f64, @floatCast(a)) == @as(f64, @floatFromInt(b)),
                else => false,
            },
            .boolean => |a| switch (other) {
                .boolean => |b| a == b,
                else => false,
            },
            .null_val => other == .null_val,
        };
    }

    fn lessThan(self: DynamicColumnValue, other: DynamicColumnValue) bool {
        return switch (self) {
            .int64 => |a| switch (other) {
                .int64 => |b| a < b,
                .int32 => |b| a < b,
                .double => |b| @as(f64, @floatFromInt(a)) < b,
                .float => |b| @as(f64, @floatFromInt(a)) < @as(f64, @floatCast(b)),
                else => false,
            },
            .int32 => |a| switch (other) {
                .int64 => |b| a < b,
                .int32 => |b| a < b,
                .double => |b| @as(f64, @floatFromInt(a)) < b,
                .float => |b| @as(f64, @floatFromInt(a)) < @as(f64, @floatCast(b)),
                else => false,
            },
            .byte_array => |a| switch (other) {
                .byte_array => |b| std.mem.order(u8, a, b) == .lt,
                else => false,
            },
            .double => |a| switch (other) {
                .double => |b| a < b,
                .float => |b| a < @as(f64, @floatCast(b)),
                .int64 => |b| a < @as(f64, @floatFromInt(b)),
                .int32 => |b| a < @as(f64, @floatFromInt(b)),
                else => false,
            },
            .float => |a| switch (other) {
                .float => |b| a < b,
                .double => |b| @as(f64, @floatCast(a)) < b,
                .int64 => |b| @as(f64, @floatCast(a)) < @as(f64, @floatFromInt(b)),
                .int32 => |b| @as(f64, @floatCast(a)) < @as(f64, @floatFromInt(b)),
                else => false,
            },
            else => false,
        };
    }
};

/// Collect all column names referenced in an expression
fn collectColumnsFromExpr(
    expr: *const glacier.sql.Expr,
    columns: *std.StringHashMap(void),
) !void {
    switch (expr.*) {
        .column => |col_name| {
            try columns.put(col_name, {});
        },
        .binary => |bin| {
            try collectColumnsFromExpr(bin.left, columns);
            try collectColumnsFromExpr(bin.right, columns);
        },
        .unary => |un| {
            try collectColumnsFromExpr(un.expr, columns);
        },
        else => {},
    }
}

/// Evaluate WHERE expression dynamically with DynamicColumnValue
fn evaluateWhereDynamic(
    expr: *const glacier.sql.Expr,
    columns: []const ColumnData,
    row_index: usize,
) bool {
    switch (expr.*) {
        .column => |col_name| {
            // Get column value
            for (columns) |col| {
                if (std.mem.eql(u8, col.name, col_name)) {
                    const val = col.values[row_index];
                    return switch (val) {
                        .boolean => |b| b,
                        else => false,
                    };
                }
            }
            return false;
        },
        .boolean => |b| return b,
        .binary => |bin| {
            const left_val = getExprValueDynamic(bin.left, columns, row_index);
            const right_val = getExprValueDynamic(bin.right, columns, row_index);

            return switch (bin.op) {
                .eq, .ne, .lt, .le, .gt, .ge => left_val.compare(right_val, bin.op),
                .and_op => evaluateWhereDynamic(bin.left, columns, row_index) and evaluateWhereDynamic(bin.right, columns, row_index),
                .or_op => evaluateWhereDynamic(bin.left, columns, row_index) or evaluateWhereDynamic(bin.right, columns, row_index),
                else => false,
            };
        },
        .unary => |un| {
            switch (un.op) {
                .not => return !evaluateWhereDynamic(un.expr, columns, row_index),
                else => return false,
            }
        },
        else => return false,
    }
}

fn getExprValueDynamic(
    expr: *const glacier.sql.Expr,
    columns: []const ColumnData,
    row_index: usize,
) DynamicColumnValue {
    switch (expr.*) {
        .number => |n| {
            if (@floor(n) == n) {
                return .{ .int64 = @intFromFloat(n) };
            }
            return .{ .double = n };
        },
        .string => |s| return .{ .byte_array = s },
        .boolean => |b| return .{ .boolean = b },
        .null_literal => return .{ .null_val = {} },
        .column => |col_name| {
            for (columns) |col| {
                if (std.mem.eql(u8, col.name, col_name)) {
                    return col.values[row_index];
                }
            }
            return .{ .null_val = {} };
        },
        else => return .{ .null_val = {} },
    }
}

/// Dynamic column data for a row group
const DynamicColumn = struct {
    name: []const u8,
    values: []DynamicColumnValue,

    pub fn deinit(self: *DynamicColumn, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
    }
};

/// Schema mapper - maps column names to indices
const SchemaMapper = struct {
    column_names: [][]const u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, metadata: glacier.parquet.FileMetaData) !SchemaMapper {
        const schema_fields = metadata.schema;
        // Skip first element (schema[0] is the root element)
        const actual_columns = schema_fields[1..];
        const column_names = try allocator.alloc([]const u8, actual_columns.len);

        for (actual_columns, 0..) |field, i| {
            column_names[i] = field.name;
        }

        return .{
            .column_names = column_names,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SchemaMapper) void {
        self.allocator.free(self.column_names);
    }

    pub fn getColumnIndex(self: *const SchemaMapper, column_name: []const u8) ?usize {
        for (self.column_names, 0..) |name, i| {
            if (std.mem.eql(u8, name, column_name)) {
                return i;
            }
        }
        return null;
    }

    pub fn getColumnName(self: *const SchemaMapper, index: usize) ?[]const u8 {
        if (index < self.column_names.len) {
            return self.column_names[index];
        }
        return null;
    }
};

/// Generic decoder for numeric types (INT64, INT32, FLOAT, DOUBLE)
/// Uses comptime to dispatch to correct parquet.Reader function
fn decodeNumericGeneric(
    comptime T: type,
    allocator: std.mem.Allocator,
    page_data: []const u8,
    num_values: usize,
    encoding: glacier.parquet.Encoding,
    is_required: bool,
    dictionary: ?[]const T,
) ![]T {
    switch (encoding) {
        .PLAIN => {
            // Comptime dispatch to correct decoder function
            if (T == i64) return glacier.parquet.Reader.decodePlainInt64(allocator, page_data, num_values, is_required);
            if (T == i32) return glacier.parquet.Reader.decodePlainInt32(allocator, page_data, num_values, is_required);
            if (T == f32) return glacier.parquet.Reader.decodePlainFloat(allocator, page_data, num_values, is_required);
            if (T == f64) return glacier.parquet.Reader.decodePlainDouble(allocator, page_data, num_values, is_required);
            @compileError("Unsupported numeric type");
        },
        .RLE_DICTIONARY, .PLAIN_DICTIONARY => {
            const dict_vals = dictionary orelse return error.MissingDictionary;
            // Comptime dispatch to correct dictionary decoder
            if (T == i64) return glacier.parquet.Reader.decodeRLEDictionaryInt64(allocator, page_data, num_values, is_required, dict_vals);
            if (T == f32) return glacier.parquet.Reader.decodeRLEDictionaryFloat(allocator, page_data, num_values, is_required, dict_vals);
            if (T == f64) return glacier.parquet.Reader.decodeRLEDictionaryDouble(allocator, page_data, num_values, is_required, dict_vals);
            // INT32 dictionary not supported via this path (handled in calling code)
            return error.UnsupportedEncoding;
        },
        else => return error.UnsupportedEncoding,
    }
}

/// Generic decoder for BOOLEAN type
fn decodeBooleanGeneric(
    allocator: std.mem.Allocator,
    page_data: []const u8,
    num_values: usize,
    encoding: glacier.parquet.Encoding,
    is_required: bool,
    dictionary: ?[]const bool,
) ![]bool {
    switch (encoding) {
        .PLAIN => {
            return glacier.parquet.Reader.decodePlainBoolean(allocator, page_data, num_values, is_required);
        },
        .RLE_DICTIONARY, .PLAIN_DICTIONARY => {
            const dict_vals = dictionary orelse return error.MissingDictionary;
            return glacier.parquet.Reader.decodeRLEDictionaryBoolean(allocator, page_data, num_values, is_required, dict_vals);
        },
        else => return error.UnsupportedEncoding,
    }
}

/// Read a single column dynamically based on its type
/// If max_rows is provided, stops reading after reaching that many values (for LIMIT optimization)
fn readColumnDynamic(
    allocator: std.mem.Allocator,
    string_arena: std.mem.Allocator,
    reader: *glacier.parquet.Reader,
    metadata: glacier.parquet.FileMetaData,
    rg: glacier.parquet.RowGroup,
    column_index: usize,
    max_rows: ?usize,
) ![]DynamicColumnValue {
    if (column_index >= rg.columns.len) return error.ColumnIndexOutOfBounds;

    const col = rg.columns[column_index];
    // Schema[0] is root, actual columns start at schema[1]
    const schema_element = metadata.schema[column_index + 1];

    // Read dictionary if present
    var dict_int: ?glacier.parquet.Reader.DictionaryInt64 = null;
    var dict_str: ?glacier.parquet.Reader.DictionaryByteArray = null;
    var dict_float: ?glacier.parquet.Reader.DictionaryFloat = null;
    var dict_double: ?glacier.parquet.Reader.DictionaryDouble = null;
    var dict_bool: ?glacier.parquet.Reader.DictionaryBoolean = null;

    if (col.meta_data.dictionary_page_offset) |_| {
        switch (schema_element.type orelse return error.MissingType) {
            .INT32 => {
                dict_int = try reader.readDictionaryPageInt32(col.meta_data, allocator);
            },
            .INT64 => {
                dict_int = try reader.readDictionaryPageInt64(col.meta_data, allocator);
            },
            .FLOAT => {
                dict_float = try reader.readDictionaryPageFloat(col.meta_data, allocator);
            },
            .DOUBLE => {
                dict_double = try reader.readDictionaryPageDouble(col.meta_data, allocator);
            },
            .BOOLEAN => {
                dict_bool = try reader.readDictionaryPageBoolean(col.meta_data, allocator);
            },
            .BYTE_ARRAY => {
                dict_str = try reader.readDictionaryPageByteArray(col.meta_data, string_arena);
            },
            else => {},
        }
    }
    defer if (dict_int) |*d| d.deinit();
    defer if (dict_str) |*d| d.deinit();
    defer if (dict_float) |*d| d.deinit();
    defer if (dict_double) |*d| d.deinit();
    defer if (dict_bool) |*d| d.deinit();

    // Multi-page reading implementation with early termination support
    // Parquet files can have MULTIPLE pages per column in a row group.
    // We need to read ALL pages until we have col.meta_data.num_values total values.
    // OPTIMIZATION: If max_rows is specified (e.g., for LIMIT 5), stop reading after max_rows values.

    const total_values: usize = @intCast(col.meta_data.num_values);

    // Determine how many values we actually need to read
    const values_to_read: usize = if (max_rows) |limit| @min(total_values, limit) else total_values;

    const result = try allocator.alloc(DynamicColumnValue, values_to_read);
    errdefer allocator.free(result);

    const is_required = if (schema_element.repetition_type) |rt| rt == .REQUIRED else false;
    const col_type = schema_element.type orelse return error.MissingType;

    var values_read: usize = 0;
    var current_offset: i64 = col.meta_data.data_page_offset;

    // Read pages until we have enough values (early termination for LIMIT)
    while (values_read < values_to_read) {
        const page = try reader.readDataPage(current_offset, allocator);
        defer allocator.free(page.compressed_data);

        const uncompressed_size: usize = @intCast(page.page_header.uncompressed_page_size);
        const page_data = try glacier.compression.decompress(
            allocator,
            col.meta_data.codec,
            page.compressed_data,
            uncompressed_size,
        );
        defer allocator.free(page_data);

        const encoding = page.page_header.data_page_header.?.encoding;
        const num_page_values: usize = @intCast(page.page_header.data_page_header.?.num_values);

        // Calculate how many values to copy from this page (handle partial page reads for LIMIT)
        const values_remaining = values_to_read - values_read;
        const values_to_copy = @min(num_page_values, values_remaining);

        // Decode this page's values based on type
        switch (col_type) {
            .INT64 => {
                const dict_vals = if (dict_int) |d| d.values else null;
                const values = try decodeNumericGeneric(i64, allocator, page_data, num_page_values, encoding, is_required, dict_vals);
                defer allocator.free(values);
                for (0..values_to_copy) |idx| {
                    result[values_read + idx] = .{ .int64 = values[idx] };
                }
            },
            .INT32 => {
                if (encoding == .PLAIN) {
                    const values = try decodeNumericGeneric(i32, allocator, page_data, num_page_values, encoding, is_required, null);
                    defer allocator.free(values);
                    for (0..values_to_copy) |idx| {
                        result[values_read + idx] = .{ .int32 = values[idx] };
                    }
                } else {
                    const dict_vals = if (dict_int) |d| d.values else null;
                    const i64_values = try decodeNumericGeneric(i64, allocator, page_data, num_page_values, encoding, is_required, dict_vals);
                    defer allocator.free(i64_values);
                    for (0..values_to_copy) |idx| {
                        result[values_read + idx] = .{ .int32 = @intCast(i64_values[idx]) };
                    }
                }
            },
            .BYTE_ARRAY => {
                const values = switch (encoding) {
                    .PLAIN => try glacier.parquet.Reader.decodePlainByteArray(
                        string_arena,
                        page_data,
                        num_page_values,
                        is_required,
                    ),
                    .RLE_DICTIONARY, .PLAIN_DICTIONARY => blk: {
                        if (dict_str) |dict| {
                            break :blk try glacier.parquet.Reader.decodeRLEDictionaryByteArray(
                                string_arena,
                                page_data,
                                num_page_values,
                                is_required,
                                dict.values,
                            );
                        } else {
                            return error.MissingDictionary;
                        }
                    },
                    else => return error.UnsupportedEncoding,
                };
                for (0..values_to_copy) |idx| {
                    result[values_read + idx] = .{ .byte_array = values[idx] };
                }
            },
            .FLOAT => {
                const dict_vals = if (dict_float) |d| d.values else null;
                const values = try decodeNumericGeneric(f32, allocator, page_data, num_page_values, encoding, is_required, dict_vals);
                defer allocator.free(values);
                for (0..values_to_copy) |idx| {
                    result[values_read + idx] = .{ .float = values[idx] };
                }
            },
            .DOUBLE => {
                const dict_vals = if (dict_double) |d| d.values else null;
                const values = try decodeNumericGeneric(f64, allocator, page_data, num_page_values, encoding, is_required, dict_vals);
                defer allocator.free(values);
                for (0..values_to_copy) |idx| {
                    result[values_read + idx] = .{ .double = values[idx] };
                }
            },
            .BOOLEAN => {
                const dict_vals = if (dict_bool) |d| d.values else null;
                const values = try decodeBooleanGeneric(allocator, page_data, num_page_values, encoding, is_required, dict_vals);
                defer allocator.free(values);
                for (0..values_to_copy) |idx| {
                    result[values_read + idx] = .{ .boolean = values[idx] };
                }
            },
            else => return error.UnsupportedParquetType,
        }

        // Update counter with values actually copied (not total in page)
        values_read += values_to_copy;

        // Early termination: if we have enough values, stop reading pages
        if (values_read >= values_to_read) {
            break;
        }

        // Calculate next page offset
        // Next page starts after: current_offset + header_size + compressed_page_size
        const compressed_size: i64 = page.page_header.compressed_page_size;
        const header_size_i64: i64 = @intCast(page.header_size);
        current_offset += header_size_i64 + compressed_size;
    }

    return result;
}
