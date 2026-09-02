//! Iceberg catalog: metadata.json via std.json; manifests via libavro.
//! The engine never globs `data/*.parquet`. File list comes from the snapshot
//! manifest-list Avro, then each manifest Avro.

const std = @import("std");
const vfs = @import("../vfs/source.zig");
const FileSource = vfs.FileSource;
const Transport = vfs.Transport;
const sql = @import("../sql/parser.zig");
const parquet = @import("../formats/parquet_wrap.zig");
const avro = @import("../formats/avro_wrap.zig");

pub const Error = error{
    InvalidMetadata,
    SnapshotNotFound,
    SchemaNotFound,
    ManifestsNeedAvro,
    UnsupportedNested,
    UnsupportedDeletes,
    UnsupportedSchemaEvolution,
    UnsupportedPartitionSpec,
};

pub const SchemaField = struct {
    id: i32,
    name: []const u8,
    required: bool,
    type_name: []const u8,
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
};

pub const Schema = struct {
    schema_id: i32,
    fields: []SchemaField,
};

pub const PartitionField = struct {
    source_id: i32,
    field_id: i32,
    name: []const u8,
    transform: []const u8,
};

pub const PartitionSpec = struct {
    spec_id: i32,
    fields: []PartitionField,
};

pub const Snapshot = struct {
    snapshot_id: i64,
    timestamp_ms: i64,
    manifest_list: []const u8,
    sequence_number: i64 = 0,
    schema_id: ?i32 = null,
};

pub const BoundValue = union(enum) {
    int: i64,
    float: f64,
    string: []const u8,
};

pub const ColBound = struct {
    column: []const u8,
    lower: BoundValue,
    upper: BoundValue,
};

pub const FileFormat = enum { parquet, avro, glacier };

pub const DataFile = struct {
    path: []const u8,
    format: FileFormat = .parquet,
    record_count: i64 = 0,
    bounds: []const ColBound = &.{},
    bytes: ?[]const u8 = null,
};

pub const TableMetadata = struct {
    format_version: i32,
    table_uuid: []const u8,
    location: []const u8,
    current_snapshot_id: ?i64,
    current_schema_id: i32,
    default_spec_id: i32 = 0,
    schemas: []Schema,
    partition_specs: []PartitionSpec = &.{},
    snapshots: []Snapshot,

    pub fn currentSnapshot(self: TableMetadata) ?*const Snapshot {
        const id = self.current_snapshot_id orelse return null;
        for (self.snapshots) |*s| {
            if (s.snapshot_id == id) return s;
        }
        return null;
    }

    pub fn currentSchema(self: TableMetadata) ?*const Schema {
        for (self.schemas) |*s| {
            if (s.schema_id == self.current_schema_id) return s;
        }
        if (self.schemas.len == 1) return &self.schemas[0];
        return null;
    }

    pub fn currentPartitionSpec(self: TableMetadata) ?*const PartitionSpec {
        for (self.partition_specs) |*s| {
            if (s.spec_id == self.default_spec_id) return s;
        }
        if (self.partition_specs.len == 1) return &self.partition_specs[0];
        return null;
    }
};

pub const Table = struct {
    dir: []const u8,
    metadata: TableMetadata,
    files: []DataFile,
};

pub fn parseMetadata(allocator: std.mem.Allocator, json_text: []const u8) !TableMetadata {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_text, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return error.InvalidMetadata,
    };

    const format_version: i32 = @intCast(try getInt(root, "format-version"));
    const table_uuid = try allocator.dupe(u8, try getString(root, "table-uuid"));
    const location = try allocator.dupe(u8, try getString(root, "location"));
    const current_snapshot_id: ?i64 = if (root.get("current-snapshot-id")) |v| try asInt(v) else null;
    const current_schema_id: i32 = @intCast(if (root.get("current-schema-id")) |v| try asInt(v) else 0);
    const default_spec_id: i32 = @intCast(if (root.get("default-spec-id")) |v| try asInt(v) else 0);

    const schemas_val = root.get("schemas") orelse return error.InvalidMetadata;
    const schemas_arr = switch (schemas_val) {
        .array => |a| a,
        else => return error.InvalidMetadata,
    };
    const schemas = try allocator.alloc(Schema, schemas_arr.items.len);
    for (schemas_arr.items, 0..) |item, i| {
        schemas[i] = try parseSchema(allocator, item);
    }

    var snapshots: []Snapshot = &.{};
    if (root.get("snapshots")) |sv| {
        const arr = switch (sv) {
            .array => |a| a,
            else => return error.InvalidMetadata,
        };
        snapshots = try allocator.alloc(Snapshot, arr.items.len);
        for (arr.items, 0..) |item, i| {
            snapshots[i] = try parseSnapshot(allocator, item);
        }
    }

    const partition_specs = try parsePartitionSpecs(allocator, root);

    return .{
        .format_version = format_version,
        .table_uuid = table_uuid,
        .location = location,
        .current_snapshot_id = current_snapshot_id,
        .current_schema_id = current_schema_id,
        .default_spec_id = default_spec_id,
        .schemas = schemas,
        .partition_specs = partition_specs,
        .snapshots = snapshots,
    };
}

pub fn openTable(allocator: std.mem.Allocator, t: Transport, table_dir: []const u8) !Table {
    const dir = try allocator.dupe(u8, table_dir);
    const meta_path = try metadataJsonPath(allocator, t, dir);
    const meta_json = try readLocation(allocator, t, meta_path);
    const metadata = try parseMetadata(allocator, meta_json);
    try checkReadSupport(metadata);
    const files = try loadSnapshotFiles(allocator, t, dir, metadata);
    return .{ .dir = dir, .metadata = metadata, .files = files };
}

/// Catalog features we do not execute. Opening the table fails instead of
/// scanning a subset and pretending the result is complete.
pub fn checkReadSupport(metadata: TableMetadata) !void {
    if (metadata.schemas.len > 1) return error.UnsupportedSchemaEvolution;
    if (metadata.currentSnapshot()) |snap| {
        if (snap.schema_id) |sid| {
            if (sid != metadata.current_schema_id) return error.UnsupportedSchemaEvolution;
        }
    }
    if (metadata.currentPartitionSpec()) |spec| {
        for (spec.fields) |f| {
            if (!isIdentityTransform(f.transform)) return error.UnsupportedPartitionSpec;
        }
    }
}

fn isIdentityTransform(transform: []const u8) bool {
    return std.ascii.eqlIgnoreCase(transform, "identity");
}

fn loadSnapshotFiles(
    allocator: std.mem.Allocator,
    t: Transport,
    table_dir: []const u8,
    metadata: TableMetadata,
) ![]DataFile {
    const snap = metadata.currentSnapshot() orelse return error.SnapshotNotFound;
    const list_path = resolveListedPath(allocator, table_dir, snap.manifest_list) catch return error.ManifestsNeedAvro;
    const list_buf = readLocation(allocator, t, list_path) catch return error.ManifestsNeedAvro;
    const manifest_paths = avro.collectStringField(allocator, list_buf, "manifest_path") catch return error.ManifestsNeedAvro;

    var files: std.ArrayList(DataFile) = .empty;
    const schema_fields = if (metadata.currentSchema()) |s| s.fields else &.{};
    for (manifest_paths) |rel| {
        const man_path = resolveListedPath(allocator, table_dir, rel) catch continue;
        const man_buf = readLocation(allocator, t, man_path) catch continue;
        const entries = avro.readIcebergEntries(allocator, man_buf) catch continue;
        for (entries) |e| {
            if (e.status == 2) continue; // Iceberg status DELETED: file no longer in the table
            try requireDataFileContent(e.content);
            try files.append(allocator, try dataFileFromEntry(allocator, table_dir, schema_fields, e));
        }
    }
    if (files.items.len == 0) return error.ManifestsNeedAvro;
    return files.items;
}

fn dataFileFromEntry(
    allocator: std.mem.Allocator,
    table_dir: []const u8,
    schema_fields: []const SchemaField,
    e: avro.ManifestEntry,
) !DataFile {
    const path = try resolveListedPath(allocator, table_dir, e.path);
    const format: FileFormat = if (std.ascii.eqlIgnoreCase(e.format, "AVRO")) .avro else .parquet;
    return .{
        .path = path,
        .format = format,
        .record_count = e.record_count,
        .bounds = try boundsFromMaps(allocator, schema_fields, e.lower, e.upper),
    };
}

fn boundsFromMaps(
    allocator: std.mem.Allocator,
    schema_fields: []const SchemaField,
    lower: []const avro.BoundBytes,
    upper: []const avro.BoundBytes,
) ![]ColBound {
    var out: std.ArrayList(ColBound) = .empty;
    for (lower) |lo| {
        const field = fieldById(schema_fields, lo.field_id) orelse continue;
        const hi = findBoundBytes(upper, lo.field_id) orelse continue;
        try out.append(allocator, .{
            .column = field.name,
            .lower = try decodeBound(allocator, field.type_name, lo.bytes),
            .upper = try decodeBound(allocator, field.type_name, hi.bytes),
        });
    }
    return out.items;
}

fn fieldById(fields: []const SchemaField, id: i32) ?SchemaField {
    for (fields) |f| {
        if (f.id == id) return f;
    }
    return null;
}

fn findBoundBytes(bounds: []const avro.BoundBytes, id: i32) ?avro.BoundBytes {
    for (bounds) |b| {
        if (b.field_id == id) return b;
    }
    return null;
}

fn decodeBound(allocator: std.mem.Allocator, type_name: []const u8, bytes: []const u8) !BoundValue {
    if (std.ascii.eqlIgnoreCase(type_name, "long") or std.ascii.eqlIgnoreCase(type_name, "timestamp") or std.ascii.eqlIgnoreCase(type_name, "timestamptz") or std.ascii.eqlIgnoreCase(type_name, "timestamp_ntz") or std.ascii.eqlIgnoreCase(type_name, "timestamp_tz")) {
        if (bytes.len < 8) return error.InvalidMetadata;
        return .{ .int = std.mem.readInt(i64, bytes[0..8], .little) };
    }
    if (std.ascii.eqlIgnoreCase(type_name, "int") or std.ascii.eqlIgnoreCase(type_name, "date")) {
        if (bytes.len < 4) return error.InvalidMetadata;
        return .{ .int = std.mem.readInt(i32, bytes[0..4], .little) };
    }
    if (std.ascii.eqlIgnoreCase(type_name, "double")) {
        if (bytes.len < 8) return error.InvalidMetadata;
        return .{ .float = @bitCast(std.mem.readInt(u64, bytes[0..8], .little)) };
    }
    if (std.ascii.eqlIgnoreCase(type_name, "float")) {
        if (bytes.len < 4) return error.InvalidMetadata;
        return .{ .float = @floatCast(@as(f32, @bitCast(std.mem.readInt(u32, bytes[0..4], .little)))) };
    }
    if (std.ascii.eqlIgnoreCase(type_name, "string") or std.ascii.eqlIgnoreCase(type_name, "binary") or std.ascii.eqlIgnoreCase(type_name, "uuid")) {
        return .{ .string = try allocator.dupe(u8, bytes) };
    }
    return error.UnsupportedNested;
}

fn resolveListedPath(allocator: std.mem.Allocator, table_dir: []const u8, listed: []const u8) ![]u8 {
    return vfs.joinLocation(allocator, table_dir, listed);
}

pub fn parquetDataFile(path: []const u8) DataFile {
    return .{ .path = path };
}

pub fn writePruneFixture(allocator: std.mem.Allocator, io: std.Io, table_dir: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try std.Io.Dir.cwd().createDirPath(io, try std.fs.path.join(a, &.{ table_dir, "metadata" }));
    try std.Io.Dir.cwd().createDirPath(io, try std.fs.path.join(a, &.{ table_dir, "data" }));

    const path_a = try a.dupeZ(u8, try std.fs.path.join(a, &.{ table_dir, "data", "part-a.parquet" }));
    const path_b = try a.dupeZ(u8, try std.fs.path.join(a, &.{ table_dir, "data", "part-b.parquet" }));

    const ids_a = [_]i64{ 1, 2, 3, 4, 5 };
    const prices_a = [_]i64{ 50, 80, 100, 120, 150 };
    const cats_a = [_][*:0]const u8{ "fruit", "fruit", "veg", "veg", "fruit" };
    const ids_b = [_]i64{ 6, 7, 8, 9, 10 };
    const prices_b = [_]i64{ 200, 90, 110, 300, 75 };
    const cats_b = [_][*:0]const u8{ "dairy", "fruit", "veg", "dairy", "fruit" };

    try parquet.writeSalesRows(path_a, &ids_a, &prices_a, &cats_a);
    try parquet.writeSalesRows(path_b, &ids_b, &prices_b, &cats_b);

    try writeText(io, try std.fs.path.join(a, &.{ table_dir, "metadata", "version-hint.text" }), "1\n");
    try writeText(io, try std.fs.path.join(a, &.{ table_dir, "metadata", "v1.metadata.json" }),
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        \\  "location": "iceberg_prune",
        \\  "last-updated-ms": 1700000000000,
        \\  "last-column-id": 3,
        \\  "current-snapshot-id": 1,
        \\  "current-schema-id": 0,
        \\  "default-spec-id": 0,
        \\  "partition-specs": [{ "spec-id": 0, "fields": [] }],
        \\  "schemas": [{
        \\    "schema-id": 0,
        \\    "fields": [
        \\      {"id": 1, "name": "id", "required": true, "type": "long"},
        \\      {"id": 2, "name": "price", "required": true, "type": "long"},
        \\      {"id": 3, "name": "category", "required": true, "type": "string"}
        \\    ]
        \\  }],
        \\  "snapshots": [{
        \\    "snapshot-id": 1,
        \\    "timestamp-ms": 1700000000000,
        \\    "sequence-number": 1,
        \\    "manifest-list": "metadata/snap-1.avro"
        \\  }]
        \\}
        \\
    );

    var list_z: [1024]u8 = undefined;
    var man_z: [1024]u8 = undefined;
    const list_path = try std.fmt.bufPrintZ(&list_z, "{s}/metadata/snap-1.avro", .{table_dir});
    const man_path = try std.fmt.bufPrintZ(&man_z, "{s}/metadata/manifest-1.avro", .{table_dir});
    try avro.writeManifestList(list_path, "metadata/manifest-1.avro");
    const data_paths = [_][*:0]const u8{ "data/part-a.parquet", "data/part-b.parquet" };
    const counts = [_]i64{ 5, 5 };
    const lower = [_]i64{ 50, 75 };
    const upper = [_]i64{ 150, 300 };
    try avro.writeDataManifest(man_path, &data_paths, &counts, &lower, &upper, 2);
}

fn writeText(io: std.Io, path: []const u8, text: []const u8) !void {
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    var buf: [1024]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(text);
    try writer.interface.flush();
}

pub fn canSkipFile(file: DataFile, expr: *const sql.BoolExpr) bool {
    // File metrics only (`lower_bounds` / `upper_bounds`). Partition values are
    // not read, so identity-partitioned tables are not extra-pruned here.
    return !mayMatch(file, expr);
}

fn mayMatch(file: DataFile, expr: *const sql.BoolExpr) bool {
    return switch (expr.*) {
        .cmp => |c| cmpMayMatch(file, c),
        .isnull => true,
        .@"and" => |b| mayMatch(file, b.left) and mayMatch(file, b.right),
        .@"or" => |b| mayMatch(file, b.left) or mayMatch(file, b.right),
    };
}

fn cmpMayMatch(file: DataFile, pred: sql.Cmp) bool {
    const col_name = switch (pred.left) {
        .column => |n| n,
        .agg => return true,
    };
    const bound = findBound(file, col_name) orelse return true;
    return rangeMayMatch(bound.lower, bound.upper, pred.op, pred.literal);
}

fn findBound(file: DataFile, name: []const u8) ?ColBound {
    for (file.bounds) |b| {
        if (std.ascii.eqlIgnoreCase(b.column, name)) return b;
    }
    return null;
}

fn rangeMayMatch(lower: BoundValue, upper: BoundValue, op: sql.CmpOp, lit: sql.Literal) bool {
    const lo = boundAsFloat(lower) orelse return true;
    const hi = boundAsFloat(upper) orelse return true;
    const v = literalAsFloat(lit) orelse {
        if (lower == .string and lit == .string) {
            return strRangeMayMatch(lower.string, upper.string, op, lit.string);
        }
        return true;
    };
    return switch (op) {
        .eq => v >= lo and v <= hi,
        .ne => true,
        .lt => lo < v,
        .le => lo <= v,
        .gt => hi > v,
        .ge => hi >= v,
    };
}

fn strRangeMayMatch(lo: []const u8, hi: []const u8, op: sql.CmpOp, v: []const u8) bool {
    const cmp_lo = std.mem.order(u8, lo, v);
    const cmp_hi = std.mem.order(u8, hi, v);
    return switch (op) {
        .eq => cmp_lo != .gt and cmp_hi != .lt,
        .ne => true,
        .lt => cmp_lo == .lt,
        .le => cmp_lo != .gt,
        .gt => cmp_hi == .gt,
        .ge => cmp_hi != .lt,
    };
}

fn boundAsFloat(v: BoundValue) ?f64 {
    return switch (v) {
        .int => |n| @floatFromInt(n),
        .float => |n| n,
        .string => null,
    };
}

fn literalAsFloat(v: sql.Literal) ?f64 {
    return switch (v) {
        .int => |n| @floatFromInt(n),
        .float => |n| n,
        .string => null,
    };
}

fn metadataJsonPath(allocator: std.mem.Allocator, t: Transport, table_dir: []const u8) ![]u8 {
    const hint_path = try vfs.joinLocation(allocator, table_dir, "metadata/version-hint.text");
    if (readLocation(allocator, t, hint_path)) |hint| {
        const trimmed = std.mem.trim(u8, hint, " \t\r\n");
        const rel = try std.fmt.allocPrint(allocator, "metadata/v{s}.metadata.json", .{trimmed});
        return vfs.joinLocation(allocator, table_dir, rel);
    } else |_| {
        return vfs.joinLocation(allocator, table_dir, "metadata/v1.metadata.json");
    }
}

fn readLocation(allocator: std.mem.Allocator, t: Transport, path: []const u8) ![]u8 {
    var source = try FileSource.openLocation(t, path);
    defer source.close();
    return source.readAll(allocator);
}

fn readAll(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    var source = try FileSource.openPath(io, path);
    defer source.close();
    return source.readAll(allocator);
}

fn parseSchema(allocator: std.mem.Allocator, value: std.json.Value) !Schema {
    const obj = switch (value) {
        .object => |o| o,
        else => return error.InvalidMetadata,
    };
    const schema_id: i32 = @intCast(if (obj.get("schema-id")) |v| try asInt(v) else 0);
    const fields_val = obj.get("fields") orelse return error.InvalidMetadata;
    const arr = switch (fields_val) {
        .array => |a| a,
        else => return error.InvalidMetadata,
    };
    const fields = try allocator.alloc(SchemaField, arr.items.len);
    for (arr.items, 0..) |item, i| {
        const f = switch (item) {
            .object => |o| o,
            else => return error.InvalidMetadata,
        };
        var type_name: []const u8 = undefined;
        var decimal_precision: i32 = 0;
        var decimal_scale: i32 = 0;
        switch (f.get("type") orelse return error.InvalidMetadata) {
            .string => |s| type_name = s,
            .object => |to| {
                const inner = try getString(to, "type");
                if (!std.ascii.eqlIgnoreCase(inner, "decimal")) return error.UnsupportedNested;
                type_name = "decimal";
                decimal_precision = @intCast(try getInt(to, "precision"));
                decimal_scale = @intCast(if (to.get("scale")) |sv| try asInt(sv) else 0);
            },
            else => return error.InvalidMetadata,
        }
        fields[i] = .{
            .id = @intCast(try asInt(f.get("id") orelse return error.InvalidMetadata)),
            .name = try allocator.dupe(u8, try getString(f, "name")),
            .required = if (f.get("required")) |r| r == .bool and r.bool else true,
            .type_name = try allocator.dupe(u8, type_name),
            .decimal_precision = decimal_precision,
            .decimal_scale = decimal_scale,
        };
    }
    return .{ .schema_id = schema_id, .fields = fields };
}

fn parseSnapshot(allocator: std.mem.Allocator, value: std.json.Value) !Snapshot {
    const obj = switch (value) {
        .object => |o| o,
        else => return error.InvalidMetadata,
    };
    return .{
        .snapshot_id = try getInt(obj, "snapshot-id"),
        .timestamp_ms = if (obj.get("timestamp-ms")) |v| try asInt(v) else 0,
        .manifest_list = try allocator.dupe(u8, try getString(obj, "manifest-list")),
        .sequence_number = if (obj.get("sequence-number")) |v| try asInt(v) else 0,
        .schema_id = if (obj.get("schema-id")) |v| @intCast(try asInt(v)) else null,
    };
}

fn parsePartitionSpecs(allocator: std.mem.Allocator, root: std.json.ObjectMap) ![]PartitionSpec {
    if (root.get("partition-specs")) |v| {
        const arr = try asArray(v);
        const specs = try allocator.alloc(PartitionSpec, arr.items.len);
        for (arr.items, 0..) |item, i| {
            specs[i] = try parseSpecObject(allocator, item);
        }
        return specs;
    }
    if (root.get("partition-spec")) |v| {
        const arr = try asArray(v);
        const fields = try parsePartitionFields(allocator, arr);
        const specs = try allocator.alloc(PartitionSpec, 1);
        specs[0] = .{ .spec_id = 0, .fields = fields };
        return specs;
    }
    const specs = try allocator.alloc(PartitionSpec, 1);
    specs[0] = .{ .spec_id = 0, .fields = &.{} };
    return specs;
}

fn parseSpecObject(allocator: std.mem.Allocator, value: std.json.Value) !PartitionSpec {
    const obj = switch (value) {
        .object => |o| o,
        else => return error.InvalidMetadata,
    };
    const spec_id: i32 = @intCast(if (obj.get("spec-id")) |v| try asInt(v) else 0);
    const fields_val = obj.get("fields") orelse return error.InvalidMetadata;
    return .{
        .spec_id = spec_id,
        .fields = try parsePartitionFields(allocator, try asArray(fields_val)),
    };
}

fn parsePartitionFields(allocator: std.mem.Allocator, arr: std.json.Array) ![]PartitionField {
    const fields = try allocator.alloc(PartitionField, arr.items.len);
    for (arr.items, 0..) |item, i| {
        const f = switch (item) {
            .object => |o| o,
            else => return error.InvalidMetadata,
        };
        fields[i] = .{
            .source_id = @intCast(try getInt(f, "source-id")),
            .field_id = @intCast(if (f.get("field-id")) |v| try asInt(v) else 1000 + @as(i64, @intCast(i))),
            .name = try allocator.dupe(u8, try getString(f, "name")),
            .transform = try allocator.dupe(u8, try getString(f, "transform")),
        };
    }
    return fields;
}

fn asArray(v: std.json.Value) !std.json.Array {
    return switch (v) {
        .array => |a| a,
        else => error.InvalidMetadata,
    };
}

fn requireDataFileContent(content: i32) !void {
    if (content != 0) return error.UnsupportedDeletes;
}

fn getString(obj: std.json.ObjectMap, key: []const u8) ![]const u8 {
    const v = obj.get(key) orelse return error.InvalidMetadata;
    return switch (v) {
        .string => |s| s,
        else => error.InvalidMetadata,
    };
}

fn getInt(obj: std.json.ObjectMap, key: []const u8) !i64 {
    return asInt(obj.get(key) orelse return error.InvalidMetadata);
}

fn asInt(v: std.json.Value) !i64 {
    return switch (v) {
        .integer => |n| n,
        .float => |n| @intFromFloat(n),
        else => error.InvalidMetadata,
    };
}

test "parse iceberg_prune metadata.json" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const json_text = try readAll(gpa, io, "tests/iceberg_prune/metadata/v1.metadata.json");
    defer gpa.free(json_text);
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json_text);
    try std.testing.expectEqual(@as(i32, 2), meta.format_version);
    try std.testing.expectEqual(@as(i64, 1), meta.current_snapshot_id.?);
    try std.testing.expectEqual(@as(usize, 3), meta.currentSchema().?.fields.len);
    try std.testing.expectEqualStrings("price", meta.currentSchema().?.fields[1].name);
    try std.testing.expect(meta.currentSnapshot() != null);
    try checkReadSupport(meta);
    try std.testing.expectEqual(@as(i32, 0), meta.default_spec_id);
    try std.testing.expectEqual(@as(usize, 0), meta.currentPartitionSpec().?.fields.len);
}

test "prune skips file outside bounds" {
    const file_low = DataFile{
        .path = "a.parquet",
        .bounds = &.{.{ .column = "price", .lower = .{ .int = 50 }, .upper = .{ .int = 150 } }},
    };
    const file_high = DataFile{
        .path = "b.parquet",
        .bounds = &.{.{ .column = "price", .lower = .{ .int = 75 }, .upper = .{ .int = 300 } }},
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try sql.parse(arena.allocator(), "SELECT * FROM t WHERE price > 180");
    const where = q.where.?;
    try std.testing.expect(canSkipFile(file_low, where));
    try std.testing.expect(!canSkipFile(file_high, where));
}

test "identity partition spec is allowed; prune still uses file bounds only" {
    const json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "u",
        \\  "location": "t",
        \\  "current-schema-id": 0,
        \\  "default-spec-id": 0,
        \\  "schemas": [{"schema-id": 0, "fields": [
        \\    {"id": 1, "name": "id", "required": true, "type": "long"},
        \\    {"id": 2, "name": "cat", "required": true, "type": "string"}
        \\  ]}],
        \\  "partition-specs": [{
        \\    "spec-id": 0,
        \\    "fields": [{"source-id": 2, "field-id": 1000, "name": "cat", "transform": "identity"}]
        \\  }],
        \\  "snapshots": []
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json);
    try checkReadSupport(meta);
    try std.testing.expectEqual(@as(usize, 1), meta.currentPartitionSpec().?.fields.len);
    try std.testing.expectEqualStrings("identity", meta.currentPartitionSpec().?.fields[0].transform);

    const file = DataFile{
        .path = "cat=fruit/part.parquet",
        .bounds = &.{.{ .column = "id", .lower = .{ .int = 1 }, .upper = .{ .int = 5 } }},
    };
    const q = try sql.parse(arena.allocator(), "SELECT * FROM t WHERE id > 10");
    try std.testing.expect(canSkipFile(file, q.where.?));
}

test "bucket partition transform is unsupported" {
    const json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "u",
        \\  "location": "t",
        \\  "current-schema-id": 0,
        \\  "default-spec-id": 0,
        \\  "schemas": [{"schema-id": 0, "fields": [
        \\    {"id": 1, "name": "id", "required": true, "type": "long"}
        \\  ]}],
        \\  "partition-specs": [{
        \\    "spec-id": 0,
        \\    "fields": [{"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[16]"}]
        \\  }]
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json);
    try std.testing.expectError(error.UnsupportedPartitionSpec, checkReadSupport(meta));
}

test "multiple schemas are unsupported evolution" {
    const json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "u",
        \\  "location": "t",
        \\  "current-schema-id": 1,
        \\  "schemas": [
        \\    {"schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]},
        \\    {"schema-id": 1, "fields": [
        \\      {"id": 1, "name": "id", "required": true, "type": "long"},
        \\      {"id": 2, "name": "extra", "required": false, "type": "string"}
        \\    ]}
        \\  ]
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json);
    try std.testing.expectError(error.UnsupportedSchemaEvolution, checkReadSupport(meta));
}

test "snapshot schema-id mismatch is unsupported evolution" {
    const json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "u",
        \\  "location": "t",
        \\  "current-schema-id": 0,
        \\  "current-snapshot-id": 1,
        \\  "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
        \\  "snapshots": [{
        \\    "snapshot-id": 1,
        \\    "timestamp-ms": 1,
        \\    "manifest-list": "metadata/snap.avro",
        \\    "schema-id": 7
        \\  }]
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json);
    try std.testing.expectError(error.UnsupportedSchemaEvolution, checkReadSupport(meta));
}

test "position and equality delete files are unsupported" {
    try requireDataFileContent(0);
    try std.testing.expectError(error.UnsupportedDeletes, requireDataFileContent(1));
    try std.testing.expectError(error.UnsupportedDeletes, requireDataFileContent(2));
}

test "parse decimal timestamptz uuid schema fields" {
    const json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "u",
        \\  "location": "t",
        \\  "current-schema-id": 0,
        \\  "schemas": [{
        \\    "schema-id": 0,
        \\    "fields": [
        \\      {"id": 1, "name": "ts", "required": true, "type": "timestamp"},
        \\      {"id": 2, "name": "tstz", "required": true, "type": "timestamptz"},
        \\      {"id": 3, "name": "id", "required": true, "type": "uuid"},
        \\      {"id": 4, "name": "amount", "required": true, "type": {"type": "decimal", "precision": 10, "scale": 2}}
        \\    ]
        \\  }]
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json);
    const fields = meta.currentSchema().?.fields;
    try std.testing.expectEqual(@as(usize, 4), fields.len);
    try std.testing.expectEqualStrings("timestamp", fields[0].type_name);
    try std.testing.expectEqualStrings("timestamptz", fields[1].type_name);
    try std.testing.expectEqualStrings("uuid", fields[2].type_name);
    try std.testing.expectEqualStrings("decimal", fields[3].type_name);
    try std.testing.expectEqual(@as(i32, 10), fields[3].decimal_precision);
    try std.testing.expectEqual(@as(i32, 2), fields[3].decimal_scale);
    try checkReadSupport(meta);
}
