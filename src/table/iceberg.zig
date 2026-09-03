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

pub const PartitionValue = struct {
    name: []const u8,
    source: []const u8,
    transform: []const u8,
    value: BoundValue,
};

pub const EqCol = struct {
    name: []const u8,
    i64s: []const i64 = &.{},
    strs: []const []const u8 = &.{},
};

pub const EqDelete = struct {
    cols: []const EqCol,
    len: usize,
};

pub const DataFile = struct {
    path: []const u8,
    format: FileFormat = .parquet,
    record_count: i64 = 0,
    bounds: []const ColBound = &.{},
    bytes: ?[]const u8 = null,
    content: i32 = 0,
    partition: []const PartitionValue = &.{},
    deleted_pos: []const u64 = &.{},
    eq_deletes: []const EqDelete = &.{},
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

    pub fn schemaById(self: TableMetadata, id: i32) ?*const Schema {
        for (self.schemas) |*s| {
            if (s.schema_id == id) return s;
        }
        return null;
    }

    /// Schema the current snapshot was written with, else the table's current schema.
    pub fn snapshotSchema(self: TableMetadata) ?*const Schema {
        if (self.currentSnapshot()) |snap| {
            if (snap.schema_id) |sid| {
                return self.schemaById(sid);
            }
        }
        return self.currentSchema();
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
    if (metadata.currentSnapshot()) |snap| {
        if (snap.schema_id) |sid| {
            if (metadata.schemaById(sid) == null) return error.SchemaNotFound;
        }
    }
    if (metadata.currentPartitionSpec()) |spec| {
        for (spec.fields) |f| {
            _ = try parseTransform(f.transform);
        }
    }
}

const Transform = union(enum) {
    identity,
    void,
    year,
    month,
    day,
    hour,
    bucket: u32,
    truncate: u32,
};

fn parseTransform(transform: []const u8) !Transform {
    if (std.ascii.eqlIgnoreCase(transform, "identity")) return .identity;
    if (std.ascii.eqlIgnoreCase(transform, "void")) return .void;
    if (std.ascii.eqlIgnoreCase(transform, "year")) return .year;
    if (std.ascii.eqlIgnoreCase(transform, "month")) return .month;
    if (std.ascii.eqlIgnoreCase(transform, "day")) return .day;
    if (std.ascii.eqlIgnoreCase(transform, "hour")) return .hour;
    if (std.mem.startsWith(u8, transform, "bucket[") and std.mem.endsWith(u8, transform, "]")) {
        const inner = transform["bucket[".len .. transform.len - 1];
        const n = std.fmt.parseInt(u32, inner, 10) catch return error.UnsupportedPartitionSpec;
        if (n == 0) return error.UnsupportedPartitionSpec;
        return .{ .bucket = n };
    }
    if (std.mem.startsWith(u8, transform, "truncate[") and std.mem.endsWith(u8, transform, "]")) {
        const inner = transform["truncate[".len .. transform.len - 1];
        const n = std.fmt.parseInt(u32, inner, 10) catch return error.UnsupportedPartitionSpec;
        return .{ .truncate = n };
    }
    return error.UnsupportedPartitionSpec;
}

/// Iceberg Appendix B: MurmurHash3 x86_32, seed 0.
pub fn murmur3_32(data: []const u8) u32 {
    const c1: u32 = 0xcc9e2d51;
    const c2: u32 = 0x1b873593;
    var h: u32 = 0;
    var i: usize = 0;
    while (i + 4 <= data.len) : (i += 4) {
        var k = std.mem.readInt(u32, data[i..][0..4], .little);
        k *%= c1;
        k = std.math.rotl(u32, k, 15);
        k *%= c2;
        h ^= k;
        h = std.math.rotl(u32, h, 13);
        h = h *% 5 +% 0xe6546b64;
    }
    var k: u32 = 0;
    const rem = data.len - i;
    if (rem == 3) k ^= @as(u32, data[i + 2]) << 16;
    if (rem >= 2) k ^= @as(u32, data[i + 1]) << 8;
    if (rem >= 1) {
        k ^= data[i];
        k *%= c1;
        k = std.math.rotl(u32, k, 15);
        k *%= c2;
        h ^= k;
    }
    h ^= @as(u32, @intCast(data.len));
    h ^= h >> 16;
    h *%= 0x85ebca6b;
    h ^= h >> 13;
    h *%= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

pub fn icebergHashI64(v: i64) u32 {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, v, .little);
    return murmur3_32(&buf);
}

pub fn icebergBucketI64(v: i64, n: u32) i64 {
    const h = icebergHashI64(v);
    return @intCast((h & 0x7FFFFFFF) % n);
}

fn applyTransform(tf: Transform, lit: sql.Literal) ?BoundValue {
    return switch (tf) {
        .identity => switch (lit) {
            .int => |v| .{ .int = v },
            .float => |v| .{ .float = v },
            .string => |s| .{ .string = s },
            .null => null,
        },
        .void => null,
        .bucket => |n| switch (lit) {
            .int => |v| .{ .int = icebergBucketI64(v, n) },
            else => null,
        },
        .truncate => |w| switch (lit) {
            .int => |v| blk: {
                const ww: i64 = @intCast(w);
                if (ww == 0) break :blk null;
                break :blk .{ .int = v - @mod(v, ww) };
            },
            .string => |s| .{ .string = s[0..@min(s.len, @as(usize, @intCast(w)))] },
            else => null,
        },
        .year => switch (lit) {
            .int => |v| .{ .int = yearFromEpoch(v) },
            else => null,
        },
        .month => switch (lit) {
            .int => |v| .{ .int = monthFromEpoch(v) },
            else => null,
        },
        .day => switch (lit) {
            .int => |v| .{ .int = dayFromEpoch(v) },
            else => null,
        },
        .hour => switch (lit) {
            .int => |v| .{ .int = @divTrunc(v, 3_600_000_000) },
            else => null,
        },
    };
}

fn yearFromEpoch(v: i64) i64 {
    const days = if (@abs(v) > 40_000) @divTrunc(v, 86_400_000_000) else v;
    return @divTrunc(days, 365);
}

fn monthFromEpoch(v: i64) i64 {
    const days = if (@abs(v) > 40_000) @divTrunc(v, 86_400_000_000) else v;
    return @divTrunc(days, 30);
}

fn dayFromEpoch(v: i64) i64 {
    if (@abs(v) > 40_000) return @divTrunc(v, 86_400_000_000);
    return v;
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

    var data: std.ArrayList(DataFile) = .empty;
    var pos_paths: std.ArrayList([]const u8) = .empty;
    var eq_paths: std.ArrayList([]const u8) = .empty;
    const schema_fields = if (metadata.snapshotSchema()) |s| s.fields else if (metadata.currentSchema()) |s| s.fields else &.{};
    const spec = metadata.currentPartitionSpec();
    for (manifest_paths) |rel| {
        const man_path = resolveListedPath(allocator, table_dir, rel) catch continue;
        const man_buf = readLocation(allocator, t, man_path) catch continue;
        const entries = avro.readIcebergEntries(allocator, man_buf) catch continue;
        for (entries) |e| {
            if (e.status == 2) continue;
            const path = try resolveListedPath(allocator, table_dir, e.path);
            if (e.content == 1) {
                try pos_paths.append(allocator, path);
                continue;
            }
            if (e.content == 2) {
                try eq_paths.append(allocator, path);
                continue;
            }
            if (e.content != 0) return error.UnsupportedDeletes;
            try data.append(allocator, try dataFileFromEntry(allocator, path, schema_fields, spec, e));
        }
    }
    if (data.items.len == 0) return error.ManifestsNeedAvro;

    const eq_deletes = try loadEqDeletes(allocator, t, eq_paths.items);
    for (data.items) |*f| f.eq_deletes = eq_deletes;

    for (pos_paths.items) |pp| {
        const hits = loadPosDeletes(allocator, t, pp) catch continue;
        for (hits) |hit| {
            for (data.items) |*f| {
                if (pathsReferToSameFile(f.path, hit.path)) {
                    f.deleted_pos = try appendPos(allocator, f.deleted_pos, hit.pos);
                }
            }
        }
    }
    return data.items;
}

const PosHit = struct { path: []const u8, pos: u64 };

fn pathsReferToSameFile(a: []const u8, b: []const u8) bool {
    if (std.mem.eql(u8, a, b)) return true;
    const longer, const shorter = if (a.len >= b.len) .{ a, b } else .{ b, a };
    if (shorter.len == 0 or !std.mem.endsWith(u8, longer, shorter)) return false;
    if (shorter.len == longer.len) return true;
    const prev = longer[longer.len - shorter.len - 1];
    return prev == '/' or prev == '\\';
}

fn appendPos(allocator: std.mem.Allocator, prev: []const u64, pos: u64) ![]const u64 {
    var out = try allocator.alloc(u64, prev.len + 1);
    @memcpy(out[0..prev.len], prev);
    out[prev.len] = pos;
    return out;
}

fn loadPosDeletes(allocator: std.mem.Allocator, t: Transport, path: []const u8) ![]PosHit {
    var reader = try parquet.openLocation(allocator, t, path);
    defer reader.close();
    const n_cols = reader.numColumns();
    var path_i: ?i32 = null;
    var pos_i: ?i32 = null;
    var i: i32 = 0;
    while (i < n_cols) : (i += 1) {
        const meta = reader.column(i) orelse continue;
        if (std.ascii.eqlIgnoreCase(meta.name, "file_path")) path_i = i;
        if (std.ascii.eqlIgnoreCase(meta.name, "pos")) pos_i = i;
    }
    const pi = path_i orelse return error.UnsupportedDeletes;
    const oi = pos_i orelse return error.UnsupportedDeletes;
    const picked = [_]i32{ pi, oi };
    var br = try reader.openBatch(&picked, 65536);
    defer br.close();
    var hits: std.ArrayList(PosHit) = .empty;
    while (try br.next()) |rb_raw| {
        var rb = rb_raw;
        defer rb.deinit();
        const paths = try rb.columnValues(0);
        const poss = try rb.columnValues(1);
        const n: usize = @intCast(paths.n);
        var row: usize = 0;
        while (row < n) : (row += 1) {
            const ba = loadBa(paths.ptr, row);
            const pslice = if (ba.length <= 0) "" else ba.data[0..@intCast(ba.length)];
            const pos = loadI64(poss.ptr, row);
            try hits.append(allocator, .{
                .path = try allocator.dupe(u8, pslice),
                .pos = @intCast(pos),
            });
        }
    }
    return hits.items;
}

fn loadBa(ptr: [*]const u8, row: usize) parquet.ByteArray {
    var v: parquet.ByteArray = undefined;
    const bytes = std.mem.asBytes(&v);
    @memcpy(bytes, ptr[row * bytes.len ..][0..bytes.len]);
    return v;
}

fn loadI64(ptr: [*]const u8, row: usize) i64 {
    var v: i64 = undefined;
    @memcpy(std.mem.asBytes(&v), ptr[row * 8 ..][0..8]);
    return v;
}

fn loadEqDeletes(allocator: std.mem.Allocator, t: Transport, paths: []const []const u8) ![]const EqDelete {
    if (paths.len == 0) return &.{};
    var out: std.ArrayList(EqDelete) = .empty;
    for (paths) |p| {
        const d = loadOneEqDelete(allocator, t, p) catch continue;
        try out.append(allocator, d);
    }
    return out.items;
}

fn loadOneEqDelete(allocator: std.mem.Allocator, t: Transport, path: []const u8) !EqDelete {
    var reader = try parquet.openLocation(allocator, t, path);
    defer reader.close();
    const n_cols: usize = @intCast(reader.numColumns());
    if (n_cols == 0) return error.UnsupportedDeletes;
    const picked = try allocator.alloc(i32, n_cols);
    var i: i32 = 0;
    while (i < reader.numColumns()) : (i += 1) picked[@intCast(i)] = i;
    var br = try reader.openBatch(picked, 65536);
    defer br.close();
    var cols: std.ArrayList(EqCol) = .empty;
    var n_rows: usize = 0;
    var first = true;
    while (try br.next()) |rb_raw| {
        var rb = rb_raw;
        defer rb.deinit();
        if (first) {
            var ci: i32 = 0;
            while (ci < reader.numColumns()) : (ci += 1) {
                const meta = reader.column(ci) orelse return error.ParquetOpenFailed;
                try cols.append(allocator, .{
                    .name = try allocator.dupe(u8, meta.name),
                    .i64s = &.{},
                    .strs = &.{},
                });
            }
            first = false;
        }
        const nrows: usize = @intCast(rb.numRows());
        var ci: i32 = 0;
        while (ci < reader.numColumns()) : (ci += 1) {
            const meta = reader.column(ci) orelse return error.ParquetOpenFailed;
            const vals = try rb.columnValues(ci);
            var col = cols.items[@intCast(ci)];
            switch (meta.physical_type) {
                .int64 => {
                    const more = try allocator.alloc(i64, col.i64s.len + nrows);
                    @memcpy(more[0..col.i64s.len], col.i64s);
                    var r: usize = 0;
                    while (r < nrows) : (r += 1) more[col.i64s.len + r] = loadI64(vals.ptr, r);
                    col.i64s = more;
                },
                .byte_array => {
                    const more = try allocator.alloc([]const u8, col.strs.len + nrows);
                    @memcpy(more[0..col.strs.len], col.strs);
                    var r: usize = 0;
                    while (r < nrows) : (r += 1) {
                        const ba = loadBa(vals.ptr, r);
                        more[col.strs.len + r] = try allocator.dupe(u8, if (ba.length <= 0) "" else ba.data[0..@intCast(ba.length)]);
                    }
                    col.strs = more;
                },
                else => return error.UnsupportedDeletes,
            }
            cols.items[@intCast(ci)] = col;
        }
        n_rows += nrows;
    }
    return .{ .cols = cols.items, .len = n_rows };
}

fn dataFileFromEntry(
    allocator: std.mem.Allocator,
    path: []const u8,
    schema_fields: []const SchemaField,
    spec: ?*const PartitionSpec,
    e: avro.ManifestEntry,
) !DataFile {
    const format: FileFormat = if (std.ascii.eqlIgnoreCase(e.format, "AVRO")) .avro else .parquet;
    return .{
        .path = path,
        .format = format,
        .record_count = e.record_count,
        .bounds = try boundsFromMaps(allocator, schema_fields, e.lower, e.upper),
        .content = e.content,
        .partition = try partitionFromPath(allocator, path, spec, schema_fields),
    };
}

fn partitionFromPath(
    allocator: std.mem.Allocator,
    path: []const u8,
    spec: ?*const PartitionSpec,
    schema_fields: []const SchemaField,
) ![]const PartitionValue {
    const s = spec orelse return &.{};
    if (s.fields.len == 0) return &.{};
    var out: std.ArrayList(PartitionValue) = .empty;
    for (s.fields) |f| {
        const key = try std.fmt.allocPrint(allocator, "{s}=", .{f.name});
        const idx = std.mem.indexOf(u8, path, key) orelse continue;
        const start = idx + key.len;
        var end = start;
        while (end < path.len and path[end] != '/' and path[end] != '\\') end += 1;
        const raw = path[start..end];
        const src = if (fieldById(schema_fields, f.source_id)) |sf| sf.name else f.name;
        const tf = parseTransform(f.transform) catch .identity;
        const value: BoundValue = switch (tf) {
            .identity => blk: {
                if (std.fmt.parseInt(i64, raw, 10)) |n| {
                    break :blk .{ .int = n };
                } else |_| {
                    break :blk .{ .string = try allocator.dupe(u8, raw) };
                }
            },
            .bucket, .year, .month, .day, .hour, .truncate => blk: {
                const n = std.fmt.parseInt(i64, raw, 10) catch continue;
                break :blk .{ .int = n };
            },
            .void => continue,
        };
        try out.append(allocator, .{
            .name = f.name,
            .source = src,
            .transform = f.transform,
            .value = value,
        });
    }
    return out.items;
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
    try avro.writeDataManifest(man_path, &data_paths, &counts, &lower, &upper, 2, &.{});
}

pub fn writeLakeFixture(allocator: std.mem.Allocator, io: std.Io, table_dir: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try std.Io.Dir.cwd().createDirPath(io, try std.fs.path.join(a, &.{ table_dir, "metadata" }));
    try std.Io.Dir.cwd().createDirPath(io, try std.fs.path.join(a, &.{ table_dir, "data" }));

    const ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const prices = [_]i64{ 50, 80, 100, 120, 150, 200, 90, 110, 300, 75 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg", "veg", "fruit", "dairy", "fruit", "veg", "dairy", "fruit" };

    var groups: [4]std.ArrayList(usize) = .{ .empty, .empty, .empty, .empty };
    for (ids, 0..) |id, i| {
        const b: usize = @intCast(icebergBucketI64(id, 4));
        try groups[b].append(a, i);
    }

    var rel_paths: std.ArrayList([*:0]const u8) = .empty;
    var counts: std.ArrayList(i64) = .empty;
    var lower: std.ArrayList(i64) = .empty;
    var upper: std.ArrayList(i64) = .empty;
    var contents: std.ArrayList(i32) = .empty;
    var id1_rel: ?[*:0]const u8 = null;
    var id1_pos: i64 = 0;

    for (groups, 0..) |g, b| {
        if (g.items.len == 0) continue;
        const dir = try std.fmt.allocPrint(a, "{s}/data/id_bucket={d}", .{ table_dir, b });
        try std.Io.Dir.cwd().createDirPath(io, dir);
        const rel = try allocZ(a, try std.fmt.allocPrint(a, "data/id_bucket={d}/part-{d}.parquet", .{ b, b }));
        const abs = try allocZ(a, try std.fmt.allocPrint(a, "{s}/{s}", .{ table_dir, rel }));
        var g_ids = try a.alloc(i64, g.items.len);
        var g_prices = try a.alloc(i64, g.items.len);
        var g_cats = try a.alloc([*:0]const u8, g.items.len);
        var lo: i64 = std.math.maxInt(i64);
        var hi: i64 = std.math.minInt(i64);
        for (g.items, 0..) |src, j| {
            g_ids[j] = ids[src];
            g_prices[j] = prices[src];
            g_cats[j] = cats[src];
            lo = @min(lo, prices[src]);
            hi = @max(hi, prices[src]);
            if (ids[src] == 1) {
                id1_rel = rel;
                id1_pos = @intCast(j);
            }
        }
        try parquet.writeSalesRows(abs, g_ids, g_prices, g_cats);
        try rel_paths.append(a, rel);
        try counts.append(a, @intCast(g.items.len));
        try lower.append(a, lo);
        try upper.append(a, hi);
        try contents.append(a, 0);
    }

    const pos_rel: [:0]const u8 = "data/pos-deletes.parquet";
    const eq_rel: [:0]const u8 = "data/eq-deletes.parquet";
    const pos_abs = try allocZ(a, try std.fmt.allocPrint(a, "{s}/{s}", .{ table_dir, pos_rel }));
    const eq_abs = try allocZ(a, try std.fmt.allocPrint(a, "{s}/{s}", .{ table_dir, eq_rel }));
    const pos_target = id1_rel orelse return error.InvalidMetadata;
    const pos_files = [_][*:0]const u8{pos_target};
    const pos_vals = [_]i64{id1_pos};
    try parquet.writePosDeletes(pos_abs, &pos_files, &pos_vals);
    const eq_ids = [_]i64{10};
    try parquet.writeEqI64Deletes(eq_abs, "id", &eq_ids);

    try rel_paths.append(a, pos_rel);
    try counts.append(a, 1);
    try lower.append(a, 0);
    try upper.append(a, 0);
    try contents.append(a, 1);

    try rel_paths.append(a, eq_rel);
    try counts.append(a, 1);
    try lower.append(a, 0);
    try upper.append(a, 0);
    try contents.append(a, 2);

    try writeText(io, try std.fs.path.join(a, &.{ table_dir, "metadata", "version-hint.text" }), "1\n");
    try writeText(io, try std.fs.path.join(a, &.{ table_dir, "metadata", "v1.metadata.json" }),
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
        \\  "location": "iceberg_lake",
        \\  "last-updated-ms": 1700000000000,
        \\  "last-column-id": 4,
        \\  "current-snapshot-id": 1,
        \\  "current-schema-id": 1,
        \\  "default-spec-id": 0,
        \\  "partition-specs": [{
        \\    "spec-id": 0,
        \\    "fields": [{"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[4]"}]
        \\  }],
        \\  "schemas": [
        \\    {"schema-id": 0, "fields": [
        \\      {"id": 1, "name": "id", "required": true, "type": "long"},
        \\      {"id": 2, "name": "price", "required": true, "type": "long"},
        \\      {"id": 3, "name": "category", "required": true, "type": "string"}
        \\    ]},
        \\    {"schema-id": 1, "fields": [
        \\      {"id": 1, "name": "id", "required": true, "type": "long"},
        \\      {"id": 2, "name": "price", "required": true, "type": "long"},
        \\      {"id": 3, "name": "category", "required": true, "type": "string"},
        \\      {"id": 4, "name": "note", "required": false, "type": "string"}
        \\    ]}
        \\  ],
        \\  "snapshots": [{
        \\    "snapshot-id": 1,
        \\    "timestamp-ms": 1700000000000,
        \\    "sequence-number": 1,
        \\    "schema-id": 1,
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
    try avro.writeDataManifest(
        man_path,
        rel_paths.items,
        counts.items,
        lower.items,
        upper.items,
        2,
        contents.items,
    );
}

fn allocZ(allocator: std.mem.Allocator, s: []const u8) ![:0]u8 {
    return allocator.dupeZ(u8, s);
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
    return !mayMatch(file, expr) or !partitionMayMatch(file, expr);
}

fn mayMatch(file: DataFile, expr: *const sql.BoolExpr) bool {
    return switch (expr.*) {
        .cmp => |c| cmpMayMatch(file, c),
        .isnull => true,
        .like, .in_list, .between, .in_query, .cmp_query => true,
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

fn partitionMayMatch(file: DataFile, expr: *const sql.BoolExpr) bool {
    return switch (expr.*) {
        .cmp => |c| partitionCmpMayMatch(file, c),
        .in_list => |p| partitionInMayMatch(file, p),
        .isnull, .like, .between, .in_query, .cmp_query => true,
        .@"and" => |b| partitionMayMatch(file, b.left) and partitionMayMatch(file, b.right),
        .@"or" => |b| partitionMayMatch(file, b.left) or partitionMayMatch(file, b.right),
    };
}

fn partitionCmpMayMatch(file: DataFile, pred: sql.Cmp) bool {
    const col_name = switch (pred.left) {
        .column => |n| n,
        .agg => return true,
    };
    const pv = findPartition(file, col_name) orelse return true;
    const tf = parseTransform(pv.transform) catch return true;
    if (pred.op == .eq) {
        const want = applyTransform(tf, pred.literal) orelse return true;
        return boundValuesEqual(pv.value, want);
    }
    if (pred.op == .ne) return true;
    return true;
}

fn partitionInMayMatch(file: DataFile, pred: sql.InList) bool {
    if (pred.negated) return true;
    const col_name = switch (pred.left) {
        .column => |n| n,
        .agg => return true,
    };
    const pv = findPartition(file, col_name) orelse return true;
    const tf = parseTransform(pv.transform) catch return true;
    for (pred.values) |lit| {
        const want = applyTransform(tf, lit) orelse continue;
        if (boundValuesEqual(pv.value, want)) return true;
    }
    return pred.values.len == 0;
}

fn findPartition(file: DataFile, name: []const u8) ?PartitionValue {
    for (file.partition) |p| {
        if (std.ascii.eqlIgnoreCase(p.source, name) or std.ascii.eqlIgnoreCase(p.name, name)) return p;
    }
    return null;
}

fn boundValuesEqual(a: BoundValue, b: BoundValue) bool {
    return switch (a) {
        .int => |x| switch (b) {
            .int => |y| x == y,
            .float => |y| @as(f64, @floatFromInt(x)) == y,
            .string => false,
        },
        .float => |x| switch (b) {
            .int => |y| x == @as(f64, @floatFromInt(y)),
            .float => |y| x == y,
            .string => false,
        },
        .string => |x| switch (b) {
            .string => |y| std.mem.eql(u8, x, y),
            else => false,
        },
    };
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
        .string, .null => null,
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
                if (!std.ascii.eqlIgnoreCase(inner, "decimal") and
                    !std.ascii.eqlIgnoreCase(inner, "list") and
                    !std.ascii.eqlIgnoreCase(inner, "struct") and
                    !std.ascii.eqlIgnoreCase(inner, "map")) return error.UnsupportedNested;
                type_name = inner;
                if (std.ascii.eqlIgnoreCase(inner, "decimal")) {
                    decimal_precision = @intCast(try getInt(to, "precision"));
                    decimal_scale = @intCast(if (to.get("scale")) |sv| try asInt(sv) else 0);
                }
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

test "bucket partition transform is allowed and prunes" {
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
    try checkReadSupport(meta);
    try std.testing.expectEqualStrings("bucket[16]", meta.currentPartitionSpec().?.fields[0].transform);

    const bucket = icebergBucketI64(1, 16);
    const other = if (bucket == 0) @as(i64, 1) else @as(i64, 0);
    const file = DataFile{
        .path = "id_bucket=0/part.parquet",
        .partition = &.{.{
            .name = "id_bucket",
            .source = "id",
            .transform = "bucket[16]",
            .value = .{ .int = other },
        }},
    };
    const q = try sql.parse(arena.allocator(), "SELECT * FROM t WHERE id = 1");
    try std.testing.expect(canSkipFile(file, q.where.?));
}

test "multiple schemas and snapshot schema-id are allowed" {
    const json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "u",
        \\  "location": "t",
        \\  "current-schema-id": 1,
        \\  "current-snapshot-id": 1,
        \\  "schemas": [
        \\    {"schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]},
        \\    {"schema-id": 1, "fields": [
        \\      {"id": 1, "name": "id", "required": true, "type": "long"},
        \\      {"id": 2, "name": "extra", "required": false, "type": "string"}
        \\    ]}
        \\  ],
        \\  "snapshots": [{
        \\    "snapshot-id": 1,
        \\    "timestamp-ms": 1,
        \\    "manifest-list": "m.avro",
        \\    "schema-id": 0
        \\  }]
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json);
    try checkReadSupport(meta);
    try std.testing.expectEqual(@as(i32, 0), meta.snapshotSchema().?.schema_id);
    try std.testing.expectEqual(@as(i32, 1), meta.currentSchema().?.schema_id);
    try std.testing.expectEqual(@as(usize, 2), meta.currentSchema().?.fields.len);
}

test "missing snapshot schema-id is SchemaNotFound" {
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
    try std.testing.expectError(error.SchemaNotFound, checkReadSupport(meta));
}

test "unknown partition transform is still unsupported" {
    const json =
        \\{
        \\  "format-version": 2,
        \\  "table-uuid": "u",
        \\  "location": "t",
        \\  "current-schema-id": 0,
        \\  "default-spec-id": 0,
        \\  "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]}],
        \\  "partition-specs": [{
        \\    "spec-id": 0,
        \\    "fields": [{"source-id": 1, "field-id": 1000, "name": "x", "transform": "unknown"}]
        \\  }]
        \\}
    ;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const meta = try parseMetadata(arena.allocator(), json);
    try std.testing.expectError(error.UnsupportedPartitionSpec, checkReadSupport(meta));
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
