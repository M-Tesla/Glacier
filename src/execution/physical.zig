//! Scan / filter / aggregate / having / window / project / scalars / distinct / order / offset / limit.
//! INNER JOIN is a hash join of two scans. Window is `OVER` without a frame clause.

const std = @import("std");
const parquet = @import("../formats/parquet_wrap.zig");
const avro = @import("../formats/avro_wrap.zig");
const native = @import("../formats/native.zig");
const sql = @import("../sql/parser.zig");
const batch_mod = @import("batch.zig");
const iceberg = @import("../table/iceberg.zig");
const vfs = @import("../vfs/source.zig");
const spill = @import("../vfs/spill.zig");

const Batch = batch_mod.Batch;
const Column = batch_mod.Column;
const DataType = batch_mod.DataType;

pub const ScanStats = struct {
    files_opened: usize = 0,
    files_pruned: usize = 0,
};

pub fn execute(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
    right_files: ?[]const iceberg.DataFile,
) !Batch {
    if (query.isLiteralOnly()) {
        var batch = try batchFromLiterals(allocator, query, 1);
        if (query.offset != null or query.limit != null) {
            batch = try takeRange(allocator, batch, query.offset orelse 0, query.limit);
        }
        return batch;
    }
    if (query.having != null and !query.needsAgg()) return error.InvalidSyntax;
    if (query.join) |_| {
        const rf = right_files orelse return error.TableNotFound;
        return executeJoin(allocator, t, files, rf, query, stats);
    }
    return executeNoJoin(allocator, t, files, query, fallback_schema, stats);
}

fn executeNoJoin(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
) !Batch {
    if (files.len == 0) return error.TableNotFound;

    if (canStreamQuery(query) and filesAreStreamable(files)) {
        return executeStream(allocator, t, files, query, fallback_schema, stats);
    }
    if (filesAreStreamable(files) and !query.hasWindow()) {
        return executeCapped(allocator, t, files, query, fallback_schema, stats);
    }

    var parts: std.ArrayList(Batch) = .empty;
    const wanted = try collectScanColumns(allocator, query);
    for (files) |file| {
        if (query.where) |expr| {
            if (iceberg.canSkipFile(file, expr)) {
                if (stats) |s| s.files_pruned += 1;
                continue;
            }
        }
        if (stats) |s| s.files_opened += 1;
        const scanned_file = switch (file.format) {
            .parquet => try scanParquet(allocator, t, file, wanted),
            .avro => try scanAvro(allocator, t, file),
            .glacier => try scanNativeAll(allocator, t, file, wanted, query.where),
        };
        try parts.append(allocator, scanned_file);
    }

    if (parts.items.len == 0) {
        if (fallback_schema) |fields| return emptyFromSchema(allocator, fields);
        return .{ .columns = &.{}, .len = 0 };
    }
    const scanned = if (parts.items.len == 1)
        parts.items[0]
    else
        try concatBatches(allocator, parts.items);
    return applyTail(allocator, scanned, query, .{});
}

fn scanAllQuery(from: []const u8) sql.Query {
    return .{
        .items = &.{.star},
        .from = from,
        .where = null,
        .group_by = &.{},
        .having = null,
        .order_by = &.{},
        .distinct = false,
        .limit = null,
        .offset = null,
    };
}

fn tableQual(from: []const u8, alias: ?[]const u8, fallback: []const u8) []const u8 {
    if (alias) |a| {
        if (a.len > 0) return a;
    }
    const trimmed = std.mem.trimEnd(u8, from, "/");
    const stem = std.fs.path.stem(trimmed);
    if (stem.len > 0) return stem;
    return fallback;
}

fn executeJoin(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    left_files: []const iceberg.DataFile,
    right_files: []const iceberg.DataFile,
    query: sql.Query,
    stats: ?*ScanStats,
) !Batch {
    const j = query.join orelse return error.InvalidSyntax;
    if (left_files.len == 0 or right_files.len == 0) return error.TableNotFound;
    const lq = tableQual(query.from, query.from_alias, "left");
    const rq = tableQual(j.table, j.alias, "right");
    if (std.ascii.eqlIgnoreCase(lq, rq)) return error.InvalidSyntax;

    const left = try executeNoJoin(allocator, t, left_files, scanAllQuery(query.from), null, stats);
    const right = try executeNoJoin(allocator, t, right_files, scanAllQuery(j.table), null, stats);
    const keys = try resolveJoinKeys(allocator, left, right, lq, rq, j.eqs);
    const joined = try hashJoin(allocator, left, right, keys.left, keys.right, lq, rq);
    return applyTail(allocator, joined, query, .{});
}

const JoinKeys = struct { left: []const usize, right: []const usize };

fn resolveJoinKeys(
    allocator: std.mem.Allocator,
    left: Batch,
    right: Batch,
    lq: []const u8,
    rq: []const u8,
    eqs: []const sql.JoinEq,
) !JoinKeys {
    if (eqs.len == 0) return error.InvalidSyntax;
    const l_idxs = try allocator.alloc(usize, eqs.len);
    const r_idxs = try allocator.alloc(usize, eqs.len);
    for (eqs, 0..) |eq, i| {
        const l_side = try bindJoinName(eq.left, left, right, lq, rq);
        const r_side = try bindJoinName(eq.right, left, right, lq, rq);
        if (l_side.side == r_side.side) return error.InvalidSyntax;
        if (l_side.side == .left) {
            l_idxs[i] = l_side.idx;
            r_idxs[i] = r_side.idx;
        } else {
            l_idxs[i] = r_side.idx;
            r_idxs[i] = l_side.idx;
        }
        if (left.columns[l_idxs[i]].data_type != right.columns[r_idxs[i]].data_type)
            return error.TypeMismatch;
    }
    return .{ .left = l_idxs, .right = r_idxs };
}

const JoinSide = enum { left, right };
const BoundName = struct { side: JoinSide, idx: usize };

fn bindJoinName(
    q: sql.QualName,
    left: Batch,
    right: Batch,
    lq: []const u8,
    rq: []const u8,
) !BoundName {
    if (q.qualifier) |qual| {
        if (std.ascii.eqlIgnoreCase(qual, lq)) {
            const idx = left.columnIndex(q.name) orelse return error.ColumnNotFound;
            return .{ .side = .left, .idx = idx };
        }
        if (std.ascii.eqlIgnoreCase(qual, rq)) {
            const idx = right.columnIndex(q.name) orelse return error.ColumnNotFound;
            return .{ .side = .right, .idx = idx };
        }
        return error.ColumnNotFound;
    }
    const li = left.columnIndex(q.name);
    const ri = right.columnIndex(q.name);
    if (li != null and ri != null) return error.AmbiguousColumn;
    if (li) |idx| return .{ .side = .left, .idx = idx };
    if (ri) |idx| return .{ .side = .right, .idx = idx };
    return error.ColumnNotFound;
}

fn qualifyBatch(allocator: std.mem.Allocator, batch: Batch, qual: []const u8) !Batch {
    const columns = try allocator.alloc(Column, batch.columns.len);
    for (batch.columns, columns) |src, *dst| {
        dst.* = src;
        dst.name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ qual, src.name });
    }
    return .{ .columns = columns, .len = batch.len };
}

fn hconcat(allocator: std.mem.Allocator, left: Batch, right: Batch) !Batch {
    if (left.len != right.len) return error.SchemaMismatch;
    const columns = try allocator.alloc(Column, left.columns.len + right.columns.len);
    @memcpy(columns[0..left.columns.len], left.columns);
    @memcpy(columns[left.columns.len..], right.columns);
    return .{ .columns = columns, .len = left.len };
}

fn hashJoin(
    allocator: std.mem.Allocator,
    left: Batch,
    right: Batch,
    left_idxs: []const usize,
    right_idxs: []const usize,
    lq: []const u8,
    rq: []const u8,
) !Batch {
    var map: std.StringArrayHashMapUnmanaged(std.ArrayList(usize)) = .empty;
    var row: usize = 0;
    while (row < right.len) : (row += 1) {
        if (rowHasNullKey(right, row, right_idxs)) continue;
        const key = try encodeKey(allocator, right, row, right_idxs);
        if (map.getPtr(key)) |list| {
            try list.append(allocator, row);
            allocator.free(key);
        } else {
            var list: std.ArrayList(usize) = .empty;
            try list.append(allocator, row);
            try map.put(allocator, key, list);
        }
    }

    var left_rows: std.ArrayList(usize) = .empty;
    var right_rows: std.ArrayList(usize) = .empty;
    row = 0;
    while (row < left.len) : (row += 1) {
        if (rowHasNullKey(left, row, left_idxs)) continue;
        const key = try encodeKey(allocator, left, row, left_idxs);
        defer allocator.free(key);
        if (map.get(key)) |list| {
            for (list.items) |rrow| {
                try left_rows.append(allocator, row);
                try right_rows.append(allocator, rrow);
            }
        }
    }

    const l_g = try gatherRows(allocator, left, left_rows.items);
    const r_g = try gatherRows(allocator, right, right_rows.items);
    const l_q = try qualifyBatch(allocator, l_g, lq);
    const r_q = try qualifyBatch(allocator, r_g, rq);
    return hconcat(allocator, l_q, r_q);
}

fn rowHasNullKey(batch: Batch, row: usize, idxs: []const usize) bool {
    for (idxs) |i| {
        if (batch.columns[i].isNull(row)) return true;
    }
    return false;
}

const Tail = struct {
    where_done: bool = false,
    agg_done: bool = false,
    sort_done: bool = false,
    distinct_done: bool = false,
    range_done: bool = false,
};

fn applyTail(allocator: std.mem.Allocator, scanned: Batch, query: sql.Query, t: Tail) !Batch {
    var batch = scanned;
    if (!t.where_done) {
        if (query.where) |expr| {
            batch = try filterExpr(allocator, batch, expr, null);
        }
    }
    if (query.needsAgg() and query.hasWindow()) return error.UnsupportedSql;
    if (query.needsAgg() and !t.agg_done) {
        batch = try aggregateBatch(allocator, batch, query);
        if (query.having) |expr| {
            batch = try filterExpr(allocator, batch, expr, query);
        }
    }
    if (query.hasWindow()) {
        batch = try applyWindows(allocator, batch, query);
    }
    if (query.order_by.len > 0 and !t.sort_done) {
        batch = try sortBatch(allocator, batch, query.order_by);
    }
    if (!query.needsAgg() and !query.isStar()) {
        batch = try projectItems(allocator, batch, query);
    }
    if (query.distinct and !t.distinct_done) {
        batch = try distinctBatch(allocator, batch);
    }
    if (!t.range_done and (query.offset != null or query.limit != null)) {
        batch = try takeRange(allocator, batch, query.offset orelse 0, query.limit);
    }
    return batch;
}

fn applyWindows(allocator: std.mem.Allocator, input: Batch, query: sql.Query) !Batch {
    var n_win: usize = 0;
    for (query.items) |item| {
        if (item == .window) n_win += 1;
    }
    if (n_win == 0) return input;
    const columns = try allocator.alloc(Column, input.columns.len + n_win);
    @memcpy(columns[0..input.columns.len], input.columns);
    var i = input.columns.len;
    for (query.items) |item| {
        if (item == .window) {
            columns[i] = try evalWindow(allocator, input, item.window);
            i += 1;
        }
    }
    return .{ .columns = columns, .len = input.len };
}

fn windowName(allocator: std.mem.Allocator, w: sql.Window) ![]const u8 {
    if (w.alias) |a| return a;
    const prefix = switch (w.kind) {
        .count => "count",
        .sum => "sum",
        .avg => "avg",
        .min => "min",
        .max => "max",
        .row_number => "row_number",
        .rank => "rank",
        .dense_rank => "dense_rank",
    };
    if (w.arg) |arg| return std.fmt.allocPrint(allocator, "{s}_{s}", .{ prefix, arg });
    return prefix;
}

fn windowToAgg(kind: sql.WindowKind) !sql.AggKind {
    return switch (kind) {
        .count => .count,
        .sum => .sum,
        .avg => .avg,
        .min => .min,
        .max => .max,
        .row_number, .rank, .dense_rank => error.InvalidSyntax,
    };
}

fn partitionRows(
    allocator: std.mem.Allocator,
    input: Batch,
    partition_by: []const []const u8,
) ![][]usize {
    if (input.len == 0) return &.{};
    if (partition_by.len == 0) {
        const all = try allocator.alloc(usize, input.len);
        for (all, 0..) |*slot, i| slot.* = i;
        const groups = try allocator.alloc([]usize, 1);
        groups[0] = all;
        return groups;
    }
    const idxs = try allocator.alloc(usize, partition_by.len);
    for (partition_by, 0..) |name, i| idxs[i] = try input.lookup(name);

    var map: std.StringArrayHashMapUnmanaged(std.ArrayList(usize)) = .empty;
    var row: usize = 0;
    while (row < input.len) : (row += 1) {
        const key = try encodeKey(allocator, input, row, idxs);
        if (map.getPtr(key)) |list| {
            try list.append(allocator, row);
            allocator.free(key);
        } else {
            var list: std.ArrayList(usize) = .empty;
            try list.append(allocator, row);
            try map.put(allocator, key, list);
        }
    }
    const groups = try allocator.alloc([]usize, map.count());
    for (map.values(), 0..) |list, i| groups[i] = list.items;
    return groups;
}

fn sortPartition(input: Batch, rows: []usize, keys: []const sql.OrderBy) !void {
    if (keys.len == 0 or rows.len <= 1) return;
    for (keys) |ob| _ = try input.lookup(ob.column);
    std.mem.sort(usize, rows, SortCtx{ .batch = input, .keys = keys }, SortCtx.lessThan);
}

fn orderKeysEqual(input: Batch, keys: []const sql.OrderBy, a: usize, b: usize) bool {
    if (keys.len == 0) return true;
    for (keys) |ob| {
        const col_i = input.lookup(ob.column) catch return false;
        if (compareCells(input.columns[col_i], a, b) != .eq) return false;
    }
    return true;
}

fn evalWindow(allocator: std.mem.Allocator, input: Batch, w: sql.Window) !Column {
    const n = input.len;
    const name = try windowName(allocator, w);
    const groups = try partitionRows(allocator, input, w.spec.partition_by);
    for (groups) |g| try sortPartition(input, g, w.spec.order_by);

    switch (w.kind) {
        .row_number, .rank, .dense_rank => {
            const i64s = try allocator.alloc(i64, n);
            if (n > 0) @memset(i64s, 0);
            for (groups) |g| {
                var rn: i64 = 0;
                var rank: i64 = 1;
                var dense: i64 = 1;
                for (g, 0..) |orig, i| {
                    rn += 1;
                    if (i == 0) {
                        rank = 1;
                        dense = 1;
                    } else if (!orderKeysEqual(input, w.spec.order_by, orig, g[i - 1])) {
                        rank = rn;
                        dense += 1;
                    }
                    i64s[orig] = switch (w.kind) {
                        .row_number => rn,
                        .rank => rank,
                        .dense_rank => dense,
                        else => unreachable,
                    };
                }
            }
            return .{ .name = name, .data_type = .int64, .len = n, .i64s = i64s };
        },
        .count, .sum, .avg, .min, .max => {
            const kind = try windowToAgg(w.kind);
            const src = if (w.arg) |arg| blk: {
                const idx = try input.lookup(arg);
                break :blk input.columns[idx];
            } else null;
            if (n == 0) {
                var col = try finalizeAgg(allocator, .{ .kind = kind }, src, name, 1);
                col.len = 0;
                return col;
            }
            const snaps = try allocator.alloc(AggState, n);
            for (groups) |g| {
                var state = AggState{ .kind = kind };
                if (w.spec.order_by.len == 0) {
                    for (g) |orig| try feed(&state, src, orig, allocator);
                    for (g) |orig| snaps[orig] = state;
                } else {
                    state = AggState{ .kind = kind };
                    for (g) |orig| {
                        try feed(&state, src, orig, allocator);
                        snaps[orig] = state;
                    }
                }
            }
            const template = snaps[0];
            var col = try finalizeAgg(allocator, template, src, name, n);
            var row: usize = 0;
            while (row < n) : (row += 1) {
                try writeAggCell(&col, snaps[row], row, allocator);
            }
            return col;
        },
    }
}

pub fn copyTo(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    dest: []const u8,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
    right_files: ?[]const iceberg.DataFile,
) !u64 {
    if (query.join == null and canStreamQuery(query) and filesAreStreamable(files) and files.len > 0) {
        return copyStream(allocator, t, files, query, dest, fallback_schema, stats);
    }
    const batch = try execute(allocator, t, files, query, fallback_schema, stats, right_files);
    var w = try native.Writer.create(allocator, t.io, dest, batch);
    errdefer {
        w.file.close(t.io);
        allocator.free(w.types);
    }
    if (batch.len > 0) {
        const page: usize = 65536;
        var off: usize = 0;
        while (off < batch.len) {
            const n: u64 = @intCast(@min(page, batch.len - off));
            const piece = try takeRange(allocator, batch, off, n);
            try w.writePage(piece);
            off += @intCast(n);
        }
    }
    const n = batch.len;
    try w.close(allocator);
    return n;
}

fn copyStream(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    dest: []const u8,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
) !u64 {
    const wanted = try collectScanColumns(allocator, query);
    var offset_left: u64 = query.offset orelse 0;
    var limit_left: ?u64 = query.limit;
    var writer: ?native.Writer = null;
    var writer_closed = false;
    errdefer if (!writer_closed) {
        if (writer) |*w| {
            w.file.close(t.io);
            allocator.free(w.types);
        }
    };
    var n_rows: u64 = 0;
    var vec_aa = std.heap.ArenaAllocator.init(t.allocator);
    defer vec_aa.deinit();

    for (files) |file| {
        if (limit_left) |lim| {
            if (lim == 0) break;
        }
        if (query.where) |expr| {
            if (iceberg.canSkipFile(file, expr)) {
                if (stats) |s| s.files_pruned += 1;
                continue;
            }
        }
        if (stats) |s| s.files_opened += 1;
        n_rows += try copyFilePages(
            allocator,
            t,
            file,
            query,
            wanted,
            &offset_left,
            &limit_left,
            &writer,
            dest,
            &vec_aa,
        );
    }

    if (writer == null) {
        const schema = if (fallback_schema) |fields|
            try emptyFromSchema(allocator, fields)
        else
            Batch{ .columns = &.{}, .len = 0 };
        var w = try native.Writer.create(allocator, t.io, dest, schema);
        try w.close(allocator);
        return 0;
    }
    try writer.?.close(allocator);
    writer_closed = true;
    return n_rows;
}

fn copyFilePages(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    query: sql.Query,
    wanted: ?[]const []const u8,
    offset_left: *u64,
    limit_left: *?u64,
    writer: *?native.Writer,
    dest: []const u8,
    vec_aa: *std.heap.ArenaAllocator,
) !u64 {
    var n_rows: u64 = 0;
    switch (file.format) {
        .avro => return error.UnsupportedType,
        .glacier => {
            var reader = try native.Reader.openLocation(allocator, t, file.path);
            defer reader.close();
            while (try reader.nextPage(vec_aa.allocator(), wanted, query.where)) |page| {
                var vec = page;
                if (query.where) |expr| {
                    vec = try filterExpr(vec_aa.allocator(), vec, expr, null);
                }
                const wrote = try emitCopyPage(allocator, t, query, vec, offset_left, limit_left, writer, dest);
                n_rows += wrote;
                _ = vec_aa.reset(.retain_capacity);
                if (limit_left.*) |lim| {
                    if (lim == 0) break;
                }
            }
        },
        .parquet => {
            var reader = try parquet.openLocation(allocator, t, file.path);
            defer reader.close();
            const picked = try pickParquetColumns(allocator, reader, wanted);
            const batch_size = streamBatchSize(query.where != null, offset_left.*, limit_left.*);
            var br = try reader.openBatch(picked, batch_size);
            defer br.close();
            while (try br.next()) |rb_val| {
                var rb = rb_val;
                defer rb.deinit();
                if (rb.numRows() <= 0) continue;
                var vec = try batchFromRowBatch(vec_aa.allocator(), reader, rb, picked);
                if (query.where) |expr| {
                    vec = try filterExpr(vec_aa.allocator(), vec, expr, null);
                }
                const wrote = try emitCopyPage(allocator, t, query, vec, offset_left, limit_left, writer, dest);
                n_rows += wrote;
                _ = vec_aa.reset(.retain_capacity);
                if (limit_left.*) |lim| {
                    if (lim == 0) break;
                }
            }
        },
    }
    return n_rows;
}

fn emitCopyPage(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    query: sql.Query,
    vec: Batch,
    offset_left: *u64,
    limit_left: *?u64,
    writer: *?native.Writer,
    dest: []const u8,
) !u64 {
    var out = vec;
    if (out.len == 0) return 0;
    if (offset_left.* > 0) {
        if (offset_left.* >= out.len) {
            offset_left.* -= out.len;
            return 0;
        }
        out = try takeRange(allocator, out, offset_left.*, null);
        offset_left.* = 0;
    }
    if (limit_left.*) |*lim| {
        if (lim.* == 0) return 0;
        if (out.len > lim.*) {
            out = try takeRange(allocator, out, 0, lim.*);
            lim.* = 0;
        } else {
            lim.* -= out.len;
        }
    }
    if (!query.isStar()) out = try projectItems(allocator, out, query);
    if (out.len == 0) return 0;
    if (writer.* == null) {
        writer.* = try native.Writer.create(allocator, t.io, dest, out);
    }
    try writer.*.?.writePage(out);
    return out.len;
}

fn projectItems(allocator: std.mem.Allocator, input: Batch, query: sql.Query) !Batch {
    const columns = try allocator.alloc(Column, query.items.len);
    for (query.items, 0..) |item, i| {
        columns[i] = switch (item) {
            .star => return error.InvalidSyntax,
            .agg => return error.InvalidSyntax,
            .window => |w| blk: {
                const name = try windowName(allocator, w);
                const idx = try input.lookup(name);
                var col = input.columns[idx];
                if (w.alias) |alias| col.name = alias;
                break :blk col;
            },
            .column => |c| blk: {
                const key = if (c.qualifier) |q|
                    try std.fmt.allocPrint(allocator, "{s}.{s}", .{ q, c.name })
                else
                    c.name;
                const idx = try input.lookup(key);
                var col = input.columns[idx];
                if (c.alias) |alias| col.name = alias;
                break :blk col;
            },
            .scalar => |s| try evalScalar(allocator, input, s),
            .literal => |lit| try fillLiteralColumn(allocator, lit, input.len),
        };
    }
    return .{ .columns = columns, .len = input.len };
}

fn batchFromLiterals(allocator: std.mem.Allocator, query: sql.Query, len: usize) !Batch {
    const columns = try allocator.alloc(Column, query.items.len);
    for (query.items, 0..) |item, i| {
        columns[i] = try fillLiteralColumn(allocator, item.literal, len);
    }
    return .{ .columns = columns, .len = len };
}

fn fillLiteralColumn(allocator: std.mem.Allocator, lit: sql.LiteralItem, len: usize) !Column {
    const name = lit.name;
    switch (lit.value) {
        .int => |v| {
            const i64s = try allocator.alloc(i64, len);
            @memset(i64s, v);
            return .{ .name = name, .data_type = .int64, .len = len, .i64s = i64s };
        },
        .float => |v| {
            const f64s = try allocator.alloc(f64, len);
            @memset(f64s, v);
            return .{ .name = name, .data_type = .float64, .len = len, .f64s = f64s };
        },
        .string => |s| {
            const offsets = try allocator.alloc(u32, len + 1);
            const bytes = try allocator.alloc(u8, s.len * len);
            var off: u32 = 0;
            for (0..len) |i| {
                offsets[i] = off;
                if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
                off += @intCast(s.len);
            }
            offsets[len] = off;
            return .{
                .name = name,
                .data_type = .utf8,
                .len = len,
                .utf8 = .{ .offsets = offsets, .bytes = bytes },
            };
        },
    }
}

fn scanAvro(allocator: std.mem.Allocator, t: vfs.Transport, file: iceberg.DataFile) !Batch {
    var reader = if (file.bytes) |b|
        try avro.openMemory(allocator, b)
    else
        try avro.openLocation(allocator, t, file.path);
    defer reader.close();

    const n_rows: usize = @intCast(reader.numRows());
    const n_cols: usize = @intCast(reader.numColumns());
    const columns = try allocator.alloc(Column, n_cols);

    var i: i32 = 0;
    while (i < reader.numColumns()) : (i += 1) {
        const meta = reader.column(i) orelse return error.AvroOpenFailed;
        const name = try allocator.dupe(u8, meta.name);
        columns[@intCast(i)] = try readAvroColumn(allocator, reader, i, meta.physical_type, name, n_rows);
    }
    return .{ .columns = columns, .len = n_rows };
}

fn readAvroColumn(
    allocator: std.mem.Allocator,
    reader: avro.Reader,
    index: i32,
    physical: avro.PhysicalType,
    name: []const u8,
    n_rows: usize,
) !Column {
    var col: Column = .{
        .name = name,
        .data_type = switch (physical) {
            .boolean => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .double => .float64,
            .byte_array => .utf8,
            else => return error.UnsupportedType,
        },
        .len = n_rows,
    };
    switch (physical) {
        .boolean => {
            col.bools = try allocator.alloc(u8, n_rows);
            const got = reader.readAllValues(index, col.bools);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.AvroOpenFailed;
        },
        .int32 => {
            col.i32s = try allocator.alloc(i32, n_rows);
            const got = reader.readAllValues(index, col.i32s);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.AvroOpenFailed;
        },
        .int64 => {
            col.i64s = try allocator.alloc(i64, n_rows);
            const got = reader.readAllValues(index, col.i64s);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.AvroOpenFailed;
        },
        .double => {
            col.f64s = try allocator.alloc(f64, n_rows);
            const got = reader.readAllValues(index, col.f64s);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.AvroOpenFailed;
        },
        .byte_array => {
            const bas = try allocator.alloc(avro.ByteArray, n_rows);
            defer allocator.free(bas);
            const got = reader.readAllValues(index, bas);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.AvroOpenFailed;
            var nbytes: usize = 0;
            for (bas) |ba| nbytes += @intCast(ba.length);
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, n_rows + 1);
            var off: u32 = 0;
            for (bas, 0..) |ba, row| {
                offsets[row] = off;
                const n: usize = @intCast(ba.length);
                if (n > 0) @memcpy(bytes[off..][0..n], ba.data[0..n]);
                off += @intCast(n);
            }
            offsets[n_rows] = off;
            col.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
        else => return error.UnsupportedType,
    }
    return col;
}

fn canStreamQuery(query: sql.Query) bool {
    return query.join == null and !query.hasWindow() and query.order_by.len == 0 and !query.distinct and !query.needsAgg();
}

fn filesAreStreamable(files: []const iceberg.DataFile) bool {
    for (files) |file| {
        if (file.format != .parquet and file.format != .glacier) return false;
        if (file.bytes != null) return false;
    }
    return true;
}

fn cappedVecSize(cap: usize) i32 {
    const per_row: usize = 64;
    const n = @max(cap / per_row, 1);
    return @intCast(@min(n, 65536));
}

fn executeCapped(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
) !Batch {
    const wanted = try collectScanColumns(allocator, query);
    const cap = spill.memoryCap();
    const vec_size = cappedVecSize(cap);

    if (query.needsAgg() and query.group_by.len == 0) {
        return executeGlobalAgg(allocator, t, files, query, fallback_schema, stats, wanted, vec_size);
    }

    var tmp = try spill.Dir.init(t.allocator, t.io);
    defer tmp.deinit();
    var vec_aa = std.heap.ArenaAllocator.init(t.allocator);
    defer vec_aa.deinit();
    var work_aa = std.heap.ArenaAllocator.init(t.allocator);
    defer work_aa.deinit();

    const mode: CapMode = if (query.needsAgg())
        .hash_agg
    else if (query.order_by.len > 0)
        .order
    else
        .hash_distinct;

    var ctx = CapCtx{
        .allocator = allocator,
        .io = t.io,
        .cap = cap,
        .query = query,
        .tmp = &tmp,
        .vec_aa = &vec_aa,
        .work_aa = &work_aa,
        .mode = mode,
        .run_paths = .empty,
        .part_paths = .{null} ** spill.partition_count,
        .part_writers = .{null} ** spill.partition_count,
    };

    for (files) |file| {
        if (query.where) |expr| {
            if (iceberg.canSkipFile(file, expr)) {
                if (stats) |s| s.files_pruned += 1;
                continue;
            }
        }
        if (stats) |s| s.files_opened += 1;
        try ctx.ingestFile(t, file, wanted, vec_size);
    }

    try ctx.closeParts();

    if (!ctx.spilled) {
        const scanned: Batch = if (ctx.grow != null) blk: {
            const frozen = try ctx.grow.?.freeze();
            ctx.grow = null;
            break :blk try cloneBatch(allocator, frozen);
        } else if (ctx.schema_batch) |s|
            s
        else if (fallback_schema) |fields|
            try emptyFromSchema(allocator, fields)
        else
            .{ .columns = &.{}, .len = 0 };
        return applyTail(allocator, scanned, query, .{ .where_done = true });
    }

    return ctx.finishSpilled();
}

const CapMode = enum { order, hash_agg, hash_distinct };

const CapCtx = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    cap: usize,
    query: sql.Query,
    tmp: *spill.Dir,
    vec_aa: *std.heap.ArenaAllocator,
    work_aa: *std.heap.ArenaAllocator,
    mode: CapMode,
    grow: ?spill.Grow = null,
    work_bytes: usize = 0,
    spilled: bool = false,
    schema_batch: ?Batch = null,
    metas: []const spill.ColMeta = &.{},
    part_idxs: []const usize = &.{},
    run_paths: std.ArrayList([]const u8),
    part_paths: [spill.partition_count]?[]const u8,
    part_writers: [spill.partition_count]?spill.RunWriter,

    fn ingestFile(
        self: *CapCtx,
        t: vfs.Transport,
        file: iceberg.DataFile,
        wanted: ?[]const []const u8,
        vec_size: i32,
    ) !void {
        switch (file.format) {
            .parquet => try self.ingestParquet(t, file, wanted, vec_size),
            .glacier => try self.ingestNative(t, file, wanted),
            .avro => return error.UnsupportedType,
        }
    }

    fn ingestParquet(
        self: *CapCtx,
        t: vfs.Transport,
        file: iceberg.DataFile,
        wanted: ?[]const []const u8,
        vec_size: i32,
    ) !void {
        var reader = try parquet.openLocation(self.allocator, t, file.path);
        defer reader.close();
        const picked = try pickParquetColumns(self.allocator, reader, wanted);
        if (picked.len == 0) return error.ColumnNotFound;
        var br = try reader.openBatch(picked, vec_size);
        defer br.close();
        while (try br.next()) |rb_val| {
            var rb = rb_val;
            defer rb.deinit();
            if (rb.numRows() <= 0) continue;
            const va = self.vec_aa.allocator();
            var vec = try batchFromRowBatch(va, reader, rb, picked);
            if (self.query.where) |expr| {
                vec = try filterExpr(va, vec, expr, null);
            }
            if (vec.len == 0) {
                _ = self.vec_aa.reset(.retain_capacity);
                continue;
            }
            try self.ingestVec(vec);
        }
    }

    fn ingestNative(
        self: *CapCtx,
        t: vfs.Transport,
        file: iceberg.DataFile,
        wanted: ?[]const []const u8,
    ) !void {
        var reader = try native.Reader.openLocation(self.allocator, t, file.path);
        defer reader.close();
        while (try reader.nextPage(self.vec_aa.allocator(), wanted, self.query.where)) |page| {
            var vec = page;
            if (vec.len == 0) {
                _ = self.vec_aa.reset(.retain_capacity);
                continue;
            }
            if (self.query.where) |expr| {
                vec = try filterExpr(self.vec_aa.allocator(), vec, expr, null);
            }
            if (vec.len == 0) {
                _ = self.vec_aa.reset(.retain_capacity);
                continue;
            }
            try self.ingestVec(vec);
        }
    }

    fn ingestVec(self: *CapCtx, vec: Batch) !void {
        if (self.schema_batch == null) {
            self.schema_batch = try emptyFromBatch(self.allocator, vec);
            self.metas = try colMetas(self.allocator, vec);
            self.part_idxs = try self.resolveIdxs(vec);
        }
        if (self.spilled and self.mode != .order) {
            try self.writeBatchToParts(vec);
            _ = self.vec_aa.reset(.retain_capacity);
            return;
        }

        if (self.grow == null) {
            self.grow = try spill.Grow.init(self.work_aa.allocator(), self.metas);
        }
        var row: usize = 0;
        while (row < vec.len) : (row += 1) {
            try self.grow.?.appendBatch(vec, row);
        }
        self.work_bytes += spill.batchBytes(vec);
        _ = self.vec_aa.reset(.retain_capacity);

        if (self.work_bytes > self.cap) {
            try self.spillWork();
        }
    }

    fn resolveIdxs(self: *CapCtx, vec: Batch) ![]const usize {
        switch (self.mode) {
            .order => return &.{},
            .hash_agg => {
                const idxs = try self.allocator.alloc(usize, self.query.group_by.len);
                for (self.query.group_by, 0..) |name, i| {
                    idxs[i] = vec.columnIndex(name) orelse return error.ColumnNotFound;
                }
                return idxs;
            },
            .hash_distinct => {
                const idxs = try self.allocator.alloc(usize, vec.columns.len);
                for (idxs, 0..) |*slot, i| slot.* = i;
                return idxs;
            },
        }
    }

    fn spillWork(self: *CapCtx) !void {
        if (self.grow == null) return;
        if (self.grow.?.len == 0) {
            self.grow = null;
            return;
        }
        const batch = try self.grow.?.freeze();
        self.grow = null;
        self.spilled = true;
        switch (self.mode) {
            .order => {
                const sorted = try sortBatch(self.work_aa.allocator(), batch, self.query.order_by);
                const path = try self.tmp.nextPath(self.allocator);
                try spill.writeBatch(self.io, path, sorted);
                try self.run_paths.append(self.allocator, path);
            },
            .hash_agg, .hash_distinct => try self.writeBatchToParts(batch),
        }
        self.work_bytes = 0;
        _ = self.work_aa.reset(.retain_capacity);
    }

    fn writeBatchToParts(self: *CapCtx, batch: Batch) !void {
        var row: usize = 0;
        while (row < batch.len) : (row += 1) {
            const key = try encodeKey(self.vec_aa.allocator(), batch, row, self.part_idxs);
            const p: usize = @intCast(std.hash.Wyhash.hash(0, key) % @as(u64, spill.partition_count));
            try self.ensurePart(p);
            try self.part_writers[p].?.appendRow(batch, row);
        }
        self.spilled = true;
    }

    fn ensurePart(self: *CapCtx, p: usize) !void {
        if (self.part_writers[p] != null) return;
        const path = try self.tmp.nextPath(self.allocator);
        self.part_paths[p] = path;
        self.part_writers[p] = try spill.RunWriter.create(self.io, path, self.schema_batch.?);
    }

    fn closeParts(self: *CapCtx) !void {
        for (&self.part_writers) |*slot| {
            if (slot.*) |*w| {
                try w.close();
                slot.* = null;
            }
        }
    }

    fn finishSpilled(self: *CapCtx) !Batch {
        switch (self.mode) {
            .order => {
                if (self.grow != null) try self.spillWork();
                const push = !self.query.distinct and !self.query.needsAgg();
                const merged = try mergeRuns(
                    self.allocator,
                    self.io,
                    self.run_paths.items,
                    self.query.order_by,
                    if (push) self.query.offset orelse 0 else 0,
                    if (push) self.query.limit else null,
                    self.schema_batch,
                );
                return applyTail(self.allocator, merged, self.query, .{
                    .where_done = true,
                    .sort_done = true,
                    .range_done = push,
                });
            },
            .hash_agg => {
                var parts: std.ArrayList(Batch) = .empty;
                for (self.part_paths) |opt| {
                    const path = opt orelse continue;
                    var cur = try spill.Cursor.open(self.allocator, self.io, path);
                    defer cur.close();
                    const grouped = try aggregateFromCursor(self.allocator, &cur, self.query);
                    if (grouped.len > 0) try parts.append(self.allocator, grouped);
                }
                var grouped: Batch = if (parts.items.len == 0)
                    self.schema_batch orelse .{ .columns = &.{}, .len = 0 }
                else if (parts.items.len == 1)
                    parts.items[0]
                else
                    try concatBatches(self.allocator, parts.items);
                if (self.query.having) |expr| {
                    grouped = try filterExpr(self.allocator, grouped, expr, self.query);
                }
                return applyTail(self.allocator, grouped, self.query, .{
                    .where_done = true,
                    .agg_done = true,
                });
            },
            .hash_distinct => {
                var parts: std.ArrayList(Batch) = .empty;
                for (self.part_paths) |opt| {
                    const path = opt orelse continue;
                    var cur = try spill.Cursor.open(self.allocator, self.io, path);
                    defer cur.close();
                    const uniq = try distinctFromCursor(self.allocator, &cur);
                    if (uniq.len > 0) try parts.append(self.allocator, uniq);
                }
                const uniq: Batch = if (parts.items.len == 0)
                    self.schema_batch orelse .{ .columns = &.{}, .len = 0 }
                else if (parts.items.len == 1)
                    parts.items[0]
                else
                    try concatBatches(self.allocator, parts.items);
                return applyTail(self.allocator, uniq, self.query, .{
                    .where_done = true,
                    .distinct_done = true,
                });
            },
        }
    }
};

fn executeGlobalAgg(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
    wanted: ?[]const []const u8,
    vec_size: i32,
) !Batch {
    _ = fallback_schema;
    try validateAgg(query);
    var vec_aa = std.heap.ArenaAllocator.init(t.allocator);
    defer vec_aa.deinit();
    const states = try makeStates(allocator, query);
    var sample: ?Batch = null;
    var any = false;

    for (files) |file| {
        if (query.where) |expr| {
            if (iceberg.canSkipFile(file, expr)) {
                if (stats) |s| s.files_pruned += 1;
                continue;
            }
        }
        if (stats) |s| s.files_opened += 1;
        switch (file.format) {
            .avro => return error.UnsupportedType,
            .glacier => {
                var reader = try native.Reader.openLocation(allocator, t, file.path);
                defer reader.close();
                if (wanted != null and wanted.?.len == 0) {
                    const n = reader.numRows();
                    for (states) |*s| {
                        if (s.kind != .count) return error.InvalidSyntax;
                        s.count += n;
                        if (n > 0) s.inited = true;
                    }
                    any = any or n > 0;
                    continue;
                }
                while (try reader.nextPage(vec_aa.allocator(), wanted, query.where)) |page| {
                    var vec = page;
                    if (query.where) |expr| {
                        vec = try filterExpr(vec_aa.allocator(), vec, expr, null);
                    }
                    if (vec.len == 0) {
                        _ = vec_aa.reset(.retain_capacity);
                        continue;
                    }
                    if (sample == null) sample = try emptyFromBatch(allocator, vec);
                    var row: usize = 0;
                    while (row < vec.len) : (row += 1) {
                        try feedRow(states, vec, query, row, allocator);
                        any = true;
                    }
                    _ = vec_aa.reset(.retain_capacity);
                }
            },
            .parquet => {
                var reader = try parquet.openLocation(allocator, t, file.path);
                defer reader.close();
                const picked = try pickParquetColumns(allocator, reader, wanted);
                if (picked.len == 0) {
                    const n: u64 = @intCast(reader.numRows());
                    for (states) |*s| {
                        if (s.kind != .count) return error.InvalidSyntax;
                        s.count += n;
                        if (n > 0) s.inited = true;
                    }
                    any = any or n > 0;
                    continue;
                }
                var br = try reader.openBatch(picked, vec_size);
                defer br.close();
                while (try br.next()) |rb_val| {
                    var rb = rb_val;
                    defer rb.deinit();
                    if (rb.numRows() <= 0) continue;
                    const va = vec_aa.allocator();
                    var vec = try batchFromRowBatch(va, reader, rb, picked);
                    if (query.where) |expr| {
                        vec = try filterExpr(va, vec, expr, null);
                    }
                    if (vec.len == 0) {
                        _ = vec_aa.reset(.retain_capacity);
                        continue;
                    }
                    if (sample == null) sample = try emptyFromBatch(allocator, vec);
                    var row: usize = 0;
                    while (row < vec.len) : (row += 1) {
                        try feedRow(states, vec, query, row, allocator);
                        any = true;
                    }
                    _ = vec_aa.reset(.retain_capacity);
                }
            },
        }
    }

    const input = sample orelse Batch{ .columns = &.{}, .len = 0 };
    const n_groups: usize = 1;
    const out_cols = try allocator.alloc(Column, query.items.len);
    var agg_i: usize = 0;
    for (query.items, 0..) |item, col_i| {
        const agg = item.agg;
        const src = try aggColumn(input, agg);
        const name = try aggName(allocator, agg);
        const template = if (any) states[agg_i] else AggState{ .kind = agg.kind };
        var col = try finalizeAgg(allocator, template, src, name, n_groups);
        try writeAggCell(&col, if (any) states[agg_i] else AggState{ .kind = agg.kind }, 0, allocator);
        out_cols[col_i] = col;
        agg_i += 1;
    }
    var batch: Batch = .{ .columns = out_cols, .len = 1 };
    if (query.having) |expr| {
        batch = try filterExpr(allocator, batch, expr, query);
    }
    return applyTail(allocator, batch, query, .{
        .where_done = true,
        .agg_done = true,
    });
}

fn colMetas(allocator: std.mem.Allocator, b: Batch) ![]spill.ColMeta {
    const metas = try allocator.alloc(spill.ColMeta, b.columns.len);
    for (b.columns, metas) |col, *m| {
        m.* = .{
            .name = try allocator.dupe(u8, col.name),
            .data_type = col.data_type,
            .decimal_precision = col.decimal_precision,
            .decimal_scale = col.decimal_scale,
        };
    }
    return metas;
}

fn emptyFromBatch(allocator: std.mem.Allocator, b: Batch) !Batch {
    const columns = try allocator.alloc(Column, b.columns.len);
    for (b.columns, 0..) |src, i| {
        columns[i] = .{
            .name = try allocator.dupe(u8, src.name),
            .data_type = src.data_type,
            .len = 0,
            .decimal_precision = src.decimal_precision,
            .decimal_scale = src.decimal_scale,
        };
        if (src.data_type == .utf8) {
            columns[i].utf8.offsets = try allocator.alloc(u32, 1);
            columns[i].utf8.offsets[0] = 0;
        }
    }
    return .{ .columns = columns, .len = 0 };
}

fn cloneBatch(allocator: std.mem.Allocator, src: Batch) !Batch {
    if (src.len == 0) return emptyFromBatch(allocator, src);
    const idx = try allocator.alloc(usize, src.len);
    for (idx, 0..) |*slot, i| slot.* = i;
    const out = try gatherRows(allocator, src, idx);
    for (out.columns) |*c| c.name = try allocator.dupe(u8, c.name);
    return out;
}

fn batchFromMeta(allocator: std.mem.Allocator, cols: []const spill.ColMeta) !Batch {
    const columns = try allocator.alloc(Column, cols.len);
    for (cols, 0..) |m, i| {
        var col: Column = .{
            .name = m.name,
            .data_type = m.data_type,
            .len = 1,
            .decimal_precision = m.decimal_precision,
            .decimal_scale = m.decimal_scale,
        };
        switch (m.data_type) {
            .boolean => col.bools = try allocator.alloc(u8, 1),
            .int32 => col.i32s = try allocator.alloc(i32, 1),
            .int64, .timestamp, .timestamptz => col.i64s = try allocator.alloc(i64, 1),
            .float32 => col.f32s = try allocator.alloc(f32, 1),
            .float64 => col.f64s = try allocator.alloc(f64, 1),
            .utf8 => {
                col.utf8.offsets = try allocator.alloc(u32, 2);
                col.utf8.offsets[0] = 0;
                col.utf8.offsets[1] = 0;
                col.utf8.bytes = &.{};
            },
            .uuid => col.uuids = try allocator.alloc([16]u8, 1),
            .decimal128 => col.i128s = try allocator.alloc(i128, 1),
        }
        columns[i] = col;
    }
    return .{ .columns = columns, .len = 1 };
}

fn fillFromCursor(allocator: std.mem.Allocator, batch: *Batch, cur: *const spill.Cursor) !void {
    batch.len = 1;
    for (batch.columns, cur.cols, cur.cells) |*col, meta, cell| {
        col.len = 1;
        switch (meta.data_type) {
            .boolean => col.bools[0] = cell.boolean,
            .int32 => col.i32s[0] = cell.i32,
            .int64, .timestamp, .timestamptz => col.i64s[0] = cell.i64,
            .float32 => col.f32s[0] = cell.f32,
            .float64 => col.f64s[0] = cell.f64,
            .utf8 => {
                col.utf8.offsets[0] = 0;
                col.utf8.offsets[1] = @intCast(cell.str.len);
                if (col.utf8.bytes.len == 0) {
                    col.utf8.bytes = try allocator.dupe(u8, cell.str);
                } else {
                    col.utf8.bytes = try allocator.realloc(col.utf8.bytes, cell.str.len);
                    if (cell.str.len > 0) @memcpy(col.utf8.bytes, cell.str);
                }
            },
            .uuid => col.uuids[0] = cell.uuid,
            .decimal128 => col.i128s[0] = cell.i128,
        }
    }
}

fn aggregateFromCursor(allocator: std.mem.Allocator, cur: *spill.Cursor, query: sql.Query) !Batch {
    try validateAgg(query);
    var rowb = try batchFromMeta(allocator, cur.cols);
    var grow = try spill.Grow.init(allocator, cur.cols);
    var groups: std.StringArrayHashMapUnmanaged(Group) = .empty;
    const group_idxs = try allocator.alloc(usize, query.group_by.len);
    for (query.group_by, 0..) |name, i| {
        group_idxs[i] = cur.columnIndex(name) orelse return error.ColumnNotFound;
    }

    while (try cur.next()) {
        try fillFromCursor(allocator, &rowb, cur);
        const encoded = try encodeKey(allocator, rowb, 0, group_idxs);
        if (groups.getPtr(encoded)) |g| {
            try feedRow(g.states, rowb, query, 0, allocator);
            allocator.free(encoded);
        } else {
            const states = try makeStates(allocator, query);
            try feedRow(states, rowb, query, 0, allocator);
            try grow.appendCursor(cur);
            try groups.put(allocator, encoded, .{ .first_row = grow.len - 1, .states = states });
        }
    }

    const n_groups = groups.count();
    const snap = try grow.freeze();
    const first_rows = try allocator.alloc(usize, n_groups);
    for (groups.values(), 0..) |g, i| first_rows[i] = g.first_row;

    const out_cols = try allocator.alloc(Column, query.items.len);
    var agg_i: usize = 0;
    for (query.items, 0..) |item, col_i| {
        switch (item) {
            .star, .scalar, .literal, .window => return error.UnsupportedSql,
            .column => |c| {
                const src_i = snap.columnIndex(c.name) orelse return error.ColumnNotFound;
                const name = c.alias orelse c.name;
                out_cols[col_i] = try copyKeyColumn(allocator, snap.columns[src_i], first_rows, name);
            },
            .agg => |agg| {
                const src = try aggColumn(snap, agg);
                const name = try aggName(allocator, agg);
                const template = if (n_groups == 0)
                    AggState{ .kind = agg.kind }
                else
                    groups.values()[0].states[agg_i];
                var col = try finalizeAgg(allocator, template, src, name, n_groups);
                if (n_groups > 0) {
                    for (groups.values(), 0..) |g, gi| {
                        try writeAggCell(&col, g.states[agg_i], gi, allocator);
                    }
                }
                out_cols[col_i] = col;
                agg_i += 1;
            },
        }
    }
    return .{ .columns = out_cols, .len = n_groups };
}

fn distinctFromCursor(allocator: std.mem.Allocator, cur: *spill.Cursor) !Batch {
    var grow = try spill.Grow.init(allocator, cur.cols);
    var rowb = try batchFromMeta(allocator, cur.cols);
    const col_idxs = try allocator.alloc(usize, cur.cols.len);
    for (col_idxs, 0..) |*slot, i| slot.* = i;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    while (try cur.next()) {
        try fillFromCursor(allocator, &rowb, cur);
        const key = try encodeKey(allocator, rowb, 0, col_idxs);
        const gop = try seen.getOrPut(allocator, key);
        if (gop.found_existing) {
            allocator.free(key);
        } else {
            try grow.appendCursor(cur);
        }
    }
    return grow.freeze();
}

fn mergeRuns(
    allocator: std.mem.Allocator,
    io: std.Io,
    paths: []const []const u8,
    keys: []const sql.OrderBy,
    offset: u64,
    limit: ?u64,
    schema: ?Batch,
) !Batch {
    if (paths.len == 0) {
        return schema orelse .{ .columns = &.{}, .len = 0 };
    }
    const cursors = try allocator.alloc(spill.Cursor, paths.len);
    const alive = try allocator.alloc(bool, paths.len);
    var opened: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < opened) : (i += 1) cursors[i].close();
    }
    for (paths, 0..) |path, i| {
        cursors[i] = try spill.Cursor.open(allocator, io, path);
        opened += 1;
        alive[i] = try cursors[i].next();
    }

    var grow = try spill.Grow.init(allocator, cursors[0].cols);
    var skipped: u64 = 0;
    var emitted: u64 = 0;
    while (true) {
        var best: ?usize = null;
        for (cursors, 0..) |*c, i| {
            if (!alive[i]) continue;
            if (best == null or cursorLess(c, &cursors[best.?], keys)) best = i;
        }
        if (best == null) break;
        if (skipped < offset) {
            skipped += 1;
        } else {
            if (limit) |lim| {
                if (emitted >= lim) break;
            }
            try grow.appendCursor(&cursors[best.?]);
            emitted += 1;
        }
        alive[best.?] = try cursors[best.?].next();
    }
    for (cursors) |*c| c.close();
    if (grow.len == 0) {
        return schema orelse try grow.freeze();
    }
    return grow.freeze();
}

fn cursorLess(a: *const spill.Cursor, b: *const spill.Cursor, keys: []const sql.OrderBy) bool {
    for (keys) |ob| {
        const ai = a.columnIndex(ob.column) orelse return false;
        const bi = b.columnIndex(ob.column) orelse return false;
        const ord = compareCursorCells(a, ai, b, bi);
        if (ord == .eq) continue;
        if (ob.desc) return ord == .gt;
        return ord == .lt;
    }
    return false;
}

fn compareCursorCells(a: *const spill.Cursor, ai: usize, b: *const spill.Cursor, bi: usize) std.math.Order {
    const ac = a.cells[ai];
    const bc = b.cells[bi];
    return switch (a.cols[ai].data_type) {
        .boolean => std.math.order(ac.boolean, bc.boolean),
        .int32 => std.math.order(ac.i32, bc.i32),
        .int64, .timestamp, .timestamptz => std.math.order(ac.i64, bc.i64),
        .float32 => std.math.order(ac.f32, bc.f32),
        .float64 => std.math.order(ac.f64, bc.f64),
        .utf8 => std.mem.order(u8, ac.str, bc.str),
        .uuid => std.mem.order(u8, &ac.uuid, &bc.uuid),
        .decimal128 => std.math.order(ac.i128, bc.i128),
    };
}

fn streamBatchSize(has_where: bool, offset_left: u64, limit_left: ?u64) i32 {
    if (has_where) return 65536;
    const cap: u64 = 65536;
    const need = offset_left + (limit_left orelse cap);
    const n = @max(@min(need, cap), 1);
    return @intCast(n);
}

fn executeStream(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
) !Batch {
    const wanted = try collectScanColumns(allocator, query);
    var offset_left: u64 = query.offset orelse 0;
    var limit_left: ?u64 = query.limit;
    var parts: std.ArrayList(Batch) = .empty;
    var empty_schema: ?Batch = null;

    for (files) |file| {
        if (limit_left) |lim| {
            if (lim == 0) break;
        }
        if (query.where) |expr| {
            if (iceberg.canSkipFile(file, expr)) {
                if (stats) |s| s.files_pruned += 1;
                continue;
            }
        }
        if (stats) |s| s.files_opened += 1;
        const piece = switch (file.format) {
            .parquet => try scanParquetStream(allocator, t, file, wanted, query.where, &offset_left, &limit_left),
            .glacier => try scanNativeStream(allocator, t, file, wanted, query.where, &offset_left, &limit_left),
            .avro => return error.UnsupportedType,
        };
        if (piece.len > 0) {
            try parts.append(allocator, piece);
        } else if (empty_schema == null and piece.columns.len > 0) {
            empty_schema = piece;
        }
    }

    var scanned: Batch = if (parts.items.len == 0) blk: {
        if (empty_schema) |s| break :blk s;
        if (fallback_schema) |fields| break :blk try emptyFromSchema(allocator, fields);
        break :blk .{ .columns = &.{}, .len = 0 };
    } else if (parts.items.len == 1)
        parts.items[0]
    else
        try concatBatches(allocator, parts.items);

    if (!query.isStar()) {
        scanned = try projectItems(allocator, scanned, query);
    }
    return scanned;
}

fn scanParquetStream(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    offset_left: *u64,
    limit_left: *?u64,
) !Batch {
    var reader = try parquet.openLocation(allocator, t, file.path);
    defer reader.close();
    const picked = try pickParquetColumns(allocator, reader, wanted);

    if (picked.len == 0) {
        var n: u64 = @intCast(reader.numRows());
        if (where != null) return error.ColumnNotFound;
        if (offset_left.* >= n) {
            offset_left.* -= n;
            return .{ .columns = &.{}, .len = 0 };
        }
        n -= offset_left.*;
        offset_left.* = 0;
        if (limit_left.*) |*lim| {
            if (n > lim.*) n = lim.*;
            lim.* -= n;
        }
        return .{ .columns = &.{}, .len = @intCast(n) };
    }

    if (where == null) {
        const nfile: u64 = @intCast(reader.numRows());
        if (offset_left.* >= nfile) {
            offset_left.* -= nfile;
            return try emptyFromPicked(allocator, reader, picked);
        }
    }

    const batch_size = streamBatchSize(where != null, offset_left.*, limit_left.*);
    var br = try reader.openBatch(picked, batch_size);
    defer br.close();

    var parts: std.ArrayList(Batch) = .empty;
    while (try br.next()) |rb_val| {
        var rb = rb_val;
        defer rb.deinit();
        const nrows64 = rb.numRows();
        if (nrows64 <= 0) continue;
        const nrows: u64 = @intCast(nrows64);

        if (where == null and offset_left.* >= nrows) {
            offset_left.* -= nrows;
            continue;
        }

        var vec = try batchFromRowBatch(allocator, reader, rb, picked);
        if (where) |expr| {
            vec = try filterExpr(allocator, vec, expr, null);
        }
        if (offset_left.* > 0) {
            if (offset_left.* >= vec.len) {
                offset_left.* -= vec.len;
                continue;
            }
            vec = try takeRange(allocator, vec, offset_left.*, null);
            offset_left.* = 0;
        }
        if (limit_left.*) |*lim| {
            if (lim.* == 0) break;
            if (vec.len > lim.*) {
                vec = try takeRange(allocator, vec, 0, lim.*);
                lim.* = 0;
            } else {
                lim.* -= vec.len;
            }
        }
        if (vec.len > 0) try parts.append(allocator, vec);
        if (limit_left.*) |lim| {
            if (lim == 0) break;
        }
    }

    if (parts.items.len == 0) return try emptyFromPicked(allocator, reader, picked);
    if (parts.items.len == 1) return parts.items[0];
    return concatBatches(allocator, parts.items);
}

fn scanNativeStream(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    offset_left: *u64,
    limit_left: *?u64,
) !Batch {
    var reader = try native.Reader.openLocation(allocator, t, file.path);
    defer reader.close();

    if (wanted != null and wanted.?.len == 0) {
        var n: u64 = reader.numRows();
        if (where != null) return error.ColumnNotFound;
        if (offset_left.* >= n) {
            offset_left.* -= n;
            return .{ .columns = &.{}, .len = 0 };
        }
        n -= offset_left.*;
        offset_left.* = 0;
        if (limit_left.*) |*lim| {
            if (n > lim.*) n = lim.*;
            lim.* -= n;
        }
        return .{ .columns = &.{}, .len = @intCast(n) };
    }

    var parts: std.ArrayList(Batch) = .empty;
    while (try reader.nextPage(allocator, wanted, where)) |page| {
        var vec = page;
        if (where) |expr| {
            vec = try filterExpr(allocator, vec, expr, null);
        }
        if (offset_left.* > 0) {
            if (offset_left.* >= vec.len) {
                offset_left.* -= vec.len;
                continue;
            }
            vec = try takeRange(allocator, vec, offset_left.*, null);
            offset_left.* = 0;
        }
        if (limit_left.*) |*lim| {
            if (lim.* == 0) break;
            if (vec.len > lim.*) {
                vec = try takeRange(allocator, vec, 0, lim.*);
                lim.* = 0;
            } else {
                lim.* -= vec.len;
            }
        }
        if (vec.len > 0) try parts.append(allocator, vec);
        if (limit_left.*) |lim| {
            if (lim == 0) break;
        }
    }
    if (parts.items.len == 0) return try native.emptySchema(allocator, reader.cols);
    if (parts.items.len == 1) return parts.items[0];
    return concatBatches(allocator, parts.items);
}

fn scanNativeAll(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
) !Batch {
    var offset: u64 = 0;
    var limit: ?u64 = null;
    return scanNativeStream(allocator, t, file, wanted, where, &offset, &limit);
}

fn batchFromRowBatch(
    allocator: std.mem.Allocator,
    reader: parquet.Reader,
    rb: parquet.RowBatch,
    picked: []const i32,
) !Batch {
    const n_rows: usize = @intCast(rb.numRows());
    const columns = try allocator.alloc(Column, picked.len);
    for (picked, 0..) |file_col, out_i| {
        const meta = reader.column(file_col) orelse return error.ParquetOpenFailed;
        const name = try allocator.dupe(u8, meta.name);
        const vals = try rb.columnValues(@intCast(out_i));
        if (n_rows > 0 and vals.n < @as(i64, @intCast(n_rows))) return error.ParquetOpenFailed;
        var col = try fillColumnFromPtr(allocator, reader, file_col, name, n_rows, vals.ptr);
        col.valid = try copyArrowValid(allocator, vals.nulls, n_rows);
        columns[out_i] = col;
    }
    return .{ .columns = columns, .len = n_rows };
}

fn emptyFromPicked(allocator: std.mem.Allocator, reader: parquet.Reader, picked: []const i32) !Batch {
    const columns = try allocator.alloc(Column, picked.len);
    for (picked, 0..) |file_col, out_i| {
        const meta = reader.column(file_col) orelse return error.ParquetOpenFailed;
        const name = try allocator.dupe(u8, meta.name);
        columns[out_i] = try fillColumnFromPtr(allocator, reader, file_col, name, 0, undefined);
    }
    return .{ .columns = columns, .len = 0 };
}

fn pickParquetColumns(
    allocator: std.mem.Allocator,
    reader: parquet.Reader,
    wanted: ?[]const []const u8,
) ![]i32 {
    const n_file_cols: i32 = reader.numColumns();
    var i: i32 = 0;
    while (i < n_file_cols) : (i += 1) {
        if (reader.columnRepLevel(i) > 0) return error.UnsupportedNested;
    }

    var picked: std.ArrayList(i32) = .empty;
    i = 0;
    while (i < n_file_cols) : (i += 1) {
        const meta = reader.column(i) orelse return error.ParquetOpenFailed;
        if (wanted) |names| {
            var keep = false;
            for (names) |n| {
                if (std.ascii.eqlIgnoreCase(n, meta.name)) {
                    keep = true;
                    break;
                }
            }
            if (!keep) continue;
        }
        try picked.append(allocator, i);
    }
    return picked.items;
}

fn collectScanColumns(allocator: std.mem.Allocator, query: sql.Query) !?[]const []const u8 {
    for (query.items) |item| {
        if (item == .star) return null;
    }
    var names: std.ArrayList([]const u8) = .empty;
    for (query.items) |item| {
        switch (item) {
            .star => {},
            .column => |c| try addScanName(&names, allocator, c.name),
            .agg => |a| if (a.arg) |arg| try addScanName(&names, allocator, arg),
            .window => |w| {
                if (w.arg) |arg| try addScanName(&names, allocator, arg);
                for (w.spec.partition_by) |p| try addScanName(&names, allocator, p);
                for (w.spec.order_by) |ob| try addScanName(&names, allocator, ob.column);
            },
            .scalar => |s| {
                try addScanName(&names, allocator, s.arg);
                if (s.coalesce_col) |c| try addScanName(&names, allocator, c);
            },
            .literal => {},
        }
    }
    for (query.group_by) |g| try addScanName(&names, allocator, g);
    for (query.order_by) |ob| try addScanName(&names, allocator, ob.column);
    if (query.where) |e| try addScanNamesFromBool(&names, allocator, e);
    if (query.having) |e| try addScanNamesFromBool(&names, allocator, e);
    return names.items;
}

fn addScanName(names: *std.ArrayList([]const u8), allocator: std.mem.Allocator, name: []const u8) !void {
    for (names.items) |n| {
        if (std.ascii.eqlIgnoreCase(n, name)) return;
    }
    try names.append(allocator, name);
}

fn addScanNamesFromBool(names: *std.ArrayList([]const u8), allocator: std.mem.Allocator, expr: *const sql.BoolExpr) !void {
    switch (expr.*) {
        .cmp => |c| switch (c.left) {
            .column => |n| try addScanName(names, allocator, n),
            .agg => |a| if (a.arg) |arg| try addScanName(names, allocator, arg),
        },
        .isnull => |p| switch (p.left) {
            .column => |n| try addScanName(names, allocator, n),
            .agg => |a| if (a.arg) |arg| try addScanName(names, allocator, arg),
        },
        .@"and", .@"or" => |b| {
            try addScanNamesFromBool(names, allocator, b.left);
            try addScanNamesFromBool(names, allocator, b.right);
        },
    }
}

fn scanParquet(allocator: std.mem.Allocator, t: vfs.Transport, file: iceberg.DataFile, wanted: ?[]const []const u8) !Batch {
    var reader = if (file.bytes) |b|
        try parquet.openMemory(allocator, b)
    else
        try parquet.openLocation(allocator, t, file.path);
    defer reader.close();

    const picked = try pickParquetColumns(allocator, reader, wanted);
    if (picked.len == 0) {
        return .{ .columns = &.{}, .len = @intCast(reader.numRows()) };
    }
    var br = try reader.openBatch(picked, 65536);
    defer br.close();
    var parts: std.ArrayList(Batch) = .empty;
    while (try br.next()) |rb_raw| {
        var rb = rb_raw;
        defer rb.deinit();
        try parts.append(allocator, try batchFromRowBatch(allocator, reader, rb, picked));
    }
    if (parts.items.len == 0) return emptyFromPicked(allocator, reader, picked);
    if (parts.items.len == 1) return parts.items[0];
    return concatBatches(allocator, parts.items);
}

fn readColumn(
    allocator: std.mem.Allocator,
    reader: parquet.Reader,
    index: i32,
    name: []const u8,
    n_rows: usize,
) !Column {
    const meta = reader.column(index) orelse return error.ParquetOpenFailed;
    const logical = parquet.columnLogical(reader, index);
    const lid: parquet.LogicalId = @enumFromInt(logical.id);

    if (meta.physical_type == .int96) return error.UnsupportedType;

    if (lid == .uuid) {
        if (meta.physical_type != .fixed_len_byte_array or logical.type_length != 16)
            return error.UnsupportedType;
        const col: Column = .{
            .name = name,
            .data_type = .uuid,
            .len = n_rows,
            .uuids = try allocator.alloc([16]u8, n_rows),
        };
        const got = reader.readAllValues(index, col.uuids);
        if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
        return col;
    }

    if (lid == .decimal) {
        if (logical.precision > 38 or logical.precision <= 0) return error.UnsupportedType;
        const col: Column = .{
            .name = name,
            .data_type = .decimal128,
            .len = n_rows,
            .i128s = try allocator.alloc(i128, n_rows),
            .decimal_precision = logical.precision,
            .decimal_scale = logical.scale,
        };
        switch (meta.physical_type) {
            .int32 => {
                const tmp = try allocator.alloc(i32, n_rows);
                defer allocator.free(tmp);
                const got = reader.readAllValues(index, tmp);
                if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
                for (tmp, 0..) |v, i| col.i128s[i] = v;
            },
            .int64 => {
                const tmp = try allocator.alloc(i64, n_rows);
                defer allocator.free(tmp);
                const got = reader.readAllValues(index, tmp);
                if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
                for (tmp, 0..) |v, i| col.i128s[i] = v;
            },
            .fixed_len_byte_array => {
                const w: usize = @intCast(logical.type_length);
                if (w == 0 or w > 16) return error.UnsupportedType;
                const raw = try allocator.alloc(u8, n_rows * w);
                defer allocator.free(raw);
                const got = reader.readAllValues(index, raw);
                if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
                var i: usize = 0;
                while (i < n_rows) : (i += 1) {
                    col.i128s[i] = decimalFromBe(raw[i * w ..][0..w]);
                }
            },
            .byte_array => {
                const bas = try allocator.alloc(parquet.ByteArray, n_rows);
                defer allocator.free(bas);
                var got: i64 = -1;
                defer if (got > 0) parquet.freeByteArrays(bas[0..@intCast(got)]);
                got = reader.readAllValues(index, bas);
                if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
                for (bas, 0..) |ba, i| {
                    const n: usize = @intCast(ba.length);
                    col.i128s[i] = decimalFromBe(if (n == 0) &.{} else ba.data[0..n]);
                }
            },
            else => return error.UnsupportedType,
        }
        return col;
    }

    if (lid == .timestamp and meta.physical_type == .int64) {
        const col: Column = .{
            .name = name,
            .data_type = if (logical.utc != 0) .timestamptz else .timestamp,
            .len = n_rows,
            .i64s = try allocator.alloc(i64, n_rows),
        };
        const got = reader.readAllValues(index, col.i64s);
        if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
        if (logical.time_unit != 1) {
            for (col.i64s) |*v| v.* = toMicros(v.*, logical.time_unit);
        }
        return col;
    }

    var col: Column = .{
        .name = name,
        .data_type = switch (meta.physical_type) {
            .boolean => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .float => .float32,
            .double => .float64,
            .byte_array => .utf8,
            else => return error.UnsupportedType,
        },
        .len = n_rows,
    };
    switch (meta.physical_type) {
        .boolean => {
            col.bools = try allocator.alloc(u8, n_rows);
            const got = reader.readAllValues(index, col.bools);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
        },
        .int32 => {
            col.i32s = try allocator.alloc(i32, n_rows);
            const got = reader.readAllValues(index, col.i32s);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
        },
        .int64 => {
            col.i64s = try allocator.alloc(i64, n_rows);
            const got = reader.readAllValues(index, col.i64s);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
        },
        .float => {
            col.f32s = try allocator.alloc(f32, n_rows);
            const got = reader.readAllValues(index, col.f32s);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
        },
        .double => {
            col.f64s = try allocator.alloc(f64, n_rows);
            const got = reader.readAllValues(index, col.f64s);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
        },
        .byte_array => {
            const bas = try allocator.alloc(parquet.ByteArray, n_rows);
            defer allocator.free(bas);
            var got: i64 = -1;
            defer if (got > 0) parquet.freeByteArrays(bas[0..@intCast(got)]);
            got = reader.readAllValues(index, bas);
            if (got < 0 or @as(usize, @intCast(got)) != n_rows) return error.ParquetOpenFailed;
            var nbytes: usize = 0;
            for (bas) |ba| nbytes += @intCast(ba.length);
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, n_rows + 1);
            var off: u32 = 0;
            for (bas, 0..) |ba, row| {
                offsets[row] = off;
                const n: usize = @intCast(ba.length);
                if (n > 0) @memcpy(bytes[off..][0..n], ba.data[0..n]);
                off += @intCast(n);
            }
            offsets[n_rows] = off;
            col.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
        else => return error.UnsupportedType,
    }
    return col;
}

fn copyUnaligned(comptime T: type, dest: []T, src: [*]const u8) void {
    @memcpy(std.mem.sliceAsBytes(dest), src[0 .. dest.len * @sizeOf(T)]);
}

fn loadUnaligned(comptime T: type, src: [*]const u8, i: usize) T {
    var v: T = undefined;
    @memcpy(std.mem.asBytes(&v), src[i * @sizeOf(T) ..][0..@sizeOf(T)]);
    return v;
}

fn fillColumnFromPtr(
    allocator: std.mem.Allocator,
    reader: parquet.Reader,
    index: i32,
    name: []const u8,
    n_rows: usize,
    src: [*]const u8,
) !Column {
    const meta = reader.column(index) orelse return error.ParquetOpenFailed;
    const logical = parquet.columnLogical(reader, index);
    const lid: parquet.LogicalId = @enumFromInt(logical.id);

    if (meta.physical_type == .int96) return error.UnsupportedType;

    if (lid == .uuid) {
        if (meta.physical_type != .fixed_len_byte_array or logical.type_length != 16)
            return error.UnsupportedType;
        const uuids = try allocator.alloc([16]u8, n_rows);
        if (n_rows > 0) @memcpy(std.mem.sliceAsBytes(uuids), src[0 .. n_rows * 16]);
        return .{ .name = name, .data_type = .uuid, .len = n_rows, .uuids = uuids };
    }

    if (lid == .decimal) {
        if (logical.precision > 38 or logical.precision <= 0) return error.UnsupportedType;
        const i128s = try allocator.alloc(i128, n_rows);
        if (n_rows > 0) {
            switch (meta.physical_type) {
                .int32 => {
                    var i: usize = 0;
                    while (i < n_rows) : (i += 1) i128s[i] = loadUnaligned(i32, src, i);
                },
                .int64 => {
                    var i: usize = 0;
                    while (i < n_rows) : (i += 1) i128s[i] = loadUnaligned(i64, src, i);
                },
                .fixed_len_byte_array => {
                    const w: usize = @intCast(logical.type_length);
                    if (w == 0 or w > 16) return error.UnsupportedType;
                    var i: usize = 0;
                    while (i < n_rows) : (i += 1) {
                        i128s[i] = decimalFromBe(src[i * w ..][0..w]);
                    }
                },
                .byte_array => {
                    var i: usize = 0;
                    while (i < n_rows) : (i += 1) {
                        const ba = loadUnaligned(parquet.ByteArray, src, i);
                        const n: usize = @intCast(ba.length);
                        i128s[i] = decimalFromBe(if (n == 0) &.{} else ba.data[0..n]);
                    }
                },
                else => return error.UnsupportedType,
            }
        }
        return .{
            .name = name,
            .data_type = .decimal128,
            .len = n_rows,
            .i128s = i128s,
            .decimal_precision = logical.precision,
            .decimal_scale = logical.scale,
        };
    }

    if (lid == .timestamp and meta.physical_type == .int64) {
        const i64s = try allocator.alloc(i64, n_rows);
        if (n_rows > 0) {
            copyUnaligned(i64, i64s, src);
            if (logical.time_unit != 1) {
                for (i64s) |*v| v.* = toMicros(v.*, logical.time_unit);
            }
        }
        return .{
            .name = name,
            .data_type = if (logical.utc != 0) .timestamptz else .timestamp,
            .len = n_rows,
            .i64s = i64s,
        };
    }

    var col: Column = .{
        .name = name,
        .data_type = switch (meta.physical_type) {
            .boolean => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .float => .float32,
            .double => .float64,
            .byte_array => .utf8,
            else => return error.UnsupportedType,
        },
        .len = n_rows,
    };
    if (n_rows == 0) {
        if (col.data_type == .utf8) {
            col.utf8.offsets = try allocator.alloc(u32, 1);
            col.utf8.offsets[0] = 0;
        }
        return col;
    }
    switch (meta.physical_type) {
        .boolean => {
            col.bools = try allocator.alloc(u8, n_rows);
            @memcpy(col.bools, src[0..n_rows]);
        },
        .int32 => {
            col.i32s = try allocator.alloc(i32, n_rows);
            copyUnaligned(i32, col.i32s, src);
        },
        .int64 => {
            col.i64s = try allocator.alloc(i64, n_rows);
            copyUnaligned(i64, col.i64s, src);
        },
        .float => {
            col.f32s = try allocator.alloc(f32, n_rows);
            copyUnaligned(f32, col.f32s, src);
        },
        .double => {
            col.f64s = try allocator.alloc(f64, n_rows);
            copyUnaligned(f64, col.f64s, src);
        },
        .byte_array => {
            var nbytes: usize = 0;
            var row: usize = 0;
            while (row < n_rows) : (row += 1) {
                nbytes += @intCast(loadUnaligned(parquet.ByteArray, src, row).length);
            }
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, n_rows + 1);
            var off: u32 = 0;
            row = 0;
            while (row < n_rows) : (row += 1) {
                offsets[row] = off;
                const ba = loadUnaligned(parquet.ByteArray, src, row);
                const n: usize = @intCast(ba.length);
                if (n > 0) @memcpy(bytes[off..][0..n], ba.data[0..n]);
                off += @intCast(n);
            }
            offsets[n_rows] = off;
            col.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
        else => return error.UnsupportedType,
    }
    return col;
}

fn toMicros(v: i64, unit: i32) i64 {
    return switch (unit) {
        0 => v *% 1000,
        1 => v,
        2 => @divTrunc(v, 1000),
        else => v,
    };
}

fn decimalFromBe(bytes: []const u8) i128 {
    if (bytes.len == 0) return 0;
    var buf: [16]u8 = undefined;
    const fill: u8 = if (bytes[0] & 0x80 != 0) 0xff else 0x00;
    @memset(&buf, fill);
    if (bytes.len >= 16) {
        @memcpy(&buf, bytes[bytes.len - 16 ..][0..16]);
    } else {
        @memcpy(buf[16 - bytes.len ..], bytes);
    }
    return std.mem.readInt(i128, &buf, .big);
}

fn filterExpr(allocator: std.mem.Allocator, input: Batch, expr: *const sql.BoolExpr, query: ?sql.Query) !Batch {
    const keep = try allocator.alloc(bool, input.len);
    defer allocator.free(keep);
    var n_keep: usize = 0;
    var row: usize = 0;
    while (row < input.len) : (row += 1) {
        const ok = try evalBool(input, row, expr, query);
        keep[row] = ok;
        if (ok) n_keep += 1;
    }
    if (n_keep == input.len) return input;

    const columns = try allocator.alloc(Column, input.columns.len);
    for (input.columns, 0..) |src, ci| {
        columns[ci] = try compactColumn(allocator, src, keep, n_keep);
    }
    return .{ .columns = columns, .len = n_keep };
}

fn evalBool(input: Batch, row: usize, expr: *const sql.BoolExpr, query: ?sql.Query) !bool {
    return switch (expr.*) {
        .cmp => |c| try evalCmp(input, row, c, query),
        .isnull => |p| blk: {
            const idx = try resolveCmpLeft(input, p.left, query);
            const n = input.columns[idx].isNull(row);
            break :blk if (p.negated) !n else n;
        },
        .@"and" => |b| try evalBool(input, row, b.left, query) and try evalBool(input, row, b.right, query),
        .@"or" => |b| try evalBool(input, row, b.left, query) or try evalBool(input, row, b.right, query),
    };
}

fn compactColumn(allocator: std.mem.Allocator, src: Column, keep: []const bool, n_keep: usize) !Column {
    var dst: Column = .{
        .name = src.name,
        .data_type = src.data_type,
        .len = n_keep,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    switch (src.data_type) {
        .boolean => {
            dst.bools = try allocator.alloc(u8, n_keep);
            var o: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) {
                    dst.bools[o] = src.bools[i];
                    o += 1;
                }
            }
        },
        .int32 => {
            dst.i32s = try allocator.alloc(i32, n_keep);
            var o: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) {
                    dst.i32s[o] = src.i32s[i];
                    o += 1;
                }
            }
        },
        .int64, .timestamp, .timestamptz => {
            dst.i64s = try allocator.alloc(i64, n_keep);
            var o: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) {
                    dst.i64s[o] = src.i64s[i];
                    o += 1;
                }
            }
        },
        .float32 => {
            dst.f32s = try allocator.alloc(f32, n_keep);
            var o: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) {
                    dst.f32s[o] = src.f32s[i];
                    o += 1;
                }
            }
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, n_keep);
            var o: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) {
                    dst.f64s[o] = src.f64s[i];
                    o += 1;
                }
            }
        },
        .utf8 => {
            var nbytes: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) nbytes += src.strAt(i).len;
            }
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, n_keep + 1);
            var o: usize = 0;
            var off: u32 = 0;
            for (keep, 0..) |k, i| {
                if (!k) continue;
                offsets[o] = off;
                const s = src.strAt(i);
                @memcpy(bytes[off..][0..s.len], s);
                off += @intCast(s.len);
                o += 1;
            }
            offsets[n_keep] = off;
            dst.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
        .uuid => {
            dst.uuids = try allocator.alloc([16]u8, n_keep);
            var o: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) {
                    dst.uuids[o] = src.uuids[i];
                    o += 1;
                }
            }
        },
        .decimal128 => {
            dst.i128s = try allocator.alloc(i128, n_keep);
            var o: usize = 0;
            for (keep, 0..) |k, i| {
                if (k) {
                    dst.i128s[o] = src.i128s[i];
                    o += 1;
                }
            }
        },
    }
    dst.valid = try compactValid(allocator, src, keep, n_keep);
    return dst;
}

fn evalCmp(input: Batch, row: usize, pred: sql.Cmp, query: ?sql.Query) !bool {
    const idx = try resolveLeft(input, pred, query);
    const col = input.columns[idx];
    if (col.isNull(row)) return false;
    return switch (col.data_type) {
        .int32 => cmpInt(@as(i64, col.i32s[row]), pred),
        .int64, .timestamp, .timestamptz => cmpInt(col.i64s[row], pred),
        .float32 => cmpFloat(@floatCast(col.f32s[row]), pred),
        .float64 => cmpFloat(col.f64s[row], pred),
        .utf8 => cmpStr(col.strAt(row), pred),
        .uuid => cmpUuid(col.uuids[row], pred),
        .decimal128 => cmpDecimal(col, row, pred),
        .boolean => error.TypeMismatch,
    };
}

fn resolveLeft(input: Batch, pred: sql.Cmp, query: ?sql.Query) !usize {
    return resolveCmpLeft(input, pred.left, query);
}

fn resolveCmpLeft(input: Batch, left: sql.CmpLeft, query: ?sql.Query) !usize {
    switch (left) {
        .column => |name| return try input.lookup(name),
        .agg => |a| {
            const q = query orelse return error.InvalidSyntax;
            for (q.items, 0..) |item, i| {
                if (item == .agg and item.agg.kind == a.kind and sql.aggArgsEqual(item.agg.arg, a.arg)) {
                    return i;
                }
            }
            return error.ColumnNotFound;
        },
    }
}

fn cmpInt(value: i64, pred: sql.Cmp) !bool {
    const rhs: i64 = switch (pred.literal) {
        .int => |v| v,
        .float => |v| @intFromFloat(v),
        .string => return error.TypeMismatch,
    };
    return cmpOrd(value, pred.op, rhs);
}

fn cmpFloat(value: f64, pred: sql.Cmp) !bool {
    const rhs: f64 = switch (pred.literal) {
        .int => |v| @floatFromInt(v),
        .float => |v| v,
        .string => return error.TypeMismatch,
    };
    return switch (pred.op) {
        .eq => value == rhs,
        .ne => value != rhs,
        .lt => value < rhs,
        .le => value <= rhs,
        .gt => value > rhs,
        .ge => value >= rhs,
    };
}

fn cmpStr(value: []const u8, pred: sql.Cmp) !bool {
    const rhs = switch (pred.literal) {
        .string => |s| s,
        else => return error.TypeMismatch,
    };
    const ord = std.mem.order(u8, value, rhs);
    return switch (pred.op) {
        .eq => ord == .eq,
        .ne => ord != .eq,
        .lt => ord == .lt,
        .le => ord != .gt,
        .gt => ord == .gt,
        .ge => ord != .lt,
    };
}

fn cmpOrd(lhs: i64, op: sql.CmpOp, rhs: i64) bool {
    return switch (op) {
        .eq => lhs == rhs,
        .ne => lhs != rhs,
        .lt => lhs < rhs,
        .le => lhs <= rhs,
        .gt => lhs > rhs,
        .ge => lhs >= rhs,
    };
}

fn pow10i128(scale: i32) i128 {
    var p: i128 = 1;
    var i: i32 = 0;
    while (i < scale) : (i += 1) p *= 10;
    return p;
}

fn cmpDecimal(col: Column, row: usize, pred: sql.Cmp) !bool {
    const lhs = col.i128s[row];
    const rhs: i128 = switch (pred.literal) {
        .int => |v| @as(i128, v) * pow10i128(col.decimal_scale),
        .float => |v| blk: {
            const scaled = v * @as(f64, @floatFromInt(pow10i128(col.decimal_scale)));
            break :blk @intFromFloat(@round(scaled));
        },
        .string => return error.TypeMismatch,
    };
    return switch (pred.op) {
        .eq => lhs == rhs,
        .ne => lhs != rhs,
        .lt => lhs < rhs,
        .le => lhs <= rhs,
        .gt => lhs > rhs,
        .ge => lhs >= rhs,
    };
}

fn parseUuid(s: []const u8) ![16]u8 {
    var hex: [32]u8 = undefined;
    var n: usize = 0;
    for (s) |ch| {
        if (ch == '-') continue;
        if (n >= 32) return error.TypeMismatch;
        hex[n] = ch;
        n += 1;
    }
    if (n != 32) return error.TypeMismatch;
    var out: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&out, &hex) catch return error.TypeMismatch;
    return out;
}

fn cmpUuid(value: [16]u8, pred: sql.Cmp) !bool {
    const rhs = switch (pred.literal) {
        .string => |s| try parseUuid(s),
        else => return error.TypeMismatch,
    };
    const ord = std.mem.order(u8, &value, &rhs);
    return switch (pred.op) {
        .eq => ord == .eq,
        .ne => ord != .eq,
        .lt => ord == .lt,
        .le => ord != .gt,
        .gt => ord == .gt,
        .ge => ord != .lt,
    };
}

fn inGroupBy(query: sql.Query, name: []const u8) bool {
    for (query.group_by) |g| {
        if (std.ascii.eqlIgnoreCase(g, name)) return true;
    }
    return false;
}

fn validateAgg(query: sql.Query) !void {
    if (query.isStar() and query.needsAgg()) return error.UnsupportedSql;
    if (query.hasAgg() and query.group_by.len == 0) {
        for (query.items) |item| {
            if (item != .agg) return error.InvalidSyntax;
        }
        return;
    }
    for (query.items) |item| {
        switch (item) {
            .star => return error.UnsupportedSql,
            .column => |c| if (!inGroupBy(query, c.name)) return error.InvalidSyntax,
            .scalar => return error.UnsupportedSql,
            .literal => return error.UnsupportedSql,
            .agg => {},
            .window => return error.UnsupportedSql,
        }
    }
}

const AggState = struct {
    kind: sql.AggKind,
    count: u64 = 0,
    sum_i: i64 = 0,
    sum_f: f64 = 0,
    sum_d: i128 = 0,
    min_i: i64 = 0,
    max_i: i64 = 0,
    min_f: f64 = 0,
    max_f: f64 = 0,
    min_s: []const u8 = "",
    max_s: []const u8 = "",
    min_d: i128 = 0,
    max_d: i128 = 0,
    min_u: [16]u8 = @splat(0),
    max_u: [16]u8 = @splat(0),
    is_float: bool = false,
    is_str: bool = false,
    is_decimal: bool = false,
    is_uuid: bool = false,
    inited: bool = false,
    decimal_precision: i32 = 0,
    decimal_scale: i32 = 0,
};

const Group = struct {
    first_row: usize,
    states: []AggState,
};

fn numericI64(col: Column, row: usize) !i64 {
    return switch (col.data_type) {
        .int32 => col.i32s[row],
        .int64, .timestamp, .timestamptz => col.i64s[row],
        else => error.TypeMismatch,
    };
}

fn feed(state: *AggState, col: ?Column, row: usize, str_alloc: ?std.mem.Allocator) !void {
    const c = col orelse {
        if (state.kind != .count) return error.InvalidSyntax;
        state.count += 1;
        return;
    };
    if (c.isNull(row)) return;
    state.count += 1;
    switch (state.kind) {
        .count => {},
        .sum, .avg => {
            if (c.data_type == .timestamp or c.data_type == .timestamptz or c.data_type == .uuid)
                return error.TypeMismatch;
            if (c.data_type == .decimal128) {
                state.is_decimal = true;
                state.decimal_precision = c.decimal_precision;
                state.decimal_scale = c.decimal_scale;
                state.sum_d += c.i128s[row];
                const scale = pow10i128(c.decimal_scale);
                state.sum_f += @as(f64, @floatFromInt(c.i128s[row])) / @as(f64, @floatFromInt(scale));
            } else if (c.data_type == .float64 or c.data_type == .float32) {
                state.is_float = true;
                state.sum_f += if (c.data_type == .float32) @floatCast(c.f32s[row]) else c.f64s[row];
            } else {
                const v = try numericI64(c, row);
                state.sum_i += v;
                state.sum_f += @floatFromInt(v);
            }
        },
        .min => switch (c.data_type) {
            .utf8 => {
                state.is_str = true;
                const s = c.strAt(row);
                if (!state.inited or std.mem.order(u8, s, state.min_s) == .lt) {
                    state.min_s = if (str_alloc) |a| try a.dupe(u8, s) else s;
                }
            },
            .uuid => {
                state.is_uuid = true;
                if (!state.inited or std.mem.order(u8, &c.uuids[row], &state.min_u) == .lt)
                    state.min_u = c.uuids[row];
            },
            .decimal128 => {
                state.is_decimal = true;
                state.decimal_precision = c.decimal_precision;
                state.decimal_scale = c.decimal_scale;
                if (!state.inited or c.i128s[row] < state.min_d) state.min_d = c.i128s[row];
            },
            .float32, .float64 => {
                state.is_float = true;
                const v: f64 = if (c.data_type == .float32) @floatCast(c.f32s[row]) else c.f64s[row];
                if (!state.inited or v < state.min_f) state.min_f = v;
            },
            else => {
                const v = try numericI64(c, row);
                if (!state.inited or v < state.min_i) state.min_i = v;
            },
        },
        .max => switch (c.data_type) {
            .utf8 => {
                state.is_str = true;
                const s = c.strAt(row);
                if (!state.inited or std.mem.order(u8, s, state.max_s) == .gt) {
                    state.max_s = if (str_alloc) |a| try a.dupe(u8, s) else s;
                }
            },
            .uuid => {
                state.is_uuid = true;
                if (!state.inited or std.mem.order(u8, &c.uuids[row], &state.max_u) == .gt)
                    state.max_u = c.uuids[row];
            },
            .decimal128 => {
                state.is_decimal = true;
                state.decimal_precision = c.decimal_precision;
                state.decimal_scale = c.decimal_scale;
                if (!state.inited or c.i128s[row] > state.max_d) state.max_d = c.i128s[row];
            },
            .float32, .float64 => {
                state.is_float = true;
                const v: f64 = if (c.data_type == .float32) @floatCast(c.f32s[row]) else c.f64s[row];
                if (!state.inited or v > state.max_f) state.max_f = v;
            },
            else => {
                const v = try numericI64(c, row);
                if (!state.inited or v > state.max_i) state.max_i = v;
            },
        },
    }
    state.inited = true;
}

fn encodeKey(allocator: std.mem.Allocator, input: Batch, row: usize, group_idxs: []const usize) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    for (group_idxs) |idx| {
        const col = input.columns[idx];
        try buf.append(allocator, if (col.isNull(row)) 0 else 1);
        if (col.isNull(row)) continue;
        try buf.append(allocator, @intFromEnum(col.data_type));
        switch (col.data_type) {
            .boolean => try buf.append(allocator, col.bools[row]),
            .int32 => {
                var b: [4]u8 = undefined;
                std.mem.writeInt(i32, &b, col.i32s[row], .little);
                try buf.appendSlice(allocator, &b);
            },
            .int64, .timestamp, .timestamptz => {
                var b: [8]u8 = undefined;
                std.mem.writeInt(i64, &b, col.i64s[row], .little);
                try buf.appendSlice(allocator, &b);
            },
            .uuid => try buf.appendSlice(allocator, &col.uuids[row]),
            .decimal128 => {
                var b: [16]u8 = undefined;
                std.mem.writeInt(i128, &b, col.i128s[row], .little);
                try buf.appendSlice(allocator, &b);
            },
            .float32 => {
                var b: [4]u8 = undefined;
                std.mem.writeInt(u32, &b, @bitCast(col.f32s[row]), .little);
                try buf.appendSlice(allocator, &b);
            },
            .float64 => {
                var b: [8]u8 = undefined;
                std.mem.writeInt(u64, &b, @bitCast(col.f64s[row]), .little);
                try buf.appendSlice(allocator, &b);
            },
            .utf8 => {
                const s = col.strAt(row);
                var lenb: [4]u8 = undefined;
                std.mem.writeInt(u32, &lenb, @intCast(s.len), .little);
                try buf.appendSlice(allocator, &lenb);
                try buf.appendSlice(allocator, s);
            },
        }
    }
    return buf.toOwnedSlice(allocator);
}

fn aggColumn(input: Batch, agg: sql.Agg) !?Column {
    const name = agg.arg orelse return null;
    const idx = try input.lookup(name);
    return input.columns[idx];
}

fn countAggs(query: sql.Query) usize {
    var n: usize = 0;
    for (query.items) |item| {
        if (item == .agg) n += 1;
    }
    return n;
}

fn makeStates(allocator: std.mem.Allocator, query: sql.Query) ![]AggState {
    const n = countAggs(query);
    const states = try allocator.alloc(AggState, n);
    var i: usize = 0;
    for (query.items) |item| {
        if (item == .agg) {
            states[i] = .{ .kind = item.agg.kind };
            i += 1;
        }
    }
    return states;
}

fn feedRow(states: []AggState, input: Batch, query: sql.Query, row: usize, str_alloc: ?std.mem.Allocator) !void {
    var i: usize = 0;
    for (query.items) |item| {
        if (item != .agg) continue;
        try feed(&states[i], try aggColumn(input, item.agg), row, str_alloc);
        i += 1;
    }
}

fn aggOutType(state: AggState, src: ?Column) DataType {
    return switch (state.kind) {
        .count => .int64,
        .avg => .float64,
        .sum => if (state.is_float) .float64 else if (state.is_decimal) .decimal128 else .int64,
        .min, .max => if (src) |c| c.data_type else .int64,
    };
}

fn aggName(allocator: std.mem.Allocator, agg: sql.Agg) ![]const u8 {
    if (agg.alias) |a| return a;
    const prefix = switch (agg.kind) {
        .count => "count",
        .sum => "sum",
        .avg => "avg",
        .min => "min",
        .max => "max",
    };
    if (agg.arg) |arg| return std.fmt.allocPrint(allocator, "{s}_{s}", .{ prefix, arg });
    return prefix;
}

fn finalizeAgg(allocator: std.mem.Allocator, state: AggState, src: ?Column, name: []const u8, n_groups: usize) !Column {
    var col: Column = .{
        .name = name,
        .data_type = aggOutType(state, src),
        .len = n_groups,
        .decimal_precision = state.decimal_precision,
        .decimal_scale = state.decimal_scale,
    };
    if (src) |c| {
        if (col.decimal_precision == 0) col.decimal_precision = c.decimal_precision;
        if (col.decimal_scale == 0) col.decimal_scale = c.decimal_scale;
    }
    switch (col.data_type) {
        .int64, .timestamp, .timestamptz => col.i64s = try allocator.alloc(i64, n_groups),
        .float32 => col.f32s = try allocator.alloc(f32, n_groups),
        .float64 => col.f64s = try allocator.alloc(f64, n_groups),
        .int32 => col.i32s = try allocator.alloc(i32, n_groups),
        .boolean => col.bools = try allocator.alloc(u8, n_groups),
        .utf8 => {
            col.utf8.offsets = try allocator.alloc(u32, n_groups + 1);
            col.utf8.bytes = &.{};
        },
        .uuid => col.uuids = try allocator.alloc([16]u8, n_groups),
        .decimal128 => col.i128s = try allocator.alloc(i128, n_groups),
    }
    return col;
}

fn writeAggCell(col: *Column, state: AggState, row: usize, allocator: std.mem.Allocator) !void {
    if (state.kind != .count and !state.inited) {
        try markNull(col, row, allocator);
        if (col.data_type == .utf8) try appendUtf8(col, "", row, allocator);
        return;
    }
    switch (state.kind) {
        .count => col.i64s[row] = @intCast(state.count),
        .sum => {
            if (col.data_type == .float64) {
                col.f64s[row] = state.sum_f;
            } else if (col.data_type == .decimal128) {
                col.i128s[row] = state.sum_d;
            } else {
                col.i64s[row] = state.sum_i;
            }
        },
        .avg => col.f64s[row] = if (state.count == 0) 0 else state.sum_f / @as(f64, @floatFromInt(state.count)),
        .min => switch (col.data_type) {
            .int64, .timestamp, .timestamptz => col.i64s[row] = state.min_i,
            .int32 => col.i32s[row] = @intCast(state.min_i),
            .float32 => col.f32s[row] = @floatCast(state.min_f),
            .float64 => col.f64s[row] = state.min_f,
            .utf8 => try appendUtf8(col, state.min_s, row, allocator),
            .uuid => col.uuids[row] = state.min_u,
            .decimal128 => col.i128s[row] = state.min_d,
            else => return error.TypeMismatch,
        },
        .max => switch (col.data_type) {
            .int64, .timestamp, .timestamptz => col.i64s[row] = state.max_i,
            .int32 => col.i32s[row] = @intCast(state.max_i),
            .float32 => col.f32s[row] = @floatCast(state.max_f),
            .float64 => col.f64s[row] = state.max_f,
            .utf8 => try appendUtf8(col, state.max_s, row, allocator),
            .uuid => col.uuids[row] = state.max_u,
            .decimal128 => col.i128s[row] = state.max_d,
            else => return error.TypeMismatch,
        },
    }
}

fn appendUtf8(col: *Column, s: []const u8, row: usize, allocator: std.mem.Allocator) !void {
    const old = col.utf8.bytes.len;
    col.utf8.bytes = try allocator.realloc(col.utf8.bytes, old + s.len);
    if (s.len > 0) @memcpy(col.utf8.bytes[old..][0..s.len], s);
    col.utf8.offsets[row] = @intCast(old);
    col.utf8.offsets[row + 1] = @intCast(old + s.len);
}

fn copyKeyColumn(allocator: std.mem.Allocator, src: Column, first_rows: []const usize, name: []const u8) !Column {
    var dst: Column = .{
        .name = name,
        .data_type = src.data_type,
        .len = first_rows.len,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    switch (src.data_type) {
        .boolean => {
            dst.bools = try allocator.alloc(u8, first_rows.len);
            for (first_rows, 0..) |r, i| dst.bools[i] = src.bools[r];
        },
        .int32 => {
            dst.i32s = try allocator.alloc(i32, first_rows.len);
            for (first_rows, 0..) |r, i| dst.i32s[i] = src.i32s[r];
        },
        .int64, .timestamp, .timestamptz => {
            dst.i64s = try allocator.alloc(i64, first_rows.len);
            for (first_rows, 0..) |r, i| dst.i64s[i] = src.i64s[r];
        },
        .uuid => {
            dst.uuids = try allocator.alloc([16]u8, first_rows.len);
            for (first_rows, 0..) |r, i| dst.uuids[i] = src.uuids[r];
        },
        .decimal128 => {
            dst.i128s = try allocator.alloc(i128, first_rows.len);
            for (first_rows, 0..) |r, i| dst.i128s[i] = src.i128s[r];
        },
        .float32 => {
            dst.f32s = try allocator.alloc(f32, first_rows.len);
            for (first_rows, 0..) |r, i| dst.f32s[i] = src.f32s[r];
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, first_rows.len);
            for (first_rows, 0..) |r, i| dst.f64s[i] = src.f64s[r];
        },
        .utf8 => {
            var nbytes: usize = 0;
            for (first_rows) |r| nbytes += src.strAt(r).len;
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, first_rows.len + 1);
            var off: u32 = 0;
            for (first_rows, 0..) |r, i| {
                offsets[i] = off;
                const s = src.strAt(r);
                if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
                off += @intCast(s.len);
            }
            offsets[first_rows.len] = off;
            dst.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
    }
    dst.valid = try mapValid(allocator, src, first_rows);
    return dst;
}

fn aggregateBatch(allocator: std.mem.Allocator, input: Batch, query: sql.Query) !Batch {
    try validateAgg(query);

    const group_idxs = try allocator.alloc(usize, query.group_by.len);
    for (query.group_by, 0..) |name, i| {
        group_idxs[i] = try input.lookup(name);
    }

    var groups: std.StringArrayHashMapUnmanaged(Group) = .empty;

    var row: usize = 0;
    while (row < input.len) : (row += 1) {
        if (group_idxs.len == 0) {
            if (groups.getPtr("")) |g| {
                try feedRow(g.states, input, query, row, null);
            } else {
                const states = try makeStates(allocator, query);
                try feedRow(states, input, query, row, null);
                try groups.put(allocator, "", .{ .first_row = row, .states = states });
            }
            continue;
        }

        const encoded = try encodeKey(allocator, input, row, group_idxs);
        if (groups.getPtr(encoded)) |g| {
            try feedRow(g.states, input, query, row, null);
            allocator.free(encoded);
        } else {
            const states = try makeStates(allocator, query);
            try feedRow(states, input, query, row, null);
            try groups.put(allocator, encoded, .{ .first_row = row, .states = states });
        }
    }

    const n_groups = groups.count();
    if (n_groups == 0 and query.group_by.len == 0 and query.hasAgg()) {
        const out_cols = try allocator.alloc(Column, query.items.len);
        for (query.items, 0..) |item, col_i| {
            const agg = item.agg;
            const src = try aggColumn(input, agg);
            const name = try aggName(allocator, agg);
            const empty_state = AggState{ .kind = agg.kind };
            var col = try finalizeAgg(allocator, empty_state, src, name, 1);
            try writeAggCell(&col, empty_state, 0, allocator);
            out_cols[col_i] = col;
        }
        return .{ .columns = out_cols, .len = 1 };
    }

    const first_rows = try allocator.alloc(usize, n_groups);
    for (groups.values(), 0..) |g, i| first_rows[i] = g.first_row;

    const out_cols = try allocator.alloc(Column, query.items.len);
    var agg_i: usize = 0;
    for (query.items, 0..) |item, col_i| {
        switch (item) {
            .star, .scalar, .literal, .window => return error.UnsupportedSql,
            .column => |c| {
                const key = if (c.qualifier) |q|
                    try std.fmt.allocPrint(allocator, "{s}.{s}", .{ q, c.name })
                else
                    c.name;
                const src_i = try input.lookup(key);
                const name = c.alias orelse key;
                out_cols[col_i] = try copyKeyColumn(allocator, input.columns[src_i], first_rows, name);
            },
            .agg => |agg| {
                const src = try aggColumn(input, agg);
                const name = try aggName(allocator, agg);
                const template = if (n_groups == 0)
                    AggState{ .kind = agg.kind }
                else
                    groups.values()[0].states[agg_i];
                var col = try finalizeAgg(allocator, template, src, name, n_groups);
                if (n_groups > 0) {
                    for (groups.values(), 0..) |g, gi| {
                        try writeAggCell(&col, g.states[agg_i], gi, allocator);
                    }
                }
                out_cols[col_i] = col;
                agg_i += 1;
            },
        }
    }

    return .{ .columns = out_cols, .len = n_groups };
}

const SortCtx = struct {
    batch: Batch,
    keys: []const sql.OrderBy,

    fn lessThan(ctx: SortCtx, a: usize, b: usize) bool {
        for (ctx.keys) |ob| {
            const col_i = ctx.batch.lookup(ob.column) catch return false;
            const ord = compareCells(ctx.batch.columns[col_i], a, b);
            if (ord == .eq) continue;
            if (ob.desc) return ord == .gt;
            return ord == .lt;
        }
        return false;
    }
};

fn compareCells(col: Column, a: usize, b: usize) std.math.Order {
    const an = col.isNull(a);
    const bn = col.isNull(b);
    if (an and bn) return .eq;
    if (an) return .gt;
    if (bn) return .lt;
    return switch (col.data_type) {
        .boolean => std.math.order(col.bools[a], col.bools[b]),
        .int32 => std.math.order(col.i32s[a], col.i32s[b]),
        .int64, .timestamp, .timestamptz => std.math.order(col.i64s[a], col.i64s[b]),
        .float32 => std.math.order(col.f32s[a], col.f32s[b]),
        .float64 => std.math.order(col.f64s[a], col.f64s[b]),
        .utf8 => std.mem.order(u8, col.strAt(a), col.strAt(b)),
        .uuid => std.mem.order(u8, &col.uuids[a], &col.uuids[b]),
        .decimal128 => std.math.order(col.i128s[a], col.i128s[b]),
    };
}

fn sortBatch(allocator: std.mem.Allocator, input: Batch, keys: []const sql.OrderBy) !Batch {
    if (input.len <= 1 or keys.len == 0) return input;
    for (keys) |ob| {
        _ = try input.lookup(ob.column);
    }
    const idx = try allocator.alloc(usize, input.len);
    for (idx, 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, idx, SortCtx{ .batch = input, .keys = keys }, SortCtx.lessThan);
    return gatherRows(allocator, input, idx);
}

fn distinctBatch(allocator: std.mem.Allocator, input: Batch) !Batch {
    if (input.len <= 1) return input;
    const col_idxs = try allocator.alloc(usize, input.columns.len);
    for (col_idxs, 0..) |*slot, i| slot.* = i;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var keep: std.ArrayList(usize) = .empty;
    var row: usize = 0;
    while (row < input.len) : (row += 1) {
        const key = try encodeKey(allocator, input, row, col_idxs);
        const gop = try seen.getOrPut(allocator, key);
        if (gop.found_existing) {
            allocator.free(key);
        } else {
            try keep.append(allocator, row);
        }
    }
    return gatherRows(allocator, input, keep.items);
}

fn scalarName(s: sql.Scalar) []const u8 {
    if (s.alias) |a| return a;
    return switch (s.kind) {
        .abs => "abs",
        .round => "round",
        .cast => "cast",
        .coalesce => "coalesce",
    };
}

fn evalScalar(allocator: std.mem.Allocator, input: Batch, s: sql.Scalar) !Column {
    const src_i = try input.lookup(s.arg);
    const src = input.columns[src_i];
    const name = scalarName(s);
    return switch (s.kind) {
        .abs => evalAbs(allocator, src, name),
        .round => evalRound(allocator, src, name),
        .cast => evalCast(allocator, src, s.cast_type orelse return error.InvalidSyntax, name),
        .coalesce => evalCoalesce(allocator, input, s),
    };
}

fn absI64(v: i64) i64 {
    if (v >= 0) return v;
    return -v;
}

fn evalAbs(allocator: std.mem.Allocator, src: Column, name: []const u8) !Column {
    var dst: Column = .{
        .name = name,
        .data_type = src.data_type,
        .len = src.len,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    switch (src.data_type) {
        .int32 => {
            dst.i32s = try allocator.alloc(i32, src.len);
            for (src.i32s, 0..) |v, i| {
                const wide: i64 = v;
                dst.i32s[i] = @intCast(if (wide < 0) -wide else wide);
            }
        },
        .int64 => {
            dst.i64s = try allocator.alloc(i64, src.len);
            for (src.i64s, 0..) |v, i| dst.i64s[i] = absI64(v);
        },
        .float32 => {
            dst.f32s = try allocator.alloc(f32, src.len);
            for (src.f32s, 0..) |v, i| dst.f32s[i] = @abs(v);
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, src.len);
            for (src.f64s, 0..) |v, i| dst.f64s[i] = @abs(v);
        },
        .decimal128 => {
            dst.i128s = try allocator.alloc(i128, src.len);
            for (src.i128s, 0..) |v, i| dst.i128s[i] = if (v < 0) -v else v;
        },
        else => return error.TypeMismatch,
    }
    dst.valid = try dupeValid(allocator, src);
    return dst;
}

fn evalRound(allocator: std.mem.Allocator, src: Column, name: []const u8) !Column {
    var dst: Column = .{ .name = name, .data_type = src.data_type, .len = src.len };
    switch (src.data_type) {
        .int32 => dst.i32s = src.i32s,
        .int64 => dst.i64s = src.i64s,
        .float32 => {
            dst.f32s = try allocator.alloc(f32, src.len);
            for (src.f32s, 0..) |v, i| dst.f32s[i] = @round(v);
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, src.len);
            for (src.f64s, 0..) |v, i| dst.f64s[i] = @round(v);
        },
        else => return error.TypeMismatch,
    }
    dst.valid = try dupeValid(allocator, src);
    return dst;
}

fn evalCoalesce(allocator: std.mem.Allocator, input: Batch, s: sql.Scalar) !Column {
    const src_i = try input.lookup(s.arg);
    const src = input.columns[src_i];
    const other: ?Column = if (s.coalesce_col) |n| input.columns[try input.lookup(n)] else null;
    if (other) |o| {
        if (o.data_type != src.data_type) return error.TypeMismatch;
    }
    const name = scalarName(s);
    const n = src.len;
    var dst: Column = .{
        .name = name,
        .data_type = src.data_type,
        .len = n,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    try allocTyped(allocator, &dst, n);

    const picks = try allocator.alloc(CoalescePick, n);
    var any_null = false;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (src.isValid(i)) {
            picks[i] = .src;
        } else if (other) |o| {
            if (o.isValid(i)) {
                picks[i] = .other;
            } else if (s.coalesce_lit != null) {
                picks[i] = .lit;
            } else {
                picks[i] = .none;
                any_null = true;
            }
        } else if (s.coalesce_lit != null) {
            picks[i] = .lit;
        } else {
            picks[i] = .none;
            any_null = true;
        }
    }

    if (src.data_type == .utf8) {
        dst.utf8 = try coalesceUtf8(allocator, src, other, s.coalesce_lit, picks);
    } else {
        i = 0;
        while (i < n) : (i += 1) {
            switch (picks[i]) {
                .src => copyTypedCell(&dst, i, src, i),
                .other => copyTypedCell(&dst, i, other.?, i),
                .lit => try writeLitCell(&dst, i, s.coalesce_lit.?),
                .none => {},
            }
        }
    }
    if (any_null) {
        const valid = try allocator.alloc(u8, n);
        for (picks, valid) |p, *v| v.* = if (p == .none) 0 else 1;
        dst.valid = valid;
    }
    return dst;
}

fn evalCast(allocator: std.mem.Allocator, src: Column, to: sql.CastType, name: []const u8) !Column {
    const n = src.len;
    var dst: Column = .{
        .name = name,
        .data_type = switch (to) {
            .int64 => .int64,
            .float64 => .float64,
            .boolean => .boolean,
            .utf8 => .utf8,
        },
        .len = n,
    };
    switch (to) {
        .int64 => {
            dst.i64s = try allocator.alloc(i64, n);
            if (n > 0) @memset(dst.i64s, 0);
            var i: usize = 0;
            while (i < n) : (i += 1) {
                if (src.isNull(i)) continue;
                dst.i64s[i] = try cellAsI64(src, i);
            }
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, n);
            if (n > 0) @memset(dst.f64s, 0);
            var i: usize = 0;
            while (i < n) : (i += 1) {
                if (src.isNull(i)) continue;
                dst.f64s[i] = try cellAsF64(src, i);
            }
        },
        .boolean => {
            dst.bools = try allocator.alloc(u8, n);
            if (n > 0) @memset(dst.bools, 0);
            var i: usize = 0;
            while (i < n) : (i += 1) {
                if (src.isNull(i)) continue;
                dst.bools[i] = if (try cellAsI64(src, i) != 0) 1 else 0;
            }
        },
        .utf8 => dst.utf8 = try cellsAsUtf8(allocator, src),
    }
    dst.valid = try dupeValid(allocator, src);
    return dst;
}

fn cellAsI64(col: Column, row: usize) !i64 {
    return switch (col.data_type) {
        .boolean => @as(i64, col.bools[row]),
        .int32 => col.i32s[row],
        .int64, .timestamp, .timestamptz => col.i64s[row],
        .float32 => @intFromFloat(@round(col.f32s[row])),
        .float64 => @intFromFloat(@round(col.f64s[row])),
        .utf8 => std.fmt.parseInt(i64, col.strAt(row), 10) catch error.TypeMismatch,
        .decimal128 => @intCast(@divTrunc(col.i128s[row], pow10i128(col.decimal_scale))),
        .uuid => error.TypeMismatch,
    };
}

fn cellAsF64(col: Column, row: usize) !f64 {
    return switch (col.data_type) {
        .boolean => @floatFromInt(col.bools[row]),
        .int32 => @floatFromInt(col.i32s[row]),
        .int64, .timestamp, .timestamptz => @floatFromInt(col.i64s[row]),
        .float32 => col.f32s[row],
        .float64 => col.f64s[row],
        .utf8 => std.fmt.parseFloat(f64, col.strAt(row)) catch error.TypeMismatch,
        .decimal128 => @as(f64, @floatFromInt(col.i128s[row])) / @as(f64, @floatFromInt(pow10i128(col.decimal_scale))),
        .uuid => error.TypeMismatch,
    };
}

fn cellsAsUtf8(allocator: std.mem.Allocator, src: Column) !batch_mod.Utf8 {
    if (src.data_type == .utf8) return src.utf8;
    var nbytes: usize = 0;
    var row: usize = 0;
    var tmp: [80]u8 = undefined;
    while (row < src.len) : (row += 1) {
        nbytes += (try formatCell(src, row, &tmp)).len;
    }
    const bytes = try allocator.alloc(u8, nbytes);
    const offsets = try allocator.alloc(u32, src.len + 1);
    var off: u32 = 0;
    row = 0;
    while (row < src.len) : (row += 1) {
        offsets[row] = off;
        const s = try formatCell(src, row, &tmp);
        if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
        off += @intCast(s.len);
    }
    offsets[src.len] = off;
    return .{ .offsets = offsets, .bytes = bytes };
}

fn formatCell(col: Column, row: usize, buf: []u8) ![]const u8 {
    return switch (col.data_type) {
        .boolean => if (col.bools[row] != 0) "true" else "false",
        .int32 => std.fmt.bufPrint(buf, "{d}", .{col.i32s[row]}),
        .int64, .timestamp, .timestamptz => std.fmt.bufPrint(buf, "{d}", .{col.i64s[row]}),
        .float32 => std.fmt.bufPrint(buf, "{d}", .{col.f32s[row]}),
        .float64 => std.fmt.bufPrint(buf, "{d}", .{col.f64s[row]}),
        .utf8 => col.strAt(row),
        .uuid => formatUuid(col.uuids[row], buf),
        .decimal128 => formatDecimal(col.i128s[row], col.decimal_scale, buf),
    };
}

fn formatUuid(id: [16]u8, buf: []u8) ![]const u8 {
    const hex = std.fmt.bytesToHex(&id, .lower);
    return std.fmt.bufPrint(buf, "{s}-{s}-{s}-{s}-{s}", .{
        hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32],
    });
}

fn formatDecimal(unscaled: i128, scale: i32, buf: []u8) ![]const u8 {
    if (scale <= 0) return std.fmt.bufPrint(buf, "{d}", .{unscaled});
    var digits_buf: [64]u8 = undefined;
    const digits = try std.fmt.bufPrint(&digits_buf, "{d}", .{unscaled});
    const neg = digits[0] == '-';
    const body = if (neg) digits[1..] else digits;
    const scale_usz: usize = @intCast(scale);
    var o: usize = 0;
    if (neg) {
        buf[o] = '-';
        o += 1;
    }
    if (body.len <= scale_usz) {
        buf[o] = '0';
        o += 1;
        buf[o] = '.';
        o += 1;
        var z: usize = 0;
        while (z < scale_usz - body.len) : (z += 1) {
            buf[o] = '0';
            o += 1;
        }
        @memcpy(buf[o..][0..body.len], body);
        o += body.len;
        return buf[0..o];
    }
    const split = body.len - scale_usz;
    @memcpy(buf[o..][0..split], body[0..split]);
    o += split;
    buf[o] = '.';
    o += 1;
    @memcpy(buf[o..][0..scale_usz], body[split..]);
    o += scale_usz;
    return buf[0..o];
}

fn takeRange(allocator: std.mem.Allocator, input: Batch, offset: u64, limit: ?u64) !Batch {
    const start: usize = @intCast(@min(offset, @as(u64, @intCast(input.len))));
    const remaining = input.len - start;
    const n: usize = if (limit) |lim| @intCast(@min(lim, @as(u64, @intCast(remaining)))) else remaining;
    if (start == 0 and n == input.len) return input;
    const idx = try allocator.alloc(usize, n);
    for (idx, 0..) |*slot, i| slot.* = start + i;
    return gatherRows(allocator, input, idx);
}

fn gatherRows(allocator: std.mem.Allocator, input: Batch, idx: []const usize) !Batch {
    const columns = try allocator.alloc(Column, input.columns.len);
    for (input.columns, 0..) |src, ci| {
        columns[ci] = try gatherColumn(allocator, src, idx);
    }
    return .{ .columns = columns, .len = idx.len };
}

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
    dst.valid = try mapValid(allocator, src, idx);
    return dst;
}

fn icebergType(f: iceberg.SchemaField) !DataType {
    const name = f.type_name;
    if (std.ascii.eqlIgnoreCase(name, "boolean")) return .boolean;
    if (std.ascii.eqlIgnoreCase(name, "int") or std.ascii.eqlIgnoreCase(name, "date")) return .int32;
    if (std.ascii.eqlIgnoreCase(name, "long")) return .int64;
    if (std.ascii.eqlIgnoreCase(name, "float")) return .float32;
    if (std.ascii.eqlIgnoreCase(name, "double")) return .float64;
    if (std.ascii.eqlIgnoreCase(name, "string") or std.ascii.eqlIgnoreCase(name, "binary")) return .utf8;
    if (std.ascii.eqlIgnoreCase(name, "timestamp") or std.ascii.eqlIgnoreCase(name, "timestamp_ntz")) return .timestamp;
    if (std.ascii.eqlIgnoreCase(name, "timestamptz") or std.ascii.eqlIgnoreCase(name, "timestamp_tz")) return .timestamptz;
    if (std.ascii.eqlIgnoreCase(name, "uuid")) return .uuid;
    if (std.ascii.eqlIgnoreCase(name, "decimal")) return .decimal128;
    return error.UnsupportedType;
}

fn emptyFromSchema(allocator: std.mem.Allocator, fields: []const iceberg.SchemaField) !Batch {
    const columns = try allocator.alloc(Column, fields.len);
    for (fields, 0..) |f, i| {
        const dt = try icebergType(f);
        var col: Column = .{
            .name = f.name,
            .data_type = dt,
            .len = 0,
            .decimal_precision = f.decimal_precision,
            .decimal_scale = f.decimal_scale,
        };
        if (dt == .utf8) {
            col.utf8.offsets = try allocator.alloc(u32, 1);
            col.utf8.offsets[0] = 0;
        }
        columns[i] = col;
    }
    return .{ .columns = columns, .len = 0 };
}

fn concatBatches(allocator: std.mem.Allocator, parts: []const Batch) !Batch {
    const n_cols = parts[0].columns.len;
    var total: usize = 0;
    for (parts) |p| {
        if (p.columns.len != n_cols) return error.SchemaMismatch;
        total += p.len;
    }
    const columns = try allocator.alloc(Column, n_cols);
    for (parts[0].columns, 0..) |template, ci| {
        columns[ci] = try concatColumn(allocator, parts, ci, template, total);
    }
    return .{ .columns = columns, .len = total };
}

fn concatColumn(allocator: std.mem.Allocator, parts: []const Batch, ci: usize, template: Column, total: usize) !Column {
    var dst: Column = .{
        .name = try allocator.dupe(u8, template.name),
        .data_type = template.data_type,
        .len = total,
        .decimal_precision = template.decimal_precision,
        .decimal_scale = template.decimal_scale,
    };
    switch (template.data_type) {
        .boolean => {
            dst.bools = try allocator.alloc(u8, total);
            var o: usize = 0;
            for (parts) |p| {
                @memcpy(dst.bools[o..][0..p.len], p.columns[ci].bools[0..p.len]);
                o += p.len;
            }
        },
        .int32 => {
            dst.i32s = try allocator.alloc(i32, total);
            var o: usize = 0;
            for (parts) |p| {
                @memcpy(dst.i32s[o..][0..p.len], p.columns[ci].i32s[0..p.len]);
                o += p.len;
            }
        },
        .int64, .timestamp, .timestamptz => {
            dst.i64s = try allocator.alloc(i64, total);
            var o: usize = 0;
            for (parts) |p| {
                @memcpy(dst.i64s[o..][0..p.len], p.columns[ci].i64s[0..p.len]);
                o += p.len;
            }
        },
        .float32 => {
            dst.f32s = try allocator.alloc(f32, total);
            var o: usize = 0;
            for (parts) |p| {
                @memcpy(dst.f32s[o..][0..p.len], p.columns[ci].f32s[0..p.len]);
                o += p.len;
            }
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, total);
            var o: usize = 0;
            for (parts) |p| {
                @memcpy(dst.f64s[o..][0..p.len], p.columns[ci].f64s[0..p.len]);
                o += p.len;
            }
        },
        .utf8 => {
            var nbytes: usize = 0;
            for (parts) |p| nbytes += p.columns[ci].utf8.bytes.len;
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, total + 1);
            var off: u32 = 0;
            var row: usize = 0;
            for (parts) |p| {
                const src = p.columns[ci];
                var r: usize = 0;
                while (r < p.len) : (r += 1) {
                    offsets[row] = off;
                    const s = src.strAt(r);
                    if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
                    off += @intCast(s.len);
                    row += 1;
                }
            }
            offsets[total] = off;
            dst.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
        .uuid => {
            dst.uuids = try allocator.alloc([16]u8, total);
            var o: usize = 0;
            for (parts) |p| {
                @memcpy(dst.uuids[o..][0..p.len], p.columns[ci].uuids[0..p.len]);
                o += p.len;
            }
        },
        .decimal128 => {
            dst.i128s = try allocator.alloc(i128, total);
            var o: usize = 0;
            for (parts) |p| {
                @memcpy(dst.i128s[o..][0..p.len], p.columns[ci].i128s[0..p.len]);
                o += p.len;
            }
        },
    }
    dst.valid = try concatValid(allocator, parts, ci, total);
    return dst;
}

fn copyArrowValid(allocator: std.mem.Allocator, bitmap: ?[*]const u8, n: usize) ![]u8 {
    if (n == 0) return &.{};
    const p = bitmap orelse return &.{};
    const out = try allocator.alloc(u8, n);
    var any_null = false;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const bit: u8 = (p[i >> 3] >> @intCast(i & 7)) & 1;
        out[i] = bit;
        if (bit == 0) any_null = true;
    }
    if (!any_null) {
        allocator.free(out);
        return &.{};
    }
    return out;
}

fn dupeValid(allocator: std.mem.Allocator, src: Column) ![]u8 {
    if (src.valid.len == 0) return &.{};
    return allocator.dupe(u8, src.valid);
}

fn mapValid(allocator: std.mem.Allocator, src: Column, idx: []const usize) ![]u8 {
    if (src.valid.len == 0) return &.{};
    const out = try allocator.alloc(u8, idx.len);
    var any_null = false;
    for (idx, 0..) |row, i| {
        out[i] = src.valid[row];
        if (out[i] == 0) any_null = true;
    }
    if (!any_null) {
        allocator.free(out);
        return &.{};
    }
    return out;
}

fn compactValid(allocator: std.mem.Allocator, src: Column, keep: []const bool, n_keep: usize) ![]u8 {
    if (src.valid.len == 0) return &.{};
    const out = try allocator.alloc(u8, n_keep);
    var o: usize = 0;
    var any_null = false;
    for (keep, 0..) |k, i| {
        if (!k) continue;
        out[o] = src.valid[i];
        if (out[o] == 0) any_null = true;
        o += 1;
    }
    if (!any_null) {
        allocator.free(out);
        return &.{};
    }
    return out;
}

fn concatValid(allocator: std.mem.Allocator, parts: []const Batch, ci: usize, total: usize) ![]u8 {
    var need = false;
    for (parts) |p| {
        if (p.columns[ci].valid.len > 0) {
            need = true;
            break;
        }
    }
    if (!need) return &.{};
    const out = try allocator.alloc(u8, total);
    @memset(out, 1);
    var o: usize = 0;
    var any_null = false;
    for (parts) |p| {
        const src = p.columns[ci];
        if (src.valid.len > 0) {
            @memcpy(out[o..][0..p.len], src.valid[0..p.len]);
            for (src.valid[0..p.len]) |v| {
                if (v == 0) any_null = true;
            }
        }
        o += p.len;
    }
    if (!any_null) {
        allocator.free(out);
        return &.{};
    }
    return out;
}

fn markNull(col: *Column, row: usize, allocator: std.mem.Allocator) !void {
    if (col.valid.len == 0) {
        col.valid = try allocator.alloc(u8, col.len);
        @memset(col.valid, 1);
    }
    col.valid[row] = 0;
}

fn allocTyped(allocator: std.mem.Allocator, col: *Column, n: usize) !void {
    switch (col.data_type) {
        .boolean => {
            col.bools = try allocator.alloc(u8, n);
            if (n > 0) @memset(col.bools, 0);
        },
        .int32 => {
            col.i32s = try allocator.alloc(i32, n);
            if (n > 0) @memset(col.i32s, 0);
        },
        .int64, .timestamp, .timestamptz => {
            col.i64s = try allocator.alloc(i64, n);
            if (n > 0) @memset(col.i64s, 0);
        },
        .float32 => {
            col.f32s = try allocator.alloc(f32, n);
            if (n > 0) @memset(col.f32s, 0);
        },
        .float64 => {
            col.f64s = try allocator.alloc(f64, n);
            if (n > 0) @memset(col.f64s, 0);
        },
        .utf8 => {},
        .uuid => {
            col.uuids = try allocator.alloc([16]u8, n);
            if (n > 0) @memset(std.mem.sliceAsBytes(col.uuids), 0);
        },
        .decimal128 => {
            col.i128s = try allocator.alloc(i128, n);
            if (n > 0) @memset(col.i128s, 0);
        },
    }
}

fn copyTypedCell(dst: *Column, di: usize, src: Column, si: usize) void {
    switch (src.data_type) {
        .boolean => dst.bools[di] = src.bools[si],
        .int32 => dst.i32s[di] = src.i32s[si],
        .int64, .timestamp, .timestamptz => dst.i64s[di] = src.i64s[si],
        .float32 => dst.f32s[di] = src.f32s[si],
        .float64 => dst.f64s[di] = src.f64s[si],
        .utf8 => {},
        .uuid => dst.uuids[di] = src.uuids[si],
        .decimal128 => dst.i128s[di] = src.i128s[si],
    }
}

fn writeLitCell(dst: *Column, row: usize, lit: sql.Literal) !void {
    switch (dst.data_type) {
        .int32 => dst.i32s[row] = @intCast(switch (lit) {
            .int => |v| v,
            .float => |v| @as(i64, @intFromFloat(v)),
            .string => return error.TypeMismatch,
        }),
        .int64, .timestamp, .timestamptz => dst.i64s[row] = switch (lit) {
            .int => |v| v,
            .float => |v| @intFromFloat(v),
            .string => return error.TypeMismatch,
        },
        .float32 => dst.f32s[row] = switch (lit) {
            .int => |v| @floatFromInt(v),
            .float => |v| @floatCast(v),
            .string => return error.TypeMismatch,
        },
        .float64 => dst.f64s[row] = switch (lit) {
            .int => |v| @floatFromInt(v),
            .float => |v| v,
            .string => return error.TypeMismatch,
        },
        else => return error.TypeMismatch,
    }
}

const CoalescePick = enum { src, other, lit, none };

fn coalesceUtf8(
    allocator: std.mem.Allocator,
    src: Column,
    other: ?Column,
    lit: ?sql.Literal,
    picks: []const CoalescePick,
) !batch_mod.Utf8 {
    const lit_s: []const u8 = if (lit) |l| switch (l) {
        .string => |s| s,
        else => return error.TypeMismatch,
    } else "";
    var nbytes: usize = 0;
    for (picks, 0..) |p, i| {
        nbytes += switch (p) {
            .src => src.strAt(i).len,
            .other => other.?.strAt(i).len,
            .lit => lit_s.len,
            .none => 0,
        };
    }
    const bytes = try allocator.alloc(u8, nbytes);
    const offsets = try allocator.alloc(u32, picks.len + 1);
    var off: u32 = 0;
    for (picks, 0..) |p, i| {
        offsets[i] = off;
        const s = switch (p) {
            .src => src.strAt(i),
            .other => other.?.strAt(i),
            .lit => lit_s,
            .none => "",
        };
        if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
        off += @intCast(s.len);
    }
    offsets[picks.len] = off;
    return .{ .offsets = offsets, .bytes = bytes };
}
