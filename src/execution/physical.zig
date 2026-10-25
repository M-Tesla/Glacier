//! Scan / filter / aggregate / having / window / project / scalars / CASE / distinct / order / offset / limit.
//! JOIN is a hash join of two scans (INNER / LEFT / RIGHT / FULL). `COUNT(*)` after
//! JOIN counts key frequencies (no gathered match rows). Window: OVER + ROWS/RANGE + LAG/LEAD.
//! UNION is concatenated in the session so each arm can scan its own FROM.
//! Expr: arithmetic, col vs col, nested scalar calls (abs/round/cast/coalesce/lower/upper/length/trim/replace/substr/concat/left/right/date_trunc/extract/year/month/day/hour/ceil/greatest).
//! Aggregates: COUNT/SUM/AVG/MIN/MAX(DISTINCT col). GROUP BY may emit first-row exprs.

const std = @import("std");
const builtin = @import("builtin");
const parquet = @import("../formats/parquet_wrap.zig");
const avro = @import("../formats/avro_wrap.zig");
const native = @import("../formats/native.zig");
const sql = @import("../sql/parser.zig");
const batch_mod = @import("batch.zig");
const iceberg = @import("../table/iceberg.zig");
const vfs = @import("../vfs/source.zig");
const spill = @import("../vfs/spill.zig");
const aws = @import("../kernel/aws.zig");

const Batch = batch_mod.Batch;
const Column = batch_mod.Column;
const DataType = batch_mod.DataType;

pub const ScanStats = struct {
    files_opened: usize = 0,
    files_pruned: usize = 0,
    row_groups_pruned: usize = 0,
};

/// Bound for the statement currently running on this thread.
pub const CancelState = struct {
    flag: std.atomic.Value(u8) = .init(0),
    /// When non-zero, the Nth `checkCancel` of this statement sets the flag.
    after: u32 = 0,
    checks: u32 = 0,
    /// Awake-clock nanoseconds. 0 means no deadline.
    deadline_ns: i128 = 0,
    io: std.Io = undefined,
    has_io: bool = false,
};

threadlocal var bound_cancel: ?*CancelState = null;
threadlocal var bound_quiet: bool = false;

pub fn bindCancel(state: ?*CancelState) void {
    bound_cancel = state;
    if (state == null) bound_quiet = false;
}

pub fn checkCancel() !void {
    const st = bound_cancel orelse return;
    if (!bound_quiet) {
        st.checks += 1;
        if (st.after != 0 and st.checks >= st.after) st.flag.store(1, .release);
        if (st.deadline_ns != 0 and st.has_io) {
            const now = std.Io.Clock.Timestamp.now(st.io, .awake).raw.toNanoseconds();
            if (now >= st.deadline_ns) st.flag.store(1, .release);
        }
    }
    if (st.flag.load(.acquire) != 0) return error.QueryCanceled;
}

pub fn execute(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
    right_files: ?[]const iceberg.DataFile,
) !Batch {
    if (query.isNoFrom()) {
        var batch = if (query.isLiteralOnly())
            try batchFromLiterals(allocator, query, 1)
        else
            try projectItems(allocator, .{ .columns = &.{}, .len = 1 }, query);
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
    if (files.len == 0) {
        const fields = fallback_schema orelse return error.TableNotFound;
        return executeOnBatch(allocator, try emptyFromSchema(allocator, fields), query);
    }

    if (canStreamQuery(query) and filesAreStreamable(files) and !filesHaveDeletes(files)) {
        return executeStream(allocator, t, files, query, fallback_schema, stats);
    }
    if (filesAreStreamable(files) and !filesHaveDeletes(files) and
        (!query.hasWindow() or (query.order_by.len > 0 and !query.needsAgg())))
    {
        return executeCapped(allocator, t, files, query, fallback_schema, stats);
    }

    var parts: std.ArrayList(Batch) = .empty;
    const wanted = try wantedForScan(allocator, query, files);
    if (try parallelFinish(allocator, t, files, query, wanted, fallback_schema, stats, &parts)) {
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
    for (files) |file| {
        try checkCancel();
        if (query.where) |expr| {
            if (iceberg.canSkipFile(file, expr)) {
                if (stats) |s| s.files_pruned += 1;
                continue;
            }
        }
        if (stats) |s| s.files_opened += 1;
        const scanned_file = switch (file.format) {
            .parquet => try scanParquet(allocator, t, file, wanted, query.where, stats),
            .avro => try scanAvro(allocator, t, file),
            .glacier => try scanNativeAll(allocator, t, file, wanted, query.where),
        };
        try parts.append(allocator, try finishScan(allocator, scanned_file, file, fallback_schema, wanted));
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

fn filesHaveDeletes(files: []const iceberg.DataFile) bool {
    for (files) |f| {
        if (f.deleted_pos.len > 0 or f.eq_deletes.len > 0) return true;
    }
    return false;
}

fn filesHaveEqDeletes(files: []const iceberg.DataFile) bool {
    for (files) |f| {
        if (f.eq_deletes.len > 0) return true;
    }
    return false;
}

/// Equality deletes need the matching columns even when the query is COUNT(*).
fn wantedForScan(
    allocator: std.mem.Allocator,
    query: sql.Query,
    files: []const iceberg.DataFile,
) !?[]const []const u8 {
    const wanted = try collectScanColumns(allocator, query);
    if (!filesHaveEqDeletes(files)) return wanted;
    if (wanted == null) return null;
    if (wanted.?.len == 0) return null;
    var names: std.ArrayList([]const u8) = .empty;
    try names.appendSlice(allocator, wanted.?);
    for (files) |f| {
        for (f.eq_deletes) |eq| {
            for (eq.cols) |c| try addScanName(&names, allocator, c.name);
        }
    }
    return names.items;
}

fn finishScan(
    allocator: std.mem.Allocator,
    batch: Batch,
    file: iceberg.DataFile,
    schema: ?[]const iceberg.SchemaField,
    wanted: ?[]const []const u8,
) !Batch {
    var out = try applyDeletes(allocator, batch, file);
    if (schema) |fields| out = try alignToSchema(allocator, out, fields, wanted);
    return out;
}

fn applyDeletes(allocator: std.mem.Allocator, input: Batch, file: iceberg.DataFile) !Batch {
    if (file.deleted_pos.len == 0 and file.eq_deletes.len == 0) return input;
    const keep = try allocator.alloc(bool, input.len);
    @memset(keep, true);
    for (file.deleted_pos) |p| {
        if (p < input.len) keep[@intCast(p)] = false;
    }
    if (file.eq_deletes.len > 0) {
        var row: usize = 0;
        while (row < input.len) : (row += 1) {
            if (!keep[row]) continue;
            for (file.eq_deletes) |eq| {
                if (rowMatchesEq(input, row, eq)) {
                    keep[row] = false;
                    break;
                }
            }
        }
    }
    var n_keep: usize = 0;
    for (keep) |k| {
        if (k) n_keep += 1;
    }
    if (n_keep == input.len) return input;
    const columns = try allocator.alloc(Column, input.columns.len);
    for (input.columns, 0..) |src, ci| {
        columns[ci] = try compactColumn(allocator, src, keep, n_keep);
    }
    return .{ .columns = columns, .len = n_keep };
}

fn rowMatchesEq(input: Batch, row: usize, eq: iceberg.EqDelete) bool {
    if (eq.len == 0 or eq.cols.len == 0) return false;
    var ei: usize = 0;
    while (ei < eq.len) : (ei += 1) {
        var ok = true;
        for (eq.cols) |ecol| {
            const idx = input.lookup(ecol.name) catch {
                ok = false;
                break;
            };
            const col = input.columns[idx];
            if (col.isNull(row)) {
                ok = false;
                break;
            }
            if (ecol.i64s.len > ei) {
                const got: i64 = switch (col.data_type) {
                    .int32 => col.i32s[row],
                    .int64, .timestamp, .timestamptz => col.i64s[row],
                    else => {
                        ok = false;
                        break;
                    },
                };
                if (got != ecol.i64s[ei]) {
                    ok = false;
                    break;
                }
            } else if (ecol.strs.len > ei) {
                if (col.data_type != .utf8 or !std.mem.eql(u8, col.strAt(row), ecol.strs[ei])) {
                    ok = false;
                    break;
                }
            } else {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }
    return false;
}

fn alignToSchema(
    allocator: std.mem.Allocator,
    input: Batch,
    fields: []const iceberg.SchemaField,
    wanted: ?[]const []const u8,
) !Batch {
    if (fields.len == 0) return input;
    const use = if (wanted) |names| blk: {
        if (names.len == 0) return input;
        var picked: std.ArrayList(iceberg.SchemaField) = .empty;
        for (fields) |f| {
            for (names) |n| {
                if (std.ascii.eqlIgnoreCase(f.name, n)) {
                    try picked.append(allocator, f);
                    break;
                }
            }
        }
        if (picked.items.len == 0) return input;
        break :blk picked.items;
    } else fields;
    const columns = try allocator.alloc(Column, use.len);
    for (use, 0..) |f, i| {
        const want = try icebergType(f);
        if (input.columnIndex(f.name)) |idx| {
            columns[i] = try promoteColumn(allocator, input.columns[idx], want);
            columns[i].name = f.name;
        } else {
            if (f.required) return error.ColumnNotFound;
            columns[i] = try nullColumn(allocator, f.name, want, input.len, f);
        }
    }
    return .{ .columns = columns, .len = input.len };
}

fn nullColumn(
    allocator: std.mem.Allocator,
    name: []const u8,
    dt: DataType,
    n: usize,
    f: iceberg.SchemaField,
) !Column {
    var col: Column = .{
        .name = name,
        .data_type = dt,
        .len = n,
        .decimal_precision = f.decimal_precision,
        .decimal_scale = f.decimal_scale,
    };
    try allocTyped(allocator, &col, n);
    if (dt == .utf8) {
        col.utf8.offsets = try allocator.alloc(u32, n + 1);
        @memset(col.utf8.offsets, 0);
        col.utf8.bytes = &.{};
    }
    if (n > 0) {
        col.valid = try allocator.alloc(u8, n);
        @memset(col.valid, 0);
    }
    return col;
}

fn promoteColumn(allocator: std.mem.Allocator, src: Column, want: DataType) !Column {
    if (src.data_type == want) return src;
    if (src.data_type == .int32 and want == .int64) {
        const i64s = try allocator.alloc(i64, src.len);
        for (src.i32s, 0..) |v, i| i64s[i] = v;
        var dst = src;
        dst.data_type = .int64;
        dst.i64s = i64s;
        dst.i32s = &.{};
        dst.valid = try dupeValid(allocator, src);
        return dst;
    }
    if (src.data_type == .float32 and want == .float64) {
        const f64s = try allocator.alloc(f64, src.len);
        for (src.f32s, 0..) |v, i| f64s[i] = v;
        var dst = src;
        dst.data_type = .float64;
        dst.f64s = f64s;
        dst.f32s = &.{};
        dst.valid = try dupeValid(allocator, src);
        return dst;
    }
    if ((src.data_type == .int32 or src.data_type == .int64) and want == .float64) {
        const f64s = try allocator.alloc(f64, src.len);
        var i: usize = 0;
        while (i < src.len) : (i += 1) {
            const v: i64 = if (src.data_type == .int32) src.i32s[i] else src.i64s[i];
            f64s[i] = @floatFromInt(v);
        }
        var dst = src;
        dst.data_type = .float64;
        dst.f64s = f64s;
        dst.valid = try dupeValid(allocator, src);
        return dst;
    }
    return error.TypeMismatch;
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
    if (query.join == null) return error.InvalidSyntax;
    if (left_files.len == 0 or right_files.len == 0) return error.TableNotFound;

    const left = try executeNoJoin(allocator, t, left_files, scanAllQuery(query.from), null, stats);
    const right = try executeNoJoin(allocator, t, right_files, scanAllQuery(query.join.?.table), null, stats);
    return joinAndTail(allocator, left, right, query);
}

pub fn joinAndTail(allocator: std.mem.Allocator, left: Batch, right: Batch, query: sql.Query) !Batch {
    const j = query.join orelse return error.InvalidSyntax;
    const lq = tableQual(query.from, query.from_alias, "left");
    const rq = tableQual(j.table, j.alias, "right");
    if (std.ascii.eqlIgnoreCase(lq, rq)) return error.InvalidSyntax;
    const shared = if (j.natural) try naturalNames(allocator, left, right) else j.using;
    const eqs = if (shared.len > 0) try usingEqs(allocator, lq, rq, shared) else j.eqs;
    const keys: JoinKeys = if (eqs.len == 0)
        .{ .left = &.{}, .right = &.{} }
    else
        try resolveJoinKeys(allocator, left, right, lq, rq, eqs);
    if (isJoinCountStar(query)) {
        const n = try hashJoinCount(allocator, left, right, keys.left, keys.right, j.kind);
        const counted = try countStarBatch(allocator, n, query);
        return applyTail(allocator, counted, query, .{ .agg_done = true });
    }
    const joined = try hashJoin(allocator, left, right, keys.left, keys.right, lq, rq, j.kind);
    const shaped = if (shared.len == 0) joined else try shapeUsing(allocator, joined, lq, rq, shared);
    return applyTail(allocator, shaped, query, .{});
}

fn naturalNames(allocator: std.mem.Allocator, left: Batch, right: Batch) ![]const []const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    errdefer names.deinit(allocator);
    for (left.columns) |col| {
        const tail = columnTail(col.name);
        var hit = false;
        for (right.columns) |other| {
            if (std.ascii.eqlIgnoreCase(columnTail(other.name), tail)) hit = true;
        }
        if (!hit) continue;
        var seen = false;
        for (names.items) |prev| {
            if (std.ascii.eqlIgnoreCase(prev, tail)) seen = true;
        }
        if (!seen) try names.append(allocator, tail);
    }
    return names.toOwnedSlice(allocator);
}

fn usingEqs(
    allocator: std.mem.Allocator,
    lq: []const u8,
    rq: []const u8,
    names: []const []const u8,
) ![]const sql.JoinEq {
    const eqs = try allocator.alloc(sql.JoinEq, names.len);
    for (names, 0..) |name, i| {
        eqs[i] = .{
            .left = .{ .qualifier = lq, .name = name },
            .right = .{ .qualifier = rq, .name = name },
        };
    }
    return eqs;
}

fn shapeUsing(
    allocator: std.mem.Allocator,
    joined: Batch,
    lq: []const u8,
    rq: []const u8,
    names: []const []const u8,
) !Batch {
    var n_keep: usize = names.len;
    for (joined.columns) |col| {
        if (qualMatches(col.name, lq) or qualMatches(col.name, rq)) {
            if (!listedName(columnTail(col.name), names)) n_keep += 1;
        }
    }
    const columns = try allocator.alloc(Column, n_keep);
    var i: usize = 0;
    for (names) |name| {
        const li = try qualIndex(joined, lq, name);
        const ri = try qualIndex(joined, rq, name);
        columns[i] = try coalesceColumns(allocator, joined.columns[li], joined.columns[ri], columnTail(joined.columns[li].name));
        i += 1;
    }
    for (joined.columns) |col| {
        if (!qualMatches(col.name, lq)) continue;
        if (listedName(columnTail(col.name), names)) continue;
        columns[i] = col;
        i += 1;
    }
    for (joined.columns) |col| {
        if (!qualMatches(col.name, rq)) continue;
        if (listedName(columnTail(col.name), names)) continue;
        columns[i] = col;
        i += 1;
    }
    return .{ .columns = columns, .len = joined.len };
}

fn qualMatches(name: []const u8, qual: []const u8) bool {
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return false;
    return std.ascii.eqlIgnoreCase(name[0..dot], qual);
}

fn columnTail(name: []const u8) []const u8 {
    const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return name;
    return name[dot + 1 ..];
}

fn listedName(name: []const u8, names: []const []const u8) bool {
    for (names) |n| {
        if (std.ascii.eqlIgnoreCase(n, name)) return true;
    }
    return false;
}

fn qualIndex(batch: Batch, qual: []const u8, name: []const u8) !usize {
    for (batch.columns, 0..) |col, i| {
        if (!qualMatches(col.name, qual)) continue;
        if (std.ascii.eqlIgnoreCase(columnTail(col.name), name)) return i;
    }
    return error.ColumnNotFound;
}

fn coalesceColumns(allocator: std.mem.Allocator, left: Column, right: Column, name: []const u8) !Column {
    if (left.data_type != right.data_type or left.len != right.len) return error.TypeMismatch;
    var dst: Column = .{
        .name = name,
        .data_type = left.data_type,
        .len = left.len,
        .decimal_precision = left.decimal_precision,
        .decimal_scale = left.decimal_scale,
    };
    const picks = try allocator.alloc(CoalescePick, left.len);
    defer allocator.free(picks);
    var any_null = false;
    for (picks, 0..) |*p, i| {
        const ln = left.isNull(i);
        const rn = right.isNull(i);
        if (ln and rn) {
            p.* = .none;
            any_null = true;
        } else if (ln) {
            p.* = .other;
        } else {
            p.* = .src;
        }
    }
    if (left.data_type == .utf8) {
        dst.utf8 = try coalesceUtf8(allocator, left, right, null, picks);
    } else {
        try allocTyped(allocator, &dst, left.len);
        for (picks, 0..) |p, i| switch (p) {
            .src => copyTypedCell(&dst, i, left, i),
            .other => copyTypedCell(&dst, i, right, i),
            .none, .lit => {},
        };
    }
    if (any_null) {
        dst.valid = try allocator.alloc(u8, left.len);
        for (picks, 0..) |p, i| dst.valid[i] = if (p == .none) 0 else 1;
    }
    return dst;
}

pub fn executeOnBatch(allocator: std.mem.Allocator, input: Batch, query: sql.Query) !Batch {
    if (query.having != null and !query.needsAgg()) return error.InvalidSyntax;
    return applyTail(allocator, input, query, .{});
}

pub fn rowMatches(allocator: std.mem.Allocator, input: Batch, row: usize, expr: *const sql.BoolExpr, query: ?sql.Query) !bool {
    return evalBool(allocator, input, row, expr, query);
}

pub fn takeRows(allocator: std.mem.Allocator, input: Batch, keep: []const bool) !Batch {
    var n_keep: usize = 0;
    for (keep) |k| if (k) {
        n_keep += 1;
    };
    if (n_keep == input.len) return input;
    const columns = try allocator.alloc(Column, input.columns.len);
    for (input.columns, 0..) |src, ci| {
        columns[ci] = try compactColumn(allocator, src, keep, n_keep);
    }
    return .{ .columns = columns, .len = n_keep };
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

const JoinRef = struct { idx: usize, present: bool };

fn hashJoin(
    allocator: std.mem.Allocator,
    left: Batch,
    right: Batch,
    left_idxs: []const usize,
    right_idxs: []const usize,
    lq: []const u8,
    rq: []const u8,
    kind: sql.JoinKind,
) !Batch {
    var left_rows: std.ArrayList(JoinRef) = .empty;
    var right_rows: std.ArrayList(JoinRef) = .empty;

    switch (kind) {
        .inner, .left, .full => {
            var map: std.StringArrayHashMapUnmanaged(std.ArrayList(usize)) = .empty;
            var used: []u8 = &.{};
            if (kind == .full and right.len > 0) {
                used = try allocator.alloc(u8, right.len);
                @memset(used, 0);
            }
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
            row = 0;
            while (row < left.len) : (row += 1) {
                const unmatched = rowHasNullKey(left, row, left_idxs);
                if (unmatched) {
                    if (kind == .inner) continue;
                    try left_rows.append(allocator, .{ .idx = row, .present = true });
                    try right_rows.append(allocator, .{ .idx = 0, .present = false });
                    continue;
                }
                const key = try encodeKey(allocator, left, row, left_idxs);
                defer allocator.free(key);
                if (map.get(key)) |list| {
                    for (list.items) |rrow| {
                        try left_rows.append(allocator, .{ .idx = row, .present = true });
                        try right_rows.append(allocator, .{ .idx = rrow, .present = true });
                        if (used.len > 0) used[rrow] = 1;
                    }
                } else if (kind != .inner) {
                    try left_rows.append(allocator, .{ .idx = row, .present = true });
                    try right_rows.append(allocator, .{ .idx = 0, .present = false });
                }
            }
            if (kind == .full) {
                row = 0;
                while (row < right.len) : (row += 1) {
                    if (used.len > 0 and used[row] != 0) continue;
                    try left_rows.append(allocator, .{ .idx = 0, .present = false });
                    try right_rows.append(allocator, .{ .idx = row, .present = true });
                }
            }
        },
        .right => {
            var map: std.StringArrayHashMapUnmanaged(std.ArrayList(usize)) = .empty;
            var row: usize = 0;
            while (row < left.len) : (row += 1) {
                if (rowHasNullKey(left, row, left_idxs)) continue;
                const key = try encodeKey(allocator, left, row, left_idxs);
                if (map.getPtr(key)) |list| {
                    try list.append(allocator, row);
                    allocator.free(key);
                } else {
                    var list: std.ArrayList(usize) = .empty;
                    try list.append(allocator, row);
                    try map.put(allocator, key, list);
                }
            }
            row = 0;
            while (row < right.len) : (row += 1) {
                if (rowHasNullKey(right, row, right_idxs)) {
                    try left_rows.append(allocator, .{ .idx = 0, .present = false });
                    try right_rows.append(allocator, .{ .idx = row, .present = true });
                    continue;
                }
                const key = try encodeKey(allocator, right, row, right_idxs);
                defer allocator.free(key);
                if (map.get(key)) |list| {
                    for (list.items) |lrow| {
                        try left_rows.append(allocator, .{ .idx = lrow, .present = true });
                        try right_rows.append(allocator, .{ .idx = row, .present = true });
                    }
                } else {
                    try left_rows.append(allocator, .{ .idx = 0, .present = false });
                    try right_rows.append(allocator, .{ .idx = row, .present = true });
                }
            }
        },
    }

    const l_g = try gatherRowsMaybe(allocator, left, left_rows.items);
    const r_g = try gatherRowsMaybe(allocator, right, right_rows.items);
    const l_q = try qualifyBatch(allocator, l_g, lq);
    const r_q = try qualifyBatch(allocator, r_g, rq);
    return hconcat(allocator, l_q, r_q);
}

fn isJoinCountStar(query: sql.Query) bool {
    if (query.where != null or query.having != null) return false;
    if (query.group_by.len != 0 or query.grouping_sets.len != 0) return false;
    if (query.distinct or query.hasWindow()) return false;
    if (query.items.len != 1) return false;
    const agg = switch (query.items[0]) {
        .agg => |a| a,
        else => return false,
    };
    return agg.kind == .count and !agg.distinct and agg.arg == null;
}

fn countStarBatch(allocator: std.mem.Allocator, n: i64, query: sql.Query) !Batch {
    const name = try aggName(allocator, query.items[0].agg);
    const i64s = try allocator.alloc(i64, 1);
    i64s[0] = n;
    const columns = try allocator.alloc(Column, 1);
    columns[0] = .{ .name = name, .data_type = .int64, .len = 1, .i64s = i64s };
    return .{ .columns = columns, .len = 1 };
}

const KeyCounts = struct {
    map: std.StringArrayHashMapUnmanaged(u64),
    nulls: u64,
};

fn keyCounts(
    allocator: std.mem.Allocator,
    batch: Batch,
    idxs: []const usize,
) !KeyCounts {
    var map: std.StringArrayHashMapUnmanaged(u64) = .empty;
    var nulls: u64 = 0;
    var row: usize = 0;
    while (row < batch.len) : (row += 1) {
        if (rowHasNullKey(batch, row, idxs)) {
            nulls += 1;
            continue;
        }
        const key = try encodeKey(allocator, batch, row, idxs);
        if (map.getPtr(key)) |n| {
            n.* += 1;
            allocator.free(key);
        } else {
            try map.put(allocator, key, 1);
        }
    }
    return .{ .map = map, .nulls = nulls };
}

fn addProduct(n: *u64, a: u64, b: u64) !void {
    const prod = try std.math.mul(u64, a, b);
    n.* = try std.math.add(u64, n.*, prod);
}

fn hashJoinCount(
    allocator: std.mem.Allocator,
    left: Batch,
    right: Batch,
    left_idxs: []const usize,
    right_idxs: []const usize,
    kind: sql.JoinKind,
) !i64 {
    const left_c = try keyCounts(allocator, left, left_idxs);
    const right_c = try keyCounts(allocator, right, right_idxs);
    var n: u64 = 0;
    switch (kind) {
        .inner => {
            var it = left_c.map.iterator();
            while (it.next()) |kv| {
                const r = right_c.map.get(kv.key_ptr.*) orelse 0;
                try addProduct(&n, kv.value_ptr.*, r);
            }
        },
        .left => {
            n = left_c.nulls;
            var it = left_c.map.iterator();
            while (it.next()) |kv| {
                const r = right_c.map.get(kv.key_ptr.*) orelse 0;
                if (r == 0) {
                    n = try std.math.add(u64, n, kv.value_ptr.*);
                } else {
                    try addProduct(&n, kv.value_ptr.*, r);
                }
            }
        },
        .right => {
            n = right_c.nulls;
            var it = right_c.map.iterator();
            while (it.next()) |kv| {
                const l = left_c.map.get(kv.key_ptr.*) orelse 0;
                if (l == 0) {
                    n = try std.math.add(u64, n, kv.value_ptr.*);
                } else {
                    try addProduct(&n, l, kv.value_ptr.*);
                }
            }
        },
        .full => {
            n = try std.math.add(u64, left_c.nulls, right_c.nulls);
            var lit = left_c.map.iterator();
            while (lit.next()) |kv| {
                const r = right_c.map.get(kv.key_ptr.*) orelse 0;
                if (r == 0) {
                    n = try std.math.add(u64, n, kv.value_ptr.*);
                } else {
                    try addProduct(&n, kv.value_ptr.*, r);
                }
            }
            var rit = right_c.map.iterator();
            while (rit.next()) |kv| {
                if (left_c.map.contains(kv.key_ptr.*)) continue;
                n = try std.math.add(u64, n, kv.value_ptr.*);
            }
        },
    }
    return std.math.cast(i64, n) orelse error.Overflow;
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
    if (query.needsAgg() and !t.agg_done) {
        batch = try aggregateBatch(allocator, batch, query);
        if (query.having) |expr| {
            batch = try filterExpr(allocator, batch, expr, query);
        }
    }
    const push_win = canPushLimitBeforeWindow(query);
    if (push_win) {
        if (query.order_by.len > 0 and !t.sort_done) {
            batch = try sortBatch(allocator, batch, query.order_by);
        }
        if (!t.range_done) {
            const keep = (query.offset orelse 0) + (query.limit orelse 0);
            batch = try takeRange(allocator, batch, 0, keep);
        }
        if (query.hasWindow()) {
            batch = try applyWindows(allocator, batch, query);
        }
        if ((!query.needsAgg() and !query.isStar()) or (query.needsAgg() and query.hasWindow())) {
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
    if (query.hasWindow()) {
        batch = try applyWindows(allocator, batch, query);
    }
    if (query.order_by.len > 0 and !t.sort_done) {
        batch = try sortBatch(allocator, batch, query.order_by);
    }
    if ((!query.needsAgg() and !query.isStar()) or (query.needsAgg() and query.hasWindow())) {
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

fn ordersEqual(a: []const sql.OrderBy, b: []const sql.OrderBy) bool {
    if (a.len != b.len) return false;
    for (a, b) |l, r| {
        if (!std.ascii.eqlIgnoreCase(l.column, r.column)) return false;
        if (l.desc != r.desc) return false;
    }
    return true;
}

fn boundLooksForward(b: sql.FrameBound) bool {
    return switch (b) {
        .unbounded_following, .following => true,
        else => false,
    };
}

fn windowPrefixPushable(w: sql.Window, order: []const sql.OrderBy) bool {
    if (w.kind == .lead) return false;
    if (!ordersEqual(w.spec.order_by, order)) return false;
    switch (w.kind) {
        .lead => return false,
        .row_number, .rank, .dense_rank, .lag => return true,
        .count, .sum, .avg, .min, .max => {
            const frame = w.spec.frame orelse return false;
            if (frame.unit != .rows) return false;
            return !boundLooksForward(frame.start) and !boundLooksForward(frame.end);
        },
    }
}

fn canPushLimitBeforeWindow(query: sql.Query) bool {
    if (query.limit == null) return false;
    if (query.order_by.len == 0) return false;
    if (query.needsAgg() or query.distinct) return false;
    if (!query.hasWindow()) return false;
    for (query.items) |item| {
        switch (item) {
            .window => |w| if (!windowPrefixPushable(w, query.order_by)) return false,
            else => {},
        }
    }
    return true;
}

fn frameIsRunningRows(frame: sql.WindowFrame) bool {
    return frame.unit == .rows and frame.start == .unbounded_preceding and frame.end == .current_row;
}

fn frameIsRunningRange(frame: sql.WindowFrame) bool {
    return frame.unit == .range and frame.start == .unbounded_preceding and frame.end == .current_row;
}

fn frameIsWholePartition(frame: sql.WindowFrame) bool {
    return frame.start == .unbounded_preceding and frame.end == .unbounded_following;
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
            var w = item.window;
            if (w.spec.ref) |name| {
                var found = false;
                for (query.named_windows) |nw| {
                    if (std.ascii.eqlIgnoreCase(nw.name, name)) {
                        w.spec = nw.spec;
                        found = true;
                        break;
                    }
                }
                if (!found) return error.InvalidSyntax;
            }
            columns[i] = try evalWindow(allocator, input, w);
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
        .lag => "lag",
        .lead => "lead",
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
        .row_number, .rank, .dense_rank, .lag, .lead => error.InvalidSyntax,
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

const FrameSpan = struct {
    start: usize = 0,
    end: usize = 0,
    empty: bool = false,
};

fn boundHasOffset(b: sql.FrameBound) bool {
    return switch (b) {
        .preceding, .following => true,
        else => false,
    };
}

fn peerStart(input: Batch, g: []const usize, i: usize, keys: []const sql.OrderBy) usize {
    var s = i;
    while (s > 0 and orderKeysEqual(input, keys, g[s], g[s - 1])) s -= 1;
    return s;
}

fn peerEnd(input: Batch, g: []const usize, i: usize, keys: []const sql.OrderBy) usize {
    var e = i;
    while (e + 1 < g.len and orderKeysEqual(input, keys, g[e], g[e + 1])) e += 1;
    return e;
}

fn rowsBoundRaw(len: usize, i: usize, bound: sql.FrameBound) isize {
    const ii: isize = @intCast(i);
    const last: isize = if (len == 0) -1 else @intCast(len - 1);
    return switch (bound) {
        .unbounded_preceding => 0,
        .unbounded_following => last,
        .current_row => ii,
        .preceding => |n| ii - @as(isize, @intCast(n)),
        .following => |n| ii + @as(isize, @intCast(n)),
    };
}

fn rowsFrameSpan(len: usize, i: usize, frame: sql.WindowFrame) FrameSpan {
    if (len == 0) return .{ .empty = true };
    const last: isize = @intCast(len - 1);
    const start_raw = rowsBoundRaw(len, i, frame.start);
    const end_raw = rowsBoundRaw(len, i, frame.end);
    if (start_raw > last or end_raw < 0) return .{ .empty = true };
    const start: usize = @intCast(@max(start_raw, 0));
    const end: usize = @intCast(@min(end_raw, last));
    if (start > end) return .{ .empty = true };
    return .{ .start = start, .end = end };
}

fn orderedF64(col: Column, row: usize, desc: bool) !f64 {
    const v = try cellAsF64(col, row);
    return if (desc) -v else v;
}

fn rangeEdgeValue(bound: sql.FrameBound, curr: f64) f64 {
    return switch (bound) {
        .unbounded_preceding => -std.math.inf(f64),
        .unbounded_following => std.math.inf(f64),
        .current_row => curr,
        .preceding => |n| curr - @as(f64, @floatFromInt(n)),
        .following => |n| curr + @as(f64, @floatFromInt(n)),
    };
}

fn rangeValueSpan(
    input: Batch,
    g: []const usize,
    i: usize,
    ob: sql.OrderBy,
    frame: sql.WindowFrame,
) !FrameSpan {
    const col = input.columns[try input.lookup(ob.column)];
    const orig = g[i];
    if (col.isNull(orig)) {
        const keys = [_]sql.OrderBy{ob};
        return .{
            .start = peerStart(input, g, i, &keys),
            .end = peerEnd(input, g, i, &keys),
        };
    }
    const curr = try orderedF64(col, orig, ob.desc);
    const lo = rangeEdgeValue(frame.start, curr);
    const hi = rangeEdgeValue(frame.end, curr);
    var start: ?usize = null;
    var end: usize = 0;
    for (g, 0..) |row, j| {
        if (col.isNull(row)) continue;
        const v = try orderedF64(col, row, ob.desc);
        if (v < lo or v > hi) continue;
        if (start == null) start = j;
        end = j;
    }
    if (start) |s| return .{ .start = s, .end = end };
    return .{ .empty = true };
}

fn rangeFrameSpan(
    input: Batch,
    g: []const usize,
    i: usize,
    spec: sql.WindowSpec,
    frame: sql.WindowFrame,
) !FrameSpan {
    if (g.len == 0) return .{ .empty = true };
    const keys = spec.order_by;
    if (boundHasOffset(frame.start) or boundHasOffset(frame.end)) {
        if (keys.len != 1) return error.UnsupportedSql;
        return rangeValueSpan(input, g, i, keys[0], frame);
    }
    const start: usize = switch (frame.start) {
        .unbounded_preceding => 0,
        .unbounded_following => g.len,
        .current_row => peerStart(input, g, i, keys),
        .preceding, .following => unreachable,
    };
    const end: usize = switch (frame.end) {
        .unbounded_following => g.len - 1,
        .unbounded_preceding => 0,
        .current_row => peerEnd(input, g, i, keys),
        .preceding, .following => unreachable,
    };
    if (start >= g.len or start > end) return .{ .empty = true };
    return .{ .start = start, .end = end };
}

fn windowFrameSpan(
    input: Batch,
    g: []const usize,
    i: usize,
    spec: sql.WindowSpec,
    frame: sql.WindowFrame,
) !FrameSpan {
    return switch (frame.unit) {
        .rows => rowsFrameSpan(g.len, i, frame),
        .range => rangeFrameSpan(input, g, i, spec, frame),
        .groups => groupsFrameSpan(input, g, i, spec, frame),
    };
}

fn groupsFrameSpan(
    input: Batch,
    g: []const usize,
    i: usize,
    spec: sql.WindowSpec,
    frame: sql.WindowFrame,
) !FrameSpan {
    if (g.len == 0) return .{ .empty = true };
    var n_groups: usize = 0;
    var cur: usize = 0;
    var row: usize = 0;
    while (row < g.len) {
        const pe = peerEnd(input, g, row, spec.order_by);
        if (i >= row and i <= pe) cur = n_groups;
        n_groups += 1;
        row = pe + 1;
    }
    const start_g = rowsBoundRaw(n_groups, cur, frame.start);
    const end_g = rowsBoundRaw(n_groups, cur, frame.end);
    const last_g: isize = if (n_groups == 0) -1 else @intCast(n_groups - 1);
    if (start_g > last_g or end_g < 0) return .{ .empty = true };
    const gs: usize = @intCast(@max(start_g, 0));
    const ge: usize = @intCast(@min(end_g, last_g));
    var gi: usize = 0;
    row = 0;
    var start: usize = 0;
    var end: usize = 0;
    while (row < g.len) {
        const pe = peerEnd(input, g, row, spec.order_by);
        if (gi == gs) start = row;
        if (gi == ge) {
            end = pe;
            break;
        }
        gi += 1;
        row = pe + 1;
    }
    if (start > end) return .{ .empty = true };
    return .{ .start = start, .end = end };
}

fn writeDefaultOrNull(dst: *Column, row: usize, lit: ?sql.Literal, allocator: std.mem.Allocator) !void {
    if (lit) |l| {
        switch (l) {
            .null => {
                try markNull(dst, row, allocator);
                if (dst.data_type == .utf8) try appendUtf8(dst, "", row, allocator);
            },
            .string => |s| {
                if (dst.data_type != .utf8) return error.TypeMismatch;
                try appendUtf8(dst, s, row, allocator);
            },
            .int, .float => try writeLitCell(dst, row, l),
        }
        return;
    }
    try markNull(dst, row, allocator);
    if (dst.data_type == .utf8) try appendUtf8(dst, "", row, allocator);
}

fn writeSrcWindowCell(dst: *Column, di: usize, src: Column, si: usize, allocator: std.mem.Allocator) !void {
    if (src.isNull(si)) {
        try markNull(dst, di, allocator);
        if (dst.data_type == .utf8) try appendUtf8(dst, "", di, allocator);
        return;
    }
    if (src.data_type == .utf8) {
        try appendUtf8(dst, src.strAt(si), di, allocator);
        return;
    }
    copyTypedCell(dst, di, src, si);
}

fn evalLagLead(allocator: std.mem.Allocator, input: Batch, w: sql.Window, groups: [][]usize) !Column {
    const arg = w.arg orelse return error.InvalidSyntax;
    const src = input.columns[try input.lookup(arg)];
    const n = input.len;
    const name = try windowName(allocator, w);
    var dst: Column = .{
        .name = name,
        .data_type = src.data_type,
        .len = n,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    if (n == 0) return dst;
    try allocTyped(allocator, &dst, n);
    if (dst.data_type == .utf8) {
        dst.utf8.offsets = try allocator.alloc(u32, n + 1);
        dst.utf8.offsets[0] = 0;
        dst.utf8.bytes = &.{};
    }
    const from = try allocator.alloc(?usize, n);
    @memset(from, null);
    const off: usize = @intCast(w.offset);
    for (groups) |g| {
        for (g, 0..) |orig, i| {
            const src_i: ?usize = switch (w.kind) {
                .lag => if (off <= i) g[i - off] else null,
                .lead => if (i + off < g.len) g[i + off] else null,
                else => return error.InvalidSyntax,
            };
            from[orig] = src_i;
        }
    }
    var row: usize = 0;
    while (row < n) : (row += 1) {
        if (from[row]) |si| {
            try writeSrcWindowCell(&dst, row, src, si, allocator);
        } else {
            try writeDefaultOrNull(&dst, row, w.default_lit, allocator);
        }
    }
    return dst;
}

fn evalWindow(allocator: std.mem.Allocator, input: Batch, w: sql.Window) !Column {
    const n = input.len;
    const name = try windowName(allocator, w);
    const groups = try partitionRows(allocator, input, w.spec.partition_by);
    for (groups) |g| try sortPartition(input, g, w.spec.order_by);

    switch (w.kind) {
        .lag, .lead => return evalLagLead(allocator, input, w, groups),
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
            const running_rows = if (w.spec.frame) |frame| frameIsRunningRows(frame) else w.spec.order_by.len > 0;
            const running_range = if (w.spec.frame) |frame| frameIsRunningRange(frame) else false;
            const whole = if (w.spec.frame) |frame| frameIsWholePartition(frame) else w.spec.order_by.len == 0;
            if (running_range) {
                for (groups) |g| {
                    var state = AggState{ .kind = kind };
                    var i: usize = 0;
                    while (i < g.len) {
                        const pe = peerEnd(input, g, i, w.spec.order_by);
                        var k = i;
                        while (k <= pe) : (k += 1) {
                            try feed(&state, src, g[k], allocator);
                        }
                        k = i;
                        while (k <= pe) : (k += 1) {
                            snaps[g[k]] = state;
                        }
                        i = pe + 1;
                    }
                }
            } else if (running_rows) {
                for (groups) |g| {
                    var state = AggState{ .kind = kind };
                    for (g) |orig| {
                        try feed(&state, src, orig, allocator);
                        snaps[orig] = state;
                    }
                }
            } else if (whole) {
                for (groups) |g| {
                    var state = AggState{ .kind = kind };
                    for (g) |orig| try feed(&state, src, orig, allocator);
                    for (g) |orig| snaps[orig] = state;
                }
            } else if (w.spec.frame) |frame| {
                for (groups) |g| {
                    for (g, 0..) |orig, i| {
                        const span = try windowFrameSpan(input, g, i, w.spec, frame);
                        var state = AggState{ .kind = kind };
                        if (!span.empty) {
                            var k = span.start;
                            while (k <= span.end) : (k += 1) {
                                try feed(&state, src, g[k], allocator);
                            }
                        }
                        snaps[orig] = state;
                    }
                }
            } else {
                for (groups) |g| {
                    var state = AggState{ .kind = kind };
                    for (g) |orig| try feed(&state, src, orig, allocator);
                    for (g) |orig| snaps[orig] = state;
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
    return writeGlacier(allocator, t, dest, batch);
}

pub fn writeGlacier(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    dest: []const u8,
    batch: Batch,
) !u64 {
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

pub fn concatUnion(allocator: std.mem.Allocator, left: Batch, right: Batch) !Batch {
    if (left.columns.len != right.columns.len) return error.SchemaMismatch;
    if (left.columns.len == 0) return left;
    for (left.columns, right.columns) |lc, rc| {
        if (lc.data_type != rc.data_type) return error.SchemaMismatch;
    }
    if (left.len == 0) return right;
    if (right.len == 0) return left;
    return concatBatches(allocator, &.{ left, right });
}

pub fn distinctRows(allocator: std.mem.Allocator, input: Batch) !Batch {
    return distinctBatch(allocator, input);
}

pub fn finishUnion(allocator: std.mem.Allocator, input: Batch, query: sql.Query) !Batch {
    var batch = input;
    if (query.order_by.len > 0) {
        batch = try sortBatch(allocator, batch, query.order_by);
    }
    if (query.offset != null or query.limit != null) {
        batch = try takeRange(allocator, batch, query.offset orelse 0, query.limit);
    }
    return batch;
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
        try checkCancel();
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
            var rg_filter: RgFilter = .{};
            defer rg_filter.deinit();
            var br = try beginParquetBatch(reader, picked, batch_size, query.where, null, &rg_filter);
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
            .agg => |agg| blk: {
                const name = try aggName(allocator, agg);
                const idx = try input.lookup(name);
                var col = input.columns[idx];
                if (agg.alias) |alias| col.name = alias;
                break :blk col;
            },
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
            .case => |cs| try evalCase(allocator, input, cs),
            .expr => |ex| try evalComputed(allocator, input, ex, query),
            .grouping => |g| blk: {
                const name = g.alias orelse g.name;
                const idx = try input.lookup(name);
                var col = input.columns[idx];
                col.name = name;
                break :blk col;
            },
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
        .null => {
            const i64s = try allocator.alloc(i64, len);
            if (len > 0) @memset(i64s, 0);
            const valid = try allocator.alloc(u8, len);
            if (len > 0) @memset(valid, 0);
            return .{
                .name = name,
                .data_type = .int64,
                .len = len,
                .i64s = i64s,
                .valid = valid,
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

/// Name of the scan path `execute` would take for this query and these files.
pub fn explainScanMode(query: sql.Query, files: []const iceberg.DataFile) []const u8 {
    if (canStreamQuery(query) and filesAreStreamable(files) and !filesHaveDeletes(files)) return "stream";
    if (filesAreStreamable(files) and !filesHaveDeletes(files) and
        (!query.hasWindow() or (query.order_by.len > 0 and !query.needsAgg())))
    {
        if (query.needsAgg() and query.group_by.len == 0) return "aggregate";
        if (query.needsAgg()) return "hash_aggregate";
        if (query.order_by.len > 0) return "sort";
        if (query.distinct) return "distinct";
    }
    if (canParallelFiles(files, query.where)) return "parallel_scan";
    return "scan";
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

    if (query.needsAgg() and query.group_by.len == 0 and query.grouping_sets.len == 0) {
        return executeGlobalAgg(allocator, t, files, query, fallback_schema, stats, wanted, vec_size);
    }
    if (hashAggFast(query)) {
        return executeHashAgg(allocator, t, files, query, fallback_schema, stats, wanted, vec_size);
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
        .stats = stats,
    };

    for (files) |file| {
        try checkCancel();
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
    stats: ?*ScanStats = null,

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
        if (comptime builtin.cpu.arch != .wasm32 and !builtin.single_threaded) {
            var scratch = std.heap.ArenaAllocator.init(std.heap.smp_allocator);
            defer scratch.deinit();
            if (try parallelParquetScan(scratch.allocator(), t, file, wanted, self.query.where, self.stats, vec_size)) |batch| {
                if (batch.columns.len > 0) try self.ingestVec(batch);
                return;
            }
        }
        var reader = try parquet.openLocation(self.allocator, t, file.path);
        defer reader.close();
        const picked = try pickParquetColumns(self.allocator, reader, wanted);
        if (picked.len == 0) return error.ColumnNotFound;
        if (self.schema_batch == null) {
            const empty = try emptyFromPicked(self.allocator, reader, picked);
            try self.noteSchema(empty);
        }
        var rg_filter: RgFilter = .{};
        defer rg_filter.deinit();
        var br = try beginParquetBatch(reader, picked, vec_size, self.query.where, self.stats, &rg_filter);
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
        try self.noteSchema(vec);
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

    fn noteSchema(self: *CapCtx, vec: Batch) !void {
        if (self.schema_batch == null) {
            self.schema_batch = try emptyFromBatch(self.allocator, vec);
        }
        if (self.metas.len == 0 and vec.columns.len > 0) {
            self.metas = try colMetas(self.allocator, vec);
            self.part_idxs = try self.resolveIdxs(vec);
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
            const p: usize = if (self.query.grouping_sets.len > 0)
                0
            else
                @intCast(std.hash.Wyhash.hash(0, key) % @as(u64, spill.partition_count));
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
                const q = self.query;
                const win_push = q.hasWindow() and canPushLimitBeforeWindow(q);
                const plain_push = !q.distinct and !q.needsAgg() and !q.hasWindow();
                const merge_off: u64 = if (plain_push) q.offset orelse 0 else 0;
                const merge_lim: ?u64 = if (plain_push)
                    q.limit
                else if (win_push)
                    (q.offset orelse 0) + q.limit.?
                else
                    null;
                const merged = try mergeRuns(
                    self.allocator,
                    self.io,
                    self.run_paths.items,
                    q.order_by,
                    merge_off,
                    merge_lim,
                    self.schema_batch,
                );
                return applyTail(self.allocator, merged, q, .{
                    .where_done = true,
                    .sort_done = true,
                    .range_done = plain_push,
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

fn aggIsDistinct(query: sql.Query) bool {
    for (query.items) |item| {
        if (item == .agg and item.agg.distinct) return true;
    }
    return false;
}

fn hashAggFast(query: sql.Query) bool {
    if (query.grouping_sets.len != 0 or query.group_by.len != 1 or query.hasWindow()) return false;
    if (aggIsDistinct(query)) return false;
    for (query.items) |item| {
        switch (item) {
            .agg, .column, .grouping => {},
            else => return false,
        }
    }
    return true;
}

const SimplePred = struct {
    name: []const u8,
    op: sql.CmpOp,
    lit: sql.Literal,
};

fn simplePred(expr: *const sql.BoolExpr) ?SimplePred {
    if (expr.* != .cmp) return null;
    const c = expr.cmp;
    if (sql.asColumn(c.left)) |name| {
        if (sql.asLiteral(c.right)) |lit| return .{ .name = name, .op = c.op, .lit = lit };
    }
    if (sql.asColumn(c.right)) |name| {
        if (sql.asLiteral(c.left)) |lit| return .{ .name = name, .op = flipCmp(c.op), .lit = lit };
    }
    return null;
}

fn colSame(a: []const u8, b: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(a, b)) return true;
    const at = if (std.mem.lastIndexOfScalar(u8, a, '.')) |d| a[d + 1 ..] else a;
    const bt = if (std.mem.lastIndexOfScalar(u8, b, '.')) |d| b[d + 1 ..] else b;
    return std.ascii.eqlIgnoreCase(at, bt);
}

fn arrowIsNull(nulls: ?[*]const u8, i: usize) bool {
    const p = nulls orelse return false;
    const bit: u8 = (p[i >> 3] >> @intCast(i & 7)) & 1;
    return bit == 0;
}

fn bitmapHasNull(nulls: ?[*]const u8, n: usize) bool {
    if (nulls == null or n == 0) return false;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        if (arrowIsNull(nulls, i)) return true;
    }
    return false;
}

fn cmpI64(op: sql.CmpOp, v: i64, lit: i64) bool {
    return switch (op) {
        .eq => v == lit,
        .ne => v != lit,
        .lt => v < lit,
        .le => v <= lit,
        .gt => v > lit,
        .ge => v >= lit,
    };
}

fn cmpF64(op: sql.CmpOp, v: f64, lit: f64) bool {
    return switch (op) {
        .eq => v == lit,
        .ne => v != lit,
        .lt => v < lit,
        .le => v <= lit,
        .gt => v > lit,
        .ge => v >= lit,
    };
}

fn predKeepI64(pred: ?SimplePred, v: i64) bool {
    const p = pred orelse return true;
    const lit = switch (p.lit) {
        .int => |n| n,
        .float => |f| blk: {
            if (f < -(1 << 53) or f > (1 << 53)) return true;
            break :blk @as(i64, @intFromFloat(f));
        },
        else => return true,
    };
    return cmpI64(p.op, v, lit);
}

const NumCol = struct {
    nulls: ?[*]const u8 = null,
    n: usize,
    kind: enum { i32, i64, f32, f64 },
    i32s: []const i32 = &.{},
    i64s: []const i64 = &.{},
    f32s: []const f32 = &.{},
    f64s: []const f64 = &.{},
    idx: []const u32 = &.{},
    dict: bool = false,

    fn atI64(self: NumCol, i: usize) ?i64 {
        if (arrowIsNull(self.nulls, i)) return null;
        if (self.dict) {
            const ix = self.idx[i];
            return switch (self.kind) {
                .i32 => if (ix >= self.i32s.len) null else self.i32s[ix],
                .i64 => if (ix >= self.i64s.len) null else self.i64s[ix],
                .f32, .f64 => null,
            };
        }
        return switch (self.kind) {
            .i32 => self.i32s[i],
            .i64 => self.i64s[i],
            else => null,
        };
    }

    fn atF64(self: NumCol, i: usize) ?f64 {
        if (arrowIsNull(self.nulls, i)) return null;
        if (self.dict) {
            const ix = self.idx[i];
            return switch (self.kind) {
                .f64 => if (ix >= self.f64s.len) null else self.f64s[ix],
                .f32 => if (ix >= self.f32s.len) null else self.f32s[ix],
                .i32 => if (ix >= self.i32s.len) null else @floatFromInt(self.i32s[ix]),
                .i64 => if (ix >= self.i64s.len) null else @floatFromInt(self.i64s[ix]),
            };
        }
        return switch (self.kind) {
            .f64 => self.f64s[i],
            .f32 => self.f32s[i],
            .i32 => @floatFromInt(self.i32s[i]),
            .i64 => @floatFromInt(self.i64s[i]),
        };
    }
};

fn alignedSlice(comptime T: type, ptr: [*]const u8, n: usize) []const T {
    const p: [*]const T = @ptrCast(@alignCast(ptr));
    return p[0..n];
}

fn loadDictFixed(comptime T: type, dict: parquet.RowBatch.DictCol, alloc: std.mem.Allocator) ![]T {
    const n: usize = @intCast(dict.dict_count);
    const width = @sizeOf(T);
    const out = try alloc.alloc(T, n);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const p = dict.dict_ptr + i * width;
        out[i] = switch (T) {
            i32 => std.mem.readInt(i32, p[0..4], .little),
            i64 => std.mem.readInt(i64, p[0..8], .little),
            f32 => @bitCast(std.mem.readInt(u32, p[0..4], .little)),
            f64 => @bitCast(std.mem.readInt(u64, p[0..8], .little)),
            else => @compileError("dict width"),
        };
    }
    return out;
}

fn loadNumCol(
    alloc: std.mem.Allocator,
    reader: parquet.Reader,
    rb: parquet.RowBatch,
    picked_i: usize,
    file_col: i32,
) !?NumCol {
    const meta = reader.column(file_col) orelse return error.ParquetOpenFailed;
    const logical = parquet.columnLogical(reader, file_col);
    const lid: parquet.LogicalId = @enumFromInt(logical.id);
    if (lid == .decimal or lid == .timestamp or lid == .uuid) return null;
    if (rb.columnDictionary(@intCast(picked_i))) |dict| {
        if (dict.offsets != null) return null;
        const n = dict.indices.len;
        return switch (meta.physical_type) {
            .int32 => .{ .n = n, .kind = .i32, .dict = true, .idx = dict.indices, .nulls = dict.nulls, .i32s = try loadDictFixed(i32, dict, alloc) },
            .int64 => .{ .n = n, .kind = .i64, .dict = true, .idx = dict.indices, .nulls = dict.nulls, .i64s = try loadDictFixed(i64, dict, alloc) },
            .float => .{ .n = n, .kind = .f32, .dict = true, .idx = dict.indices, .nulls = dict.nulls, .f32s = try loadDictFixed(f32, dict, alloc) },
            .double => .{ .n = n, .kind = .f64, .dict = true, .idx = dict.indices, .nulls = dict.nulls, .f64s = try loadDictFixed(f64, dict, alloc) },
            else => null,
        };
    }
    const vals = rb.columnValues(@intCast(picked_i)) catch return null;
    if (vals.n < 0) return null;
    const n: usize = @intCast(vals.n);
    if (n == 0) return .{ .n = 0, .kind = .i64 };
    return switch (meta.physical_type) {
        .int32 => .{ .n = n, .kind = .i32, .nulls = vals.nulls, .i32s = alignedSlice(i32, vals.ptr, n) },
        .int64 => .{ .n = n, .kind = .i64, .nulls = vals.nulls, .i64s = alignedSlice(i64, vals.ptr, n) },
        .float => .{ .n = n, .kind = .f32, .nulls = vals.nulls, .f32s = alignedSlice(f32, vals.ptr, n) },
        .double => .{ .n = n, .kind = .f64, .nulls = vals.nulls, .f64s = alignedSlice(f64, vals.ptr, n) },
        else => null,
    };
}

fn findPicked(reader: parquet.Reader, picked: []const i32, name: []const u8) ?usize {
    for (picked, 0..) |file_col, i| {
        const meta = reader.column(file_col) orelse continue;
        if (colSame(meta.name, name)) return i;
    }
    return null;
}

fn applyPartial(states: []AggState, query: sql.Query, n: u64, sum_f: f64, sum_i: i64, is_float: bool) void {
    var si: usize = 0;
    for (query.items) |item| {
        if (item != .agg) continue;
        const st = &states[si];
        si += 1;
        switch (item.agg.kind) {
            .count => st.count += n,
            .sum, .avg => {
                st.count += n;
                st.inited = true;
                if (is_float) {
                    st.is_float = true;
                    st.sum_f += sum_f;
                } else {
                    st.sum_i += sum_i;
                    st.sum_f = @floatFromInt(st.sum_i);
                }
            },
            .min => if (n == 1) {
                st.count += 1;
                if (is_float) {
                    st.is_float = true;
                    if (!st.inited or sum_f < st.min_f) st.min_f = sum_f;
                } else if (!st.inited or sum_i < st.min_i) st.min_i = sum_i;
                st.inited = true;
            },
            .max => if (n == 1) {
                st.count += 1;
                if (is_float) {
                    st.is_float = true;
                    if (!st.inited or sum_f > st.max_f) st.max_f = sum_f;
                } else if (!st.inited or sum_i > st.max_i) st.max_i = sum_i;
                st.inited = true;
            },
        }
    }
}

fn feedGlobalFast(
    states: []AggState,
    reader: parquet.Reader,
    rb: parquet.RowBatch,
    picked: []const i32,
    query: sql.Query,
) !bool {
    if (aggIsDistinct(query)) return false;
    const pred: ?SimplePred = if (query.where) |w| simplePred(w) orelse return false else null;
    var arena = std.heap.ArenaAllocator.init(reader.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var value_i: ?usize = null;
    var pred_i: ?usize = null;
    var si: usize = 0;
    for (query.items) |item| {
        if (item != .agg) continue;
        const agg = item.agg;
        if (agg.kind != .count and agg.kind != .sum and agg.kind != .avg and agg.kind != .min and agg.kind != .max)
            return false;
        if (agg.arg) |arg| {
            const pi = findPicked(reader, picked, arg) orelse return false;
            if (value_i) |v| {
                if (v != pi) return false;
            } else value_i = pi;
        } else if (agg.kind != .count) return false;
        si += 1;
    }
    if (pred) |p| pred_i = findPicked(reader, picked, p.name) orelse return false;
    if (value_i == null and pred_i == null) return false;

    const nrows_src = value_i orelse pred_i.?;
    const col = try loadNumCol(aa, reader, rb, nrows_src, picked[nrows_src]) orelse return false;
    if (col.n == 0) return true;
    var pred_col = col;
    var val_col = col;
    if (pred_i) |pi| {
        if (value_i != null and pi != value_i.?) {
            pred_col = try loadNumCol(aa, reader, rb, pi, picked[pi]) orelse return false;
            if (pred_col.n != col.n) return false;
        }
    }
    if (value_i) |pi| {
        if (pi != nrows_src) {
            val_col = try loadNumCol(aa, reader, rb, pi, picked[pi]) orelse return false;
            if (val_col.n != col.n) return false;
        }
    }
    const nullable = bitmapHasNull(col.nulls, col.n) or bitmapHasNull(pred_col.nulls, pred_col.n) or bitmapHasNull(val_col.nulls, val_col.n);

    const fast_sum = !nullable and pred == null and value_i != null and val_col.kind == .f64 and val_col.dict and blk: {
        var ok = true;
        for (query.items) |item| {
            if (item != .agg) continue;
            if (item.agg.kind == .min or item.agg.kind == .max) ok = false;
        }
        break :blk ok;
    };
    if (fast_sum) {
        var s: f64 = 0;
        for (val_col.idx) |ix| {
            if (ix >= val_col.f64s.len) return error.ParquetOpenFailed;
            s += val_col.f64s[ix];
        }
        applyPartial(states, query, @intCast(val_col.n), s, 0, true);
        return true;
    }

    if (!nullable and pred != null and value_i != null and val_col.dict and val_col.kind == .f64 and pred_col.dict and pred_col.kind == .i32) {
        const p = pred.?;
        const lit: i64 = switch (p.lit) {
            .int => |iv| iv,
            .float => |fv| @as(i64, @intFromFloat(fv)),
            else => return false,
        };
        if (pred_col.idx.len != val_col.idx.len) return false;
        var s: f64 = 0;
        var matched: u64 = 0;
        for (pred_col.idx, val_col.idx) |ki, vi| {
            if (ki >= pred_col.i32s.len or vi >= val_col.f64s.len) return error.ParquetOpenFailed;
            if (!cmpI64(p.op, pred_col.i32s[ki], lit)) continue;
            s += val_col.f64s[vi];
            matched += 1;
        }
        applyPartial(states, query, matched, s, 0, true);
        return true;
    }

    if (nullable) return false;
    const n = col.n;
    var matched: u64 = 0;
    var sum_f: f64 = 0;
    var sum_i: i64 = 0;
    var min_f: f64 = 0;
    var max_f: f64 = 0;
    var min_i: i64 = 0;
    var max_i: i64 = 0;
    var extreme = false;
    const as_float = val_col.kind == .f32 or val_col.kind == .f64;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        if (pred) |p| {
            if (pred_col.kind == .f32 or pred_col.kind == .f64) {
                const v = pred_col.atF64(row) orelse continue;
                const lit: f64 = switch (p.lit) {
                    .int => |iv| @floatFromInt(iv),
                    .float => |fv| fv,
                    else => return false,
                };
                if (!cmpF64(p.op, v, lit)) continue;
            } else {
                const v = pred_col.atI64(row) orelse continue;
                if (!predKeepI64(p, v)) continue;
            }
        }
        matched += 1;
        if (value_i != null) {
            if (as_float) {
                const v = val_col.atF64(row) orelse return false;
                sum_f += v;
                if (!extreme or v < min_f) min_f = v;
                if (!extreme or v > max_f) max_f = v;
                extreme = true;
            } else {
                const v = val_col.atI64(row) orelse return false;
                sum_i += v;
                if (!extreme or v < min_i) min_i = v;
                if (!extreme or v > max_i) max_i = v;
                extreme = true;
            }
        }
    }
    if (value_i == null) {
        var csi: usize = 0;
        for (query.items) |item| {
            if (item != .agg) continue;
            states[csi].count += matched;
            csi += 1;
        }
        return true;
    }
    applyPartial(states, query, matched, sum_f, sum_i, as_float);
    if (extreme) {
        var msi: usize = 0;
        for (query.items) |item| {
            if (item != .agg) continue;
            const st = &states[msi];
            msi += 1;
            if (item.agg.kind == .min) {
                if (as_float) {
                    st.is_float = true;
                    if (!st.inited or min_f < st.min_f) st.min_f = min_f;
                } else if (!st.inited or min_i < st.min_i) st.min_i = min_i;
                st.inited = true;
            } else if (item.agg.kind == .max) {
                if (as_float) {
                    st.is_float = true;
                    if (!st.inited or max_f > st.max_f) st.max_f = max_f;
                } else if (!st.inited or max_i > st.max_i) st.max_i = max_i;
                st.inited = true;
            }
        }
    }
    return true;
}

const GroupAcc = struct {
    alloc: std.mem.Allocator,
    ints: std.AutoArrayHashMapUnmanaged(i64, []AggState) = .empty,
    strs: std.StringArrayHashMapUnmanaged([]AggState) = .empty,
    kind: enum { none, ints, strs } = .none,
    key_dt: DataType = .int64,
    sum_float: bool = false,

    fn deinit(self: *GroupAcc) void {
        var ii = self.ints.iterator();
        while (ii.next()) |e| self.alloc.free(e.value_ptr.*);
        self.ints.deinit(self.alloc);
        var si = self.strs.iterator();
        while (si.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.strs.deinit(self.alloc);
    }

    fn touchInt(self: *GroupAcc, key: i64, query: sql.Query) ![]AggState {
        self.kind = .ints;
        const gop = try self.ints.getOrPut(self.alloc, key);
        if (!gop.found_existing) gop.value_ptr.* = try makeStates(self.alloc, query);
        return gop.value_ptr.*;
    }
};

fn mergeAggInto(dst: *AggState, src: AggState) void {
    switch (dst.kind) {
        .count => dst.count += src.count,
        .sum, .avg => {
            dst.count += src.count;
            if (src.is_float or dst.is_float) {
                const base: f64 = if (dst.is_float) dst.sum_f else @floatFromInt(dst.sum_i);
                const add: f64 = if (src.is_float) src.sum_f else @floatFromInt(src.sum_i);
                dst.sum_f = base + add;
                dst.is_float = true;
            } else {
                dst.sum_i += src.sum_i;
                dst.sum_f = @floatFromInt(dst.sum_i);
            }
            dst.inited = dst.inited or src.inited;
        },
        .min => {
            dst.count += src.count;
            if (!src.inited) return;
            if (!dst.inited) {
                dst.* = src;
                dst.kind = .min;
                return;
            }
            if (src.is_float or dst.is_float) {
                const a: f64 = if (dst.is_float) dst.min_f else @floatFromInt(dst.min_i);
                const b: f64 = if (src.is_float) src.min_f else @floatFromInt(src.min_i);
                dst.min_f = @min(a, b);
                dst.is_float = true;
            } else if (src.min_i < dst.min_i) dst.min_i = src.min_i;
            dst.inited = true;
        },
        .max => {
            dst.count += src.count;
            if (!src.inited) return;
            if (!dst.inited) {
                dst.* = src;
                dst.kind = .max;
                return;
            }
            if (src.is_float or dst.is_float) {
                const a: f64 = if (dst.is_float) dst.max_f else @floatFromInt(dst.max_i);
                const b: f64 = if (src.is_float) src.max_f else @floatFromInt(src.max_i);
                dst.max_f = @max(a, b);
                dst.is_float = true;
            } else if (src.max_i > dst.max_i) dst.max_i = src.max_i;
            dst.inited = true;
        },
    }
}

fn columnsHaveOffsetIndex(reader: parquet.Reader, picked: []const i32) bool {
    if (picked.len == 0 or reader.numRowGroups() < 1) return false;
    for (picked) |col| {
        if (reader.hasOffsetIndex(0, col)) continue;
        if (reader.columnRepeats(col)) return false;
    }
    return true;
}

fn numericFileCol(reader: parquet.Reader, file_col: i32) bool {
    const meta = reader.column(file_col) orelse return false;
    switch (meta.physical_type) {
        .int32, .int64, .float, .double => {},
        else => return false,
    }
    const logical = parquet.columnLogical(reader, file_col);
    const lid: parquet.LogicalId = @enumFromInt(logical.id);
    return lid != .decimal and lid != .timestamp and lid != .uuid;
}

const GSlice = struct {
    path: []const u8,
    query: sql.Query,
    wanted: ?[]const []const u8,
    vec_size: i32,
    lo: i32,
    hi: i32,
    row_off: i64 = 0,
    row_len: i64 = 0,
    stats: ?*ScanStats,
    cancel: ?*CancelState,
    states: []AggState = &.{},
    any: bool = false,
    slow: bool = false,
    err: ?anyerror = null,
};

fn globalSliceJob(job: *GSlice) error{Canceled}!void {
    bindCancel(job.cancel);
    defer bindCancel(null);
    globalSliceJobInner(job) catch |err| switch (err) {
        error.QueryCanceled => return error.Canceled,
        else => job.err = err,
    };
}

fn globalSliceJobInner(job: *GSlice) !void {
    const heap = std.heap.smp_allocator;
    job.states = try makeStates(heap, job.query);
    var reader = try parquet.openLocalPath(heap, job.path);
    defer reader.close();
    const picked = try pickParquetColumns(heap, reader, job.wanted);
    defer heap.free(picked);
    const ranged = job.row_len > 0;
    const nrg: usize = if (ranged) 0 else @intCast(@max(reader.numRowGroups(), 0));
    var seen: []u8 = &.{};
    if (!ranged and job.query.where != null) seen = try heap.alloc(u8, nrg);
    defer if (seen.len > 0) heap.free(seen);
    if (seen.len > 0) @memset(seen, 0);
    var slice = RgSlice{
        .lo = job.lo,
        .hi = job.hi,
        .where = job.query.where,
        .stats = job.stats,
        .seen = seen,
    };
    var hook = parquet.RgHook{ .keep = keepRowGroupSlice, .user = &slice };
    var br = try reader.openBatchFilteredEx(picked, job.vec_size, if (ranged) null else &hook, true);
    if (ranged) try br.setRowRange(job.row_off, job.row_len);
    defer br.close();
    while (try br.next()) |rb_val| {
        try checkCancel();
        var rb = rb_val;
        defer rb.deinit();
        if (rb.numRows() <= 0) continue;
        if (!try feedGlobalFast(job.states, reader, rb, picked, job.query)) {
            job.slow = true;
            return;
        }
        job.any = true;
    }
}

fn parallelParquetGlobal(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    query: sql.Query,
    states: []AggState,
    any: *bool,
    sample: *?Batch,
    stats: ?*ScanStats,
    wanted: ?[]const []const u8,
    vec_size: i32,
) !bool {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.single_threaded) return false;
    if (file.bytes != null or aws.isRemote(file.path)) return false;
    const width = scanWidth();
    if (width < 2 or aggIsDistinct(query)) return false;
    if (query.where) |w| if (simplePred(w) == null) return false;

    const plan = blk: {
        var reader = try parquet.openLocation(allocator, t, file.path);
        defer reader.close();
        const n = reader.numRowGroups();
        const picked = try pickParquetColumns(allocator, reader, wanted);
        defer allocator.free(picked);
        if (picked.len == 0) return false;
        for (query.items) |item| {
            if (item != .agg) continue;
            switch (item.agg.kind) {
                .count, .sum, .avg, .min, .max => {},
            }
            if (item.agg.arg) |arg| {
                const pi = findPicked(reader, picked, arg) orelse return false;
                if (!numericFileCol(reader, picked[pi])) return false;
            } else if (item.agg.kind != .count) return false;
        }
        if (query.where) |w| {
            if (simplePred(w)) |p| {
                const pi = findPicked(reader, picked, p.name) orelse return false;
                if (!numericFileCol(reader, picked[pi])) return false;
            }
        }
        const nrows = reader.numRows();
        const by_row = n < 2 and nrows >= 65536 and columnsHaveOffsetIndex(reader, picked);
        if (n < 2 and !by_row) return false;
        break :blk .{ .nrg = n, .nrows = nrows, .by_row = by_row };
    };

    const n: usize = if (plan.by_row) width else @min(width, @as(usize, @intCast(plan.nrg)));
    const jobs = try allocator.alloc(GSlice, n);
    defer allocator.free(jobs);
    const heap = std.heap.smp_allocator;
    defer for (jobs) |job| if (job.states.len > 0) heap.free(job.states);

    if (plan.by_row) {
        const base: i64 = @divFloor(plan.nrows, @as(i64, @intCast(n)));
        var extra: i64 = @mod(plan.nrows, @as(i64, @intCast(n)));
        var off: i64 = 0;
        for (jobs) |*job| {
            const span = base + if (extra > 0) blk: {
                extra -= 1;
                break :blk @as(i64, 1);
            } else 0;
            job.* = .{
                .path = file.path,
                .query = query,
                .wanted = wanted,
                .vec_size = vec_size,
                .lo = 0,
                .hi = 0,
                .row_off = off,
                .row_len = span,
                .stats = stats,
                .cancel = bound_cancel,
            };
            off += span;
        }
    } else {
        const base: i32 = @intCast(@divFloor(plan.nrg, @as(i32, @intCast(n))));
        var extra: i32 = @intCast(@mod(plan.nrg, @as(i32, @intCast(n))));
        var lo: i32 = 0;
        for (jobs) |*job| {
            const span = base + if (extra > 0) blk: {
                extra -= 1;
                break :blk @as(i32, 1);
            } else 0;
            job.* = .{
                .path = file.path,
                .query = query,
                .wanted = wanted,
                .vec_size = vec_size,
                .lo = lo,
                .hi = lo + span,
                .stats = stats,
                .cancel = bound_cancel,
            };
            lo += span;
        }
    }

    var group: std.Io.Group = .init;
    var joined = false;
    defer if (!joined) group.cancel(t.io);
    for (jobs) |*job| {
        group.concurrent(t.io, globalSliceJob, .{job}) catch |err| switch (err) {
            error.ConcurrencyUnavailable => try globalSliceJobInner(job),
        };
    }
    group.await(t.io) catch |err| {
        joined = true;
        if (err == error.Canceled) return error.QueryCanceled;
        return err;
    };
    joined = true;

    for (jobs) |job| {
        if (job.err) |e| return e;
        if (job.slow) return false;
    }
    for (jobs) |job| {
        for (states, job.states) |*dst, src| mergeAggInto(dst, src);
        any.* = any.* or job.any;
    }
    if (any.* and sample.* == null) {
        var reader = try parquet.openLocation(allocator, t, file.path);
        defer reader.close();
        const picked = try pickParquetColumns(allocator, reader, wanted);
        defer allocator.free(picked);
        sample.* = try emptyFromPicked(allocator, reader, picked);
    }
    return true;
}

const HSlice = struct {
    path: []const u8,
    query: sql.Query,
    wanted: ?[]const []const u8,
    vec_size: i32,
    lo: i32,
    hi: i32,
    row_off: i64 = 0,
    row_len: i64 = 0,
    stats: ?*ScanStats,
    cancel: ?*CancelState,
    acc: GroupAcc = undefined,
    slow: bool = false,
    err: ?anyerror = null,
};

fn groupSliceJob(job: *HSlice) error{Canceled}!void {
    bindCancel(job.cancel);
    defer bindCancel(null);
    groupSliceJobInner(job) catch |err| switch (err) {
        error.QueryCanceled => return error.Canceled,
        else => job.err = err,
    };
}

fn groupSliceJobInner(job: *HSlice) !void {
    const heap = std.heap.smp_allocator;
    job.acc = .{ .alloc = heap };
    var reader = try parquet.openLocalPath(heap, job.path);
    defer reader.close();
    const picked = try pickParquetColumns(heap, reader, job.wanted);
    defer heap.free(picked);
    const ranged = job.row_len > 0;
    const nrg: usize = if (ranged) 0 else @intCast(@max(reader.numRowGroups(), 0));
    var seen: []u8 = &.{};
    if (!ranged and job.query.where != null) seen = try heap.alloc(u8, nrg);
    defer if (seen.len > 0) heap.free(seen);
    if (seen.len > 0) @memset(seen, 0);
    var slice = RgSlice{
        .lo = job.lo,
        .hi = job.hi,
        .where = job.query.where,
        .stats = job.stats,
        .seen = seen,
    };
    var hook = parquet.RgHook{ .keep = keepRowGroupSlice, .user = &slice };
    var br = try reader.openBatchFilteredEx(picked, job.vec_size, if (ranged) null else &hook, true);
    if (ranged) try br.setRowRange(job.row_off, job.row_len);
    defer br.close();
    var scratch = std.heap.ArenaAllocator.init(heap);
    defer scratch.deinit();
    while (try br.next()) |rb_val| {
        try checkCancel();
        var rb = rb_val;
        defer rb.deinit();
        if (rb.numRows() <= 0) continue;
        if (!try foldGroupBatch(&job.acc, reader, rb, picked, job.query, scratch.allocator())) {
            job.slow = true;
            return;
        }
        _ = scratch.reset(.retain_capacity);
    }
}

fn parallelParquetGroups(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    query: sql.Query,
    acc: *GroupAcc,
    stats: ?*ScanStats,
    wanted: ?[]const []const u8,
    vec_size: i32,
) !bool {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.single_threaded) return false;
    if (file.bytes != null or aws.isRemote(file.path)) return false;
    if (!hashAggFast(query)) return false;
    const width = scanWidth();
    if (width < 2) return false;

    const plan = blk: {
        var reader = try parquet.openLocation(allocator, t, file.path);
        defer reader.close();
        const n = reader.numRowGroups();
        const picked = try pickParquetColumns(allocator, reader, wanted);
        defer allocator.free(picked);
        const gi = findPicked(reader, picked, query.group_by[0]) orelse return false;
        if (!numericFileCol(reader, picked[gi])) return false;
        for (query.items) |item| {
            if (item != .agg) continue;
            if (item.agg.arg) |arg| {
                const pi = findPicked(reader, picked, arg) orelse return false;
                if (!numericFileCol(reader, picked[pi])) return false;
            }
        }
        if (query.where) |w| {
            if (simplePred(w)) |p| {
                const pi = findPicked(reader, picked, p.name) orelse return false;
                if (!numericFileCol(reader, picked[pi])) return false;
            } else return false;
        }
        const nrows = reader.numRows();
        const by_row = n < 2 and nrows >= 65536 and columnsHaveOffsetIndex(reader, picked);
        if (n < 2 and !by_row) return false;
        break :blk .{ .nrg = n, .nrows = nrows, .by_row = by_row };
    };

    const n: usize = if (plan.by_row) width else @min(width, @as(usize, @intCast(plan.nrg)));
    const jobs = try allocator.alloc(HSlice, n);
    defer allocator.free(jobs);
    defer for (jobs) |*job| job.acc.deinit();

    if (plan.by_row) {
        const base: i64 = @divFloor(plan.nrows, @as(i64, @intCast(n)));
        var extra: i64 = @mod(plan.nrows, @as(i64, @intCast(n)));
        var off: i64 = 0;
        for (jobs) |*job| {
            const span = base + if (extra > 0) blk: {
                extra -= 1;
                break :blk @as(i64, 1);
            } else 0;
            job.* = .{
                .path = file.path,
                .query = query,
                .wanted = wanted,
                .vec_size = vec_size,
                .lo = 0,
                .hi = 0,
                .row_off = off,
                .row_len = span,
                .stats = stats,
                .cancel = bound_cancel,
                .acc = .{ .alloc = std.heap.smp_allocator },
            };
            off += span;
        }
    } else {
        const base: i32 = @intCast(@divFloor(plan.nrg, @as(i32, @intCast(n))));
        var extra: i32 = @intCast(@mod(plan.nrg, @as(i32, @intCast(n))));
        var lo: i32 = 0;
        for (jobs) |*job| {
            const span = base + if (extra > 0) blk: {
                extra -= 1;
                break :blk @as(i32, 1);
            } else 0;
            job.* = .{
                .path = file.path,
                .query = query,
                .wanted = wanted,
                .vec_size = vec_size,
                .lo = lo,
                .hi = lo + span,
                .stats = stats,
                .cancel = bound_cancel,
                .acc = .{ .alloc = std.heap.smp_allocator },
            };
            lo += span;
        }
    }

    var group: std.Io.Group = .init;
    var joined = false;
    defer if (!joined) group.cancel(t.io);
    for (jobs) |*job| {
        group.concurrent(t.io, groupSliceJob, .{job}) catch |err| switch (err) {
            error.ConcurrencyUnavailable => try groupSliceJobInner(job),
        };
    }
    group.await(t.io) catch |err| {
        joined = true;
        if (err == error.Canceled) return error.QueryCanceled;
        return err;
    };
    joined = true;

    for (jobs) |job| {
        if (job.err) |e| return e;
        if (job.slow) return false;
    }
    for (jobs) |job| {
        acc.sum_float = acc.sum_float or job.acc.sum_float;
        if (job.acc.kind != .ints) continue;
        acc.key_dt = job.acc.key_dt;
        var it = job.acc.ints.iterator();
        while (it.next()) |e| {
            const dst = try acc.touchInt(e.key_ptr.*, query);
            for (dst, e.value_ptr.*) |*d, s| mergeAggInto(d, s);
        }
    }
    return true;
}

fn executeHashAgg(
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
    var acc = GroupAcc{ .alloc = allocator };
    defer acc.deinit();
    var vec_aa = std.heap.ArenaAllocator.init(t.allocator);
    defer vec_aa.deinit();

    for (files) |file| {
        try checkCancel();
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
                while (try reader.nextPage(vec_aa.allocator(), wanted, query.where)) |page| {
                    var vec = page;
                    if (query.where) |expr| vec = try filterExpr(vec_aa.allocator(), vec, expr, null);
                    if (vec.len == 0) {
                        _ = vec_aa.reset(.retain_capacity);
                        continue;
                    }
                    try ingestGroupRows(&acc, vec, query);
                    _ = vec_aa.reset(.retain_capacity);
                }
            },
            .parquet => {
                if (try parallelParquetGroups(allocator, t, file, query, &acc, stats, wanted, vec_size))
                    continue;
                var reader = try parquet.openLocation(allocator, t, file.path);
                defer reader.close();
                const picked = try pickParquetColumns(allocator, reader, wanted);
                if (picked.len == 0) return error.ColumnNotFound;
                var rg_filter: RgFilter = .{ .preserve_dict = true };
                defer rg_filter.deinit();
                var br = try beginParquetBatch(reader, picked, vec_size, query.where, stats, &rg_filter);
                defer br.close();
                while (try br.next()) |rb_val| {
                    var rb = rb_val;
                    defer rb.deinit();
                    if (rb.numRows() <= 0) continue;
                    if (try foldGroupBatch(&acc, reader, rb, picked, query, vec_aa.allocator())) {
                        _ = vec_aa.reset(.retain_capacity);
                        continue;
                    }
                    var vec = try batchFromRowBatch(vec_aa.allocator(), reader, rb, picked);
                    if (query.where) |expr| vec = try filterExpr(vec_aa.allocator(), vec, expr, null);
                    if (vec.len > 0) try ingestGroupRows(&acc, vec, query);
                    _ = vec_aa.reset(.retain_capacity);
                }
            },
        }
    }

    var batch = try finishGroups(allocator, &acc, query);
    if (query.having) |expr| batch = try filterExpr(allocator, batch, expr, query);
    return applyTail(allocator, batch, query, .{ .where_done = true, .agg_done = true });
}

fn foldGroupBatch(
    acc: *GroupAcc,
    reader: parquet.Reader,
    rb: parquet.RowBatch,
    picked: []const i32,
    query: sql.Query,
    alloc: std.mem.Allocator,
) !bool {
    const gname = query.group_by[0];
    const gi = findPicked(reader, picked, gname) orelse return false;
    const pred: ?SimplePred = if (query.where) |w| simplePred(w) orelse return false else null;
    const key = try loadNumCol(alloc, reader, rb, gi, picked[gi]) orelse return false;
    if (key.kind != .i32 and key.kind != .i64) return false;
    acc.key_dt = if (key.kind == .i32) .int32 else .int64;
    if (acc.kind == .strs) return false;

    var value_i: ?usize = null;
    var value_name: ?[]const u8 = null;
    var has_extreme = false;
    for (query.items) |item| {
        if (item != .agg) continue;
        if (item.agg.kind == .min or item.agg.kind == .max) has_extreme = true;
        if (item.agg.arg) |arg| {
            const pi = findPicked(reader, picked, arg) orelse return false;
            if (value_i) |v| {
                if (v != pi) return false;
            } else {
                value_i = pi;
                value_name = arg;
            }
        }
    }
    var val: ?NumCol = null;
    if (value_i) |pi| {
        const loaded = try loadNumCol(alloc, reader, rb, pi, picked[pi]) orelse return false;
        if (loaded.n != key.n) return false;
        if (loaded.kind == .f32 or loaded.kind == .f64) acc.sum_float = true;
        val = loaded;
    }
    if (pred) |p| {
        const on_key = colSame(p.name, gname);
        const on_val = if (value_name) |vn| colSame(p.name, vn) else false;
        if (!on_key and !on_val) return false;
    }

    const key_null = bitmapHasNull(key.nulls, key.n);
    const val_null = if (val) |v| bitmapHasNull(v.nulls, v.n) else false;
    const scatter = !has_extreme and !key_null and !val_null and key.dict and key.kind == .i32 and val != null and val.?.dict and val.?.kind == .f64 and (pred == null or colSame(pred.?.name, gname));
    if (scatter) {
        const kd = key.i32s;
        const vd = val.?.f64s;
        const counts = try alloc.alloc(u64, kd.len);
        const sums = try alloc.alloc(f64, kd.len);
        @memset(counts, 0);
        @memset(sums, 0);
        const lit_i: i32 = if (pred) |p| switch (p.lit) {
            .int => |n| if (n < std.math.minInt(i32) or n > std.math.maxInt(i32)) return false else @intCast(n),
            else => return false,
        } else 0;
        for (key.idx, val.?.idx) |ki, vi| {
            if (ki >= kd.len or vi >= vd.len) return error.ParquetOpenFailed;
            if (pred) |p| if (!cmpI64(p.op, kd[ki], lit_i)) continue;
            counts[ki] += 1;
            sums[ki] += vd[vi];
        }
        for (kd, counts, sums) |k, c, s| {
            if (c == 0) continue;
            const sts = try acc.touchInt(k, query);
            applyPartial(sts, query, c, s, 0, true);
        }
        return true;
    }

    if (key_null or val_null) return false;
    var row: usize = 0;
    while (row < key.n) : (row += 1) {
        const kv = key.atI64(row) orelse continue;
        if (pred) |p| {
            if (colSame(p.name, gname)) {
                if (!predKeepI64(p, kv)) continue;
            } else if (val) |vc| {
                if (vc.kind == .f32 or vc.kind == .f64) {
                    const fv = vc.atF64(row) orelse continue;
                    const lit: f64 = switch (p.lit) {
                        .int => |iv| @floatFromInt(iv),
                        .float => |f| f,
                        else => return false,
                    };
                    if (!cmpF64(p.op, fv, lit)) continue;
                } else {
                    const iv = vc.atI64(row) orelse continue;
                    if (!predKeepI64(p, iv)) continue;
                }
            }
        }
        const sts = try acc.touchInt(kv, query);
        if (val) |vc| {
            if (vc.kind == .f32 or vc.kind == .f64) {
                const fv = vc.atF64(row) orelse continue;
                applyPartial(sts, query, 1, fv, 0, true);
            } else {
                const iv = vc.atI64(row) orelse continue;
                applyPartial(sts, query, 1, 0, iv, false);
            }
        } else {
            applyPartial(sts, query, 1, 0, 0, false);
        }
    }
    return true;
}

fn ingestGroupRows(acc: *GroupAcc, vec: Batch, query: sql.Query) !void {
    const gname = query.group_by[0];
    const gi = findBatchCol(vec, gname) orelse return error.ColumnNotFound;
    const col = vec.columns[gi];
    var row: usize = 0;
    while (row < vec.len) : (row += 1) {
        switch (col.data_type) {
            .int32, .int64 => {
                if (acc.kind == .strs) return error.TypeMismatch;
                acc.key_dt = col.data_type;
                if (col.isNull(row)) continue;
                const key: i64 = if (col.data_type == .int32) col.i32s[row] else col.i64s[row];
                const sts = try acc.touchInt(key, query);
                try feedRow(sts, vec, query, row, acc.alloc);
            },
            .utf8 => {
                if (acc.kind == .ints) return error.TypeMismatch;
                acc.kind = .strs;
                acc.key_dt = .utf8;
                var buf: [512]u8 = undefined;
                var heap: ?[]u8 = null;
                const key: []const u8 = if (col.isNull(row)) blk: {
                    buf[0] = 0;
                    break :blk buf[0..1];
                } else blk: {
                    const s = col.strAt(row);
                    if (s.len + 1 > buf.len) {
                        const big = try acc.alloc.alloc(u8, s.len + 1);
                        big[0] = 1;
                        @memcpy(big[1..], s);
                        heap = big;
                        break :blk big;
                    }
                    buf[0] = 1;
                    @memcpy(buf[1..][0..s.len], s);
                    break :blk buf[0 .. s.len + 1];
                };
                if (acc.strs.getPtr(key)) |sts| {
                    try feedRow(sts.*, vec, query, row, acc.alloc);
                    if (heap) |h| acc.alloc.free(h);
                } else {
                    const owned = heap orelse try acc.alloc.dupe(u8, key);
                    const sts = try makeStates(acc.alloc, query);
                    try feedRow(sts, vec, query, row, acc.alloc);
                    try acc.strs.put(acc.alloc, owned, sts);
                }
            },
            else => return error.UnsupportedType,
        }
    }
}

fn findBatchCol(vec: Batch, name: []const u8) ?usize {
    if (vec.columnIndex(name)) |i| return i;
    const tail = if (std.mem.lastIndexOfScalar(u8, name, '.')) |d| name[d + 1 ..] else return null;
    return vec.columnIndex(tail);
}

fn finishGroups(allocator: std.mem.Allocator, acc: *GroupAcc, query: sql.Query) !Batch {
    const n: usize = switch (acc.kind) {
        .none => 0,
        .ints => acc.ints.count(),
        .strs => acc.strs.count(),
    };
    const ncols = aggOutLen(query);
    const out_cols = try allocator.alloc(Column, ncols);
    var col_i: usize = 0;
    var agg_i: usize = 0;
    const int_keys = acc.ints.keys();
    const int_vals = acc.ints.values();
    const str_keys = acc.strs.keys();
    const str_vals = acc.strs.values();
    for (query.items) |item| {
        switch (item) {
            .window => {},
            .column => |c| {
                const name = c.alias orelse c.name;
                out_cols[col_i] = try finishKeyColumn(allocator, acc, name, n, int_keys, str_keys);
                col_i += 1;
            },
            .grouping => |g| {
                out_cols[col_i] = try groupingColumn(allocator, g.alias orelse g.name, n, &.{});
                col_i += 1;
            },
            .agg => |agg| {
                const dummy: Column = .{
                    .name = agg.arg orelse "",
                    .data_type = if (acc.sum_float) .float64 else .int64,
                    .len = 0,
                };
                const template: AggState = if (n == 0)
                    .{ .kind = agg.kind, .is_float = acc.sum_float and (agg.kind == .sum or agg.kind == .avg or agg.kind == .min or agg.kind == .max) }
                else switch (acc.kind) {
                    .ints => int_vals[0][agg_i],
                    .strs => str_vals[0][agg_i],
                    .none => .{ .kind = agg.kind },
                };
                var col = try finalizeAgg(allocator, template, dummy, try aggName(allocator, agg), n);
                if (n > 0) {
                    switch (acc.kind) {
                        .ints => for (int_vals, 0..) |sts, gi| try writeAggCell(&col, sts[agg_i], gi, allocator),
                        .strs => for (str_vals, 0..) |sts, gi| try writeAggCell(&col, sts[agg_i], gi, allocator),
                        .none => {},
                    }
                }
                out_cols[col_i] = col;
                col_i += 1;
                agg_i += 1;
            },
            else => return error.UnsupportedSql,
        }
    }
    return .{ .columns = out_cols, .len = n };
}

fn finishKeyColumn(
    allocator: std.mem.Allocator,
    acc: *GroupAcc,
    name: []const u8,
    n: usize,
    int_keys: []i64,
    str_keys: [][]const u8,
) !Column {
    var col: Column = .{ .name = name, .data_type = acc.key_dt, .len = n };
    switch (acc.key_dt) {
        .int32 => {
            col.i32s = try allocator.alloc(i32, n);
            for (int_keys, 0..) |k, i| col.i32s[i] = @intCast(k);
        },
        .int64 => {
            col.i64s = try allocator.alloc(i64, n);
            @memcpy(col.i64s, int_keys);
        },
        .utf8 => {
            var nbytes: usize = 0;
            for (str_keys) |k| {
                if (k.len > 0 and k[0] == 1) nbytes += k.len - 1;
            }
            const bytes = try allocator.alloc(u8, nbytes);
            const offs = try allocator.alloc(u32, n + 1);
            const valid = try allocator.alloc(u8, n);
            var off: u32 = 0;
            var any_null = false;
            for (str_keys, 0..) |k, i| {
                offs[i] = off;
                if (k.len == 0 or k[0] == 0) {
                    valid[i] = 0;
                    any_null = true;
                } else {
                    valid[i] = 1;
                    const s = k[1..];
                    if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
                    off += @intCast(s.len);
                }
            }
            offs[n] = off;
            col.utf8 = .{ .offsets = offs, .bytes = bytes };
            if (any_null) col.valid = valid else allocator.free(valid);
        },
        else => return error.UnsupportedType,
    }
    return col;
}

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
        try checkCancel();
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
                    var n = reader.numRows();
                    if (query.where) |expr| {
                        const probe: Batch = .{ .columns = &.{}, .len = 1 };
                        if (!try evalBool(allocator, probe, 0, expr, query)) n = 0;
                    }
                    for (states) |*s| {
                        if (s.kind != .count or s.distinct) return error.InvalidSyntax;
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
                if (try parallelParquetGlobal(allocator, t, file, query, states, &any, &sample, stats, wanted, vec_size))
                    continue;
                var reader = try parquet.openLocation(allocator, t, file.path);
                defer reader.close();
                const picked = try pickParquetColumns(allocator, reader, wanted);
                if (picked.len == 0) {
                    var n: u64 = @intCast(reader.numRows());
                    if (query.where) |expr| {
                        const probe: Batch = .{ .columns = &.{}, .len = 1 };
                        if (!try evalBool(allocator, probe, 0, expr, query)) n = 0;
                    }
                    for (states) |*s| {
                        if (s.kind != .count or s.distinct) return error.InvalidSyntax;
                        s.count += n;
                        if (n > 0) s.inited = true;
                    }
                    any = any or n > 0;
                    continue;
                }
                var rg_filter: RgFilter = .{ .preserve_dict = true };
                defer rg_filter.deinit();
                var br = try beginParquetBatch(reader, picked, vec_size, query.where, stats, &rg_filter);
                defer br.close();
                while (try br.next()) |rb_val| {
                    var rb = rb_val;
                    defer rb.deinit();
                    if (rb.numRows() <= 0) continue;
                    const va = vec_aa.allocator();
                    if (try feedGlobalFast(states, reader, rb, picked, query)) {
                        any = true;
                        if (sample == null) sample = try emptyFromPicked(allocator, reader, picked);
                        continue;
                    }
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
    if (query.grouping_sets.len > 0) {
        var grow = try spill.Grow.init(allocator, cur.cols);
        while (try cur.next()) try grow.appendCursor(cur);
        const snap = try grow.freeze();
        return aggregateGroupingSets(allocator, snap, query);
    }
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

    const out_cols = try allocator.alloc(Column, aggOutLen(query));
    var col_i: usize = 0;
    if (query.hasWindow()) {
        for (query.group_by) |gname| {
            const src_i = snap.columnIndex(gname) orelse return error.ColumnNotFound;
            out_cols[col_i] = try copyKeyColumn(allocator, snap.columns[src_i], first_rows, gname);
            col_i += 1;
        }
    }
    var agg_i: usize = 0;
    for (query.items) |item| {
        switch (item) {
            .star, .scalar, .literal, .case => return error.UnsupportedSql,
            .expr => |ex| {
                out_cols[col_i] = try evalGroupComputed(allocator, snap, first_rows, ex, query);
                col_i += 1;
            },
            .window => {},
            .column => |c| {
                const src_i = snap.columnIndex(c.name) orelse return error.ColumnNotFound;
                const name = c.alias orelse c.name;
                out_cols[col_i] = try copyKeyColumn(allocator, snap.columns[src_i], first_rows, name);
                col_i += 1;
            },
            .grouping => |g| {
                out_cols[col_i] = try groupingColumn(allocator, g.alias orelse g.name, n_groups, &.{});
                col_i += 1;
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
                col_i += 1;
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

    if ((query.offset orelse 0) == 0 and query.limit == null) {
        if (try parallelStream(allocator, t, files, query, wanted, fallback_schema, stats, &parts, &empty_schema)) {
            return finishStream(allocator, query, fallback_schema, &parts, empty_schema);
        }
    }

    for (files) |file| {
        try checkCancel();
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
            .parquet => try scanParquetStream(allocator, t, file, wanted, query.where, &offset_left, &limit_left, stats),
            .glacier => try scanNativeStream(allocator, t, file, wanted, query.where, &offset_left, &limit_left),
            .avro => return error.UnsupportedType,
        };
        if (piece.len > 0) {
            try parts.append(allocator, piece);
        } else if (empty_schema == null and piece.columns.len > 0) {
            empty_schema = piece;
        }
    }

    return finishStream(allocator, query, fallback_schema, &parts, empty_schema);
}

fn finishStream(
    allocator: std.mem.Allocator,
    query: sql.Query,
    fallback_schema: ?[]const iceberg.SchemaField,
    parts: *std.ArrayList(Batch),
    empty_schema: ?Batch,
) !Batch {
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

const ScanMode = enum { finish, stream };

const PSlot = struct {
    arena: std.heap.ArenaAllocator,
    batch: Batch = .{ .columns = &.{}, .len = 0 },
    err: ?anyerror = null,
    ran: bool = false,
};

const PJob = struct {
    t: vfs.Transport,
    file: iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    schema: ?[]const iceberg.SchemaField,
    mode: ScanMode,
    slot: *PSlot,
    cancel: ?*CancelState,
    offset: u64 = 0,
    limit: ?u64 = null,
    stats: ?*ScanStats = null,
};

fn scanWidth() usize {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.single_threaded) return 1;
    if (std.c.getenv("GLACIER_THREADS")) |raw| {
        const text = std.mem.span(raw);
        if (text.len == 0) return 1;
        return @max(std.fmt.parseInt(usize, text, 10) catch 1, 1);
    }
    return @max(std.Thread.getCpuCount() catch 1, 1);
}

fn canParallelFiles(files: []const iceberg.DataFile, where: ?*const sql.BoolExpr) bool {
    if (scanWidth() < 2) return false;
    var n: usize = 0;
    for (files) |file| {
        if (file.bytes != null) return false;
        if (aws.isRemote(file.path)) return false;
        if (where) |expr| {
            if (iceberg.canSkipFile(file, expr)) continue;
        }
        n += 1;
    }
    return n >= 2;
}

fn parallelFinish(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    wanted: ?[]const []const u8,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
    parts: *std.ArrayList(Batch),
) !bool {
    const got = try parallelBatches(allocator, t, files, wanted, query.where, fallback_schema, stats, .finish) orelse return false;
    for (got) |batch| try parts.append(allocator, batch);
    return true;
}

fn parallelStream(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    query: sql.Query,
    wanted: ?[]const []const u8,
    fallback_schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
    parts: *std.ArrayList(Batch),
    empty_schema: *?Batch,
) !bool {
    const got = try parallelBatches(allocator, t, files, wanted, query.where, fallback_schema, stats, .stream) orelse return false;
    for (got) |piece| {
        if (piece.len > 0) {
            try parts.append(allocator, piece);
        } else if (empty_schema.* == null and piece.columns.len > 0) {
            empty_schema.* = piece;
        }
    }
    return true;
}

fn parallelBatches(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    files: []const iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    schema: ?[]const iceberg.SchemaField,
    stats: ?*ScanStats,
    mode: ScanMode,
) !?[]Batch {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.single_threaded) return null;
    if (!canParallelFiles(files, where)) return null;

    const slots = try allocator.alloc(PSlot, files.len);
    for (slots) |*s| s.* = .{ .arena = .init(std.heap.smp_allocator) };
    defer for (slots) |*s| s.arena.deinit();

    const jobs = try allocator.alloc(PJob, files.len);
    var n_jobs: usize = 0;
    for (files, 0..) |file, i| {
        try checkCancel();
        if (where) |expr| {
            if (iceberg.canSkipFile(file, expr)) {
                if (stats) |s| s.files_pruned += 1;
                continue;
            }
        }
        if (stats) |s| s.files_opened += 1;
        jobs[n_jobs] = .{
            .t = t,
            .file = file,
            .wanted = wanted,
            .where = where,
            .schema = schema,
            .mode = mode,
            .slot = &slots[i],
            .cancel = bound_cancel,
            .stats = stats,
        };
        n_jobs += 1;
    }

    var group: std.Io.Group = .init;
    var joined = false;
    defer if (!joined) group.cancel(t.io);
    for (jobs[0..n_jobs]) |*job| {
        group.concurrent(t.io, parallelScanJob, .{job}) catch |err| switch (err) {
            error.ConcurrencyUnavailable => try parallelScanJobInner(job),
        };
    }
    group.await(t.io) catch |err| {
        joined = true;
        if (err == error.Canceled) return error.QueryCanceled;
        return err;
    };
    joined = true;

    var failed: ?anyerror = null;
    for (jobs[0..n_jobs]) |job| {
        if (job.slot.err) |e| {
            failed = e;
            break;
        }
    }
    if (failed) |e| return e;
    try checkCancel();

    var out: std.ArrayList(Batch) = .empty;
    for (slots) |*s| {
        if (!s.ran) continue;
        try out.append(allocator, try cloneBatch(allocator, s.batch));
    }
    return try out.toOwnedSlice(allocator);
}

fn parallelScanJob(job: *PJob) error{Canceled}!void {
    bindCancel(job.cancel);
    bound_quiet = true;
    defer {
        bound_quiet = false;
        bindCancel(null);
    }
    parallelScanJobInner(job) catch |err| switch (err) {
        error.QueryCanceled => return error.Canceled,
        else => job.slot.err = err,
    };
}

fn parallelScanJobInner(job: *PJob) !void {
    const a = job.slot.arena.allocator();
    job.slot.ran = true;
    job.slot.batch = switch (job.mode) {
        .finish => blk: {
            const scanned = switch (job.file.format) {
                .parquet => try scanParquet(a, job.t, job.file, job.wanted, job.where, job.stats),
                .avro => try scanAvro(a, job.t, job.file),
                .glacier => try scanNativeAll(a, job.t, job.file, job.wanted, job.where),
            };
            break :blk try finishScan(a, scanned, job.file, job.schema, job.wanted);
        },
        .stream => switch (job.file.format) {
            .parquet => try scanParquetStream(a, job.t, job.file, job.wanted, job.where, &job.offset, &job.limit, job.stats),
            .glacier => try scanNativeStream(a, job.t, job.file, job.wanted, job.where, &job.offset, &job.limit),
            .avro => return error.UnsupportedType,
        },
    };
}

const RgCtx = struct {
    where: *const sql.BoolExpr,
    stats: ?*ScanStats,
    seen: []u8,
};

const RgSlice = struct {
    lo: i32,
    hi: i32,
    where: ?*const sql.BoolExpr,
    stats: ?*ScanStats,
    seen: []u8,
};

const RgFilter = struct {
    ctx: RgCtx = undefined,
    hook: parquet.RgHook = undefined,
    seen: []u8 = &.{},
    preserve_dict: bool = false,
    allocator: std.mem.Allocator = if (builtin.cpu.arch == .wasm32)
        std.heap.wasm_allocator
    else
        std.heap.page_allocator,

    fn deinit(self: *RgFilter) void {
        if (self.seen.len == 0) return;
        self.allocator.free(self.seen);
        self.seen = &.{};
    }
};

fn beginParquetBatch(
    reader: parquet.Reader,
    picked: []const i32,
    batch_size: i32,
    where: ?*const sql.BoolExpr,
    stats: ?*ScanStats,
    filter: *RgFilter,
) !parquet.BatchReader {
    if (where) |expr| {
        const n: usize = @intCast(@max(reader.numRowGroups(), 0));
        const seen = try reader.allocator.alloc(u8, n);
        @memset(seen, 0);
        filter.* = .{
            .allocator = reader.allocator,
            .seen = seen,
            .ctx = .{ .where = expr, .stats = stats, .seen = seen },
        };
        filter.hook = .{ .keep = keepRowGroup, .user = &filter.ctx };
        return reader.openBatchFilteredEx(picked, batch_size, &filter.hook, filter.preserve_dict);
    }
    return reader.openBatchFilteredEx(picked, batch_size, null, filter.preserve_dict);
}

fn keepRowGroupSlice(reader: ?*anyopaque, rg: i32, user: ?*anyopaque) callconv(.c) c_int {
    const ctx: *RgSlice = @ptrCast(@alignCast(user orelse return 1));
    if (rg < ctx.lo or rg >= ctx.hi) return 0;
    const expr = ctx.where orelse return 1;
    const handle = reader orelse return 1;
    const keep = rgMayMatch(handle, rg, expr);
    if (rg >= 0 and @as(usize, @intCast(rg)) < ctx.seen.len) {
        const i: usize = @intCast(rg);
        if (ctx.seen[i] == 0) {
            ctx.seen[i] = 1;
            if (!keep) {
                if (ctx.stats) |s| {
                    _ = @atomicRmw(usize, &s.row_groups_pruned, .Add, 1, .monotonic);
                }
            }
        }
    }
    return if (keep) 1 else 0;
}

fn keepRowGroup(reader: ?*anyopaque, rg: i32, user: ?*anyopaque) callconv(.c) c_int {
    const ctx: *RgCtx = @ptrCast(@alignCast(user orelse return 1));
    const handle = reader orelse return 1;
    const keep = rgMayMatch(handle, rg, ctx.where);
    if (rg >= 0 and @as(usize, @intCast(rg)) < ctx.seen.len) {
        const i: usize = @intCast(rg);
        if (ctx.seen[i] == 0) {
            ctx.seen[i] = 1;
            if (!keep) {
                if (ctx.stats) |s| {
                    _ = @atomicRmw(usize, &s.row_groups_pruned, .Add, 1, .monotonic);
                }
            }
        }
    }
    return if (keep) 1 else 0;
}

fn rgMayMatch(handle: *anyopaque, rg: i32, expr: *const sql.BoolExpr) bool {
    return switch (expr.*) {
        .@"and" => |b| rgMayMatch(handle, rg, b.left) and rgMayMatch(handle, rg, b.right),
        .@"or" => |b| rgMayMatch(handle, rg, b.left) or rgMayMatch(handle, rg, b.right),
        .cmp => |cmp| rgCmpMayMatch(handle, rg, cmp),
        .between => |b| rgBetweenMayMatch(handle, rg, b),
        .isnull => |n| rgNullMayMatch(handle, rg, n),
        else => true,
    };
}

fn flipCmp(op: sql.CmpOp) sql.CmpOp {
    return switch (op) {
        .eq, .ne => op,
        .lt => .gt,
        .le => .ge,
        .gt => .lt,
        .ge => .le,
    };
}

const RgCol = struct {
    index: i32,
    physical: parquet.PhysicalType,
};

fn findRgCol(handle: *anyopaque, name: []const u8) ?RgCol {
    const n = parquet.rawNumColumns(handle);
    var i: i32 = 0;
    while (i < n) : (i += 1) {
        const col = parquet.rawColumnName(handle, i) orelse continue;
        if (std.ascii.eqlIgnoreCase(col, name)) {
            return .{ .index = i, .physical = parquet.rawColumnType(handle, i) };
        }
    }
    return null;
}

fn rgCmpMayMatch(handle: *anyopaque, rg: i32, cmp: sql.Cmp) bool {
    if (sql.asColumn(cmp.left)) |name| {
        if (sql.asLiteral(cmp.right)) |lit| return rgColLit(handle, rg, name, cmp.op, lit);
    }
    if (sql.asColumn(cmp.right)) |name| {
        if (sql.asLiteral(cmp.left)) |lit| return rgColLit(handle, rg, name, flipCmp(cmp.op), lit);
    }
    return true;
}

fn rgBetweenMayMatch(handle: *anyopaque, rg: i32, between: sql.Between) bool {
    if (between.negated) return true;
    const name = switch (between.left) {
        .column => |n| n,
        .agg => return true,
    };
    return rgColLit(handle, rg, name, .ge, between.lo) and rgColLit(handle, rg, name, .le, between.hi);
}

fn rgNullMayMatch(handle: *anyopaque, rg: i32, isnull: sql.IsNull) bool {
    const name = switch (isnull.left) {
        .column => |n| n,
        .agg => return true,
    };
    const col = findRgCol(handle, name) orelse return true;
    const st = parquet.rawColumnStats(handle, rg, col.index) catch return true;
    if (st.has_null_count == 0) return true;
    if (isnull.negated) return st.null_count != st.num_values;
    return st.null_count != 0;
}

fn rgColLit(handle: *anyopaque, rg: i32, name: []const u8, op: sql.CmpOp, lit: sql.Literal) bool {
    const col = findRgCol(handle, name) orelse return true;
    var i32v: i32 = 0;
    var i64v: i64 = 0;
    var f32v: f32 = 0;
    var f64v: f64 = 0;
    const bytes: []const u8 = switch (col.physical) {
        .int32 => blk: {
            if (lit != .int) return true;
            if (lit.int < std.math.minInt(i32) or lit.int > std.math.maxInt(i32)) return true;
            i32v = @intCast(lit.int);
            break :blk std.mem.asBytes(&i32v);
        },
        .int64 => blk: {
            if (lit != .int) return true;
            i64v = lit.int;
            break :blk std.mem.asBytes(&i64v);
        },
        .float => blk: {
            f32v = switch (lit) {
                .int => |v| blk2: {
                    if (v < -(1 << 24) or v > (1 << 24)) return true;
                    break :blk2 @floatFromInt(v);
                },
                .float => |v| blk2: {
                    const f: f32 = @floatCast(v);
                    if (@as(f64, f) != v) return true;
                    break :blk2 f;
                },
                else => return true,
            };
            break :blk std.mem.asBytes(&f32v);
        },
        .double => blk: {
            f64v = switch (lit) {
                .int => |v| blk2: {
                    if (v < -(1 << 53) or v > (1 << 53)) return true;
                    break :blk2 @floatFromInt(v);
                },
                .float => |v| v,
                else => return true,
            };
            break :blk std.mem.asBytes(&f64v);
        },
        .byte_array => blk: {
            if (lit != .string) return true;
            break :blk lit.string;
        },
        else => return true,
    };
    return parquet.rawRowGroupMightMatch(handle, rg, col.index, @intFromEnum(op), bytes);
}

fn scanParquetStream(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    offset_left: *u64,
    limit_left: *?u64,
    stats: ?*ScanStats,
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

    if (offset_left.* == 0 and limit_left.* == null and file.bytes == null) {
        if (try parallelParquetScan(allocator, t, file, wanted, where, stats, 65536)) |batch| return batch;
    }

    const batch_size = streamBatchSize(where != null, offset_left.*, limit_left.*);
    var rg_filter: RgFilter = .{};
    defer rg_filter.deinit();
    var br = try beginParquetBatch(reader, picked, batch_size, where, stats, &rg_filter);
    defer br.close();

    var parts: std.ArrayList(Batch) = .empty;
    while (true) {
        try checkCancel();
        const rb_val = (try br.next()) orelse break;
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
    while (true) {
        try checkCancel();
        const page = (try reader.nextPage(allocator, wanted, where)) orelse break;
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

fn expandDictColumn(
    allocator: std.mem.Allocator,
    reader: parquet.Reader,
    file_col: i32,
    name: []const u8,
    dict: parquet.RowBatch.DictCol,
) !Column {
    const meta = reader.column(file_col) orelse return error.ParquetOpenFailed;
    const logical = parquet.columnLogical(reader, file_col);
    const lid: parquet.LogicalId = @enumFromInt(logical.id);
    if (lid == .decimal or lid == .timestamp or lid == .uuid) return error.UnsupportedType;
    const n = dict.indices.len;
    switch (meta.physical_type) {
        .int32 => {
            const src = try loadDictFixed(i32, dict, allocator);
            defer allocator.free(src);
            const out = try allocator.alloc(i32, n);
            for (dict.indices, out) |ix, *slot| {
                if (ix >= src.len) return error.ParquetOpenFailed;
                slot.* = src[ix];
            }
            var col: Column = .{ .name = name, .data_type = .int32, .len = n, .i32s = out };
            col.valid = try copyArrowValid(allocator, dict.nulls, n);
            return col;
        },
        .int64 => {
            const src = try loadDictFixed(i64, dict, allocator);
            defer allocator.free(src);
            const out = try allocator.alloc(i64, n);
            for (dict.indices, out) |ix, *slot| {
                if (ix >= src.len) return error.ParquetOpenFailed;
                slot.* = src[ix];
            }
            var col: Column = .{ .name = name, .data_type = .int64, .len = n, .i64s = out };
            col.valid = try copyArrowValid(allocator, dict.nulls, n);
            return col;
        },
        .float => {
            const src = try loadDictFixed(f32, dict, allocator);
            defer allocator.free(src);
            const out = try allocator.alloc(f32, n);
            for (dict.indices, out) |ix, *slot| {
                if (ix >= src.len) return error.ParquetOpenFailed;
                slot.* = src[ix];
            }
            var col: Column = .{ .name = name, .data_type = .float32, .len = n, .f32s = out };
            col.valid = try copyArrowValid(allocator, dict.nulls, n);
            return col;
        },
        .double => {
            const src = try loadDictFixed(f64, dict, allocator);
            defer allocator.free(src);
            const out = try allocator.alloc(f64, n);
            for (dict.indices, out) |ix, *slot| {
                if (ix >= src.len) return error.ParquetOpenFailed;
                slot.* = src[ix];
            }
            var col: Column = .{ .name = name, .data_type = .float64, .len = n, .f64s = out };
            col.valid = try copyArrowValid(allocator, dict.nulls, n);
            return col;
        },
        .byte_array => {
            const offs = dict.offsets orelse return error.ParquetOpenFailed;
            var nbytes: usize = 0;
            for (dict.indices) |ix| {
                if (ix >= @as(usize, @intCast(dict.dict_count))) return error.ParquetOpenFailed;
                const at: usize = offs[ix];
                const len = std.mem.readInt(u32, dict.dict_ptr[at..][0..4], .little);
                nbytes += len;
            }
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, n + 1);
            var off: u32 = 0;
            for (dict.indices, 0..) |ix, row| {
                offsets[row] = off;
                const at: usize = offs[ix];
                const len = std.mem.readInt(u32, dict.dict_ptr[at..][0..4], .little);
                if (len > 0) @memcpy(bytes[off..][0..len], dict.dict_ptr[at + 4 ..][0..len]);
                off += len;
            }
            offsets[n] = off;
            var col: Column = .{
                .name = name,
                .data_type = .utf8,
                .len = n,
                .utf8 = .{ .offsets = offsets, .bytes = bytes },
            };
            col.valid = try copyArrowValid(allocator, dict.nulls, n);
            return col;
        },
        else => return error.UnsupportedType,
    }
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
        if (reader.columnRepLevel(file_col) > 0) {
            columns[out_i] = try fillListAsUtf8(allocator, reader, rb, @intCast(out_i), file_col, name);
            continue;
        }
        if (rb.columnDictionary(@intCast(out_i))) |dict| {
            columns[out_i] = try expandDictColumn(allocator, reader, file_col, name, dict);
            continue;
        }
        const vals = try rb.columnValues(@intCast(out_i));
        if (n_rows > 0 and vals.n < @as(i64, @intCast(n_rows))) return error.ParquetOpenFailed;
        var col = try fillColumnFromPtr(allocator, reader, file_col, name, n_rows, vals.ptr);
        col.valid = try copyArrowValid(allocator, vals.nulls, n_rows);
        columns[out_i] = col;
    }
    var len = n_rows;
    if (picked.len == 1 and reader.columnRepLevel(picked[0]) > 0) len = columns[0].len;
    return .{ .columns = columns, .len = len };
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
        if (reader.columnRepLevel(i) > 1) return error.UnsupportedNested;
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
                var spec = w.spec;
                if (spec.ref) |name| {
                    for (query.named_windows) |nw| {
                        if (std.ascii.eqlIgnoreCase(nw.name, name)) {
                            spec = nw.spec;
                            break;
                        }
                    }
                }
                if (w.arg) |arg| try addScanName(&names, allocator, arg);
                for (spec.partition_by) |p| try addScanName(&names, allocator, p);
                for (spec.order_by) |ob| try addScanName(&names, allocator, ob.column);
            },
            .scalar => |s| {
                try addScanName(&names, allocator, s.arg);
                if (s.coalesce_col) |c| try addScanName(&names, allocator, c);
            },
            .literal => {},
            .expr => |ex| try addScanNamesFromValue(&names, allocator, ex.expr),
            .case => |cs| {
                for (cs.arms) |arm| {
                    try addScanNamesFromBool(&names, allocator, arm.when);
                    try addScanNameFromCaseValue(&names, allocator, arm.then);
                }
                try addScanNameFromCaseValue(&names, allocator, cs.else_value);
            },
            .grouping => |g| try addScanName(&names, allocator, g.name),
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

fn addScanNameFromCaseValue(names: *std.ArrayList([]const u8), allocator: std.mem.Allocator, v: sql.CaseValue) !void {
    switch (v) {
        .column => |n| try addScanName(names, allocator, n),
        .literal, .null => {},
    }
}

fn addScanNamesFromValue(names: *std.ArrayList([]const u8), allocator: std.mem.Allocator, e: *const sql.Expr) !void {
    switch (e.*) {
        .literal => {},
        .column => |n| {
            const bare = if (std.mem.lastIndexOfScalar(u8, n, '.')) |d| n[d + 1 ..] else n;
            try addScanName(names, allocator, bare);
        },
        .agg => |a| if (a.arg) |arg| try addScanName(names, allocator, arg),
        .binary => |b| {
            try addScanNamesFromValue(names, allocator, b.left);
            try addScanNamesFromValue(names, allocator, b.right);
        },
        .unary_minus => |a| try addScanNamesFromValue(names, allocator, a),
        .call => |c| {
            for (c.args) |arg| try addScanNamesFromValue(names, allocator, arg);
        },
        .subquery => {},
    }
}

fn addScanNamesFromLeft(names: *std.ArrayList([]const u8), allocator: std.mem.Allocator, left: sql.CmpLeft) !void {
    switch (left) {
        .column => |n| try addScanName(names, allocator, n),
        .agg => |a| if (a.arg) |arg| try addScanName(names, allocator, arg),
    }
}

fn addScanNamesFromBool(names: *std.ArrayList([]const u8), allocator: std.mem.Allocator, expr: *const sql.BoolExpr) !void {
    switch (expr.*) {
        .cmp => |c| {
            try addScanNamesFromValue(names, allocator, c.left);
            try addScanNamesFromValue(names, allocator, c.right);
        },
        .isnull => |p| try addScanNamesFromLeft(names, allocator, p.left),
        .like => |p| try addScanNamesFromLeft(names, allocator, p.left),
        .in_list => |p| try addScanNamesFromLeft(names, allocator, p.left),
        .in_query => |p| try addScanNamesFromLeft(names, allocator, p.left),
        .cmp_query => |p| try addScanNamesFromValue(names, allocator, p.left),
        .between => |p| try addScanNamesFromLeft(names, allocator, p.left),
        .exists => {},
        .@"and", .@"or" => |b| {
            try addScanNamesFromBool(names, allocator, b.left);
            try addScanNamesFromBool(names, allocator, b.right);
        },
    }
}

const PScan = struct {
    path: []const u8,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    vec_size: i32,
    lo: i32,
    hi: i32,
    row_off: i64 = 0,
    row_len: i64 = 0,
    stats: ?*ScanStats,
    cancel: ?*CancelState,
    batch: Batch = .{ .columns = &.{}, .len = 0 },
    err: ?anyerror = null,
};

fn pScanJob(job: *PScan) error{Canceled}!void {
    bindCancel(job.cancel);
    bound_quiet = true;
    defer {
        bound_quiet = false;
        bindCancel(null);
    }
    pScanJobInner(job) catch |err| switch (err) {
        error.QueryCanceled => return error.Canceled,
        else => job.err = err,
    };
}

fn freeBatch(allocator: std.mem.Allocator, batch: Batch) void {
    freeBatchBuffers(allocator, batch, true);
}

fn freeBatchBuffers(allocator: std.mem.Allocator, batch: Batch, free_names: bool) void {
    if (batch.columns.len == 0) return;
    for (batch.columns) |col| {
        if (free_names and col.name.len > 0) allocator.free(col.name);
        if (col.bools.len > 0) allocator.free(col.bools);
        if (col.i32s.len > 0) allocator.free(col.i32s);
        if (col.i64s.len > 0) allocator.free(col.i64s);
        if (col.f32s.len > 0) allocator.free(col.f32s);
        if (col.f64s.len > 0) allocator.free(col.f64s);
        if (col.utf8.offsets.len > 0) allocator.free(col.utf8.offsets);
        if (col.utf8.bytes.len > 0) allocator.free(col.utf8.bytes);
        if (col.uuids.len > 0) allocator.free(col.uuids);
        if (col.i128s.len > 0) allocator.free(col.i128s);
        if (col.valid.len > 0) allocator.free(col.valid);
    }
    allocator.free(batch.columns);
}

fn pScanJobInner(job: *PScan) !void {
    const heap = std.heap.smp_allocator;
    var reader = try parquet.openLocalPath(heap, job.path);
    defer reader.close();
    const picked = try pickParquetColumns(heap, reader, job.wanted);
    defer heap.free(picked);
    const ranged = job.row_len > 0;
    const nrg: usize = if (ranged) 0 else @intCast(@max(reader.numRowGroups(), 0));
    var seen: []u8 = &.{};
    if (!ranged and job.where != null) seen = try heap.alloc(u8, nrg);
    defer if (seen.len > 0) heap.free(seen);
    if (seen.len > 0) @memset(seen, 0);
    var slice = RgSlice{
        .lo = job.lo,
        .hi = job.hi,
        .where = job.where,
        .stats = job.stats,
        .seen = seen,
    };
    var hook = parquet.RgHook{ .keep = keepRowGroupSlice, .user = &slice };
    var br = try reader.openBatchFilteredEx(picked, job.vec_size, if (ranged) null else &hook, false);
    if (ranged) try br.setRowRange(job.row_off, job.row_len);
    defer br.close();

    var parts: std.ArrayList(Batch) = .empty;
    var handed = false;
    defer if (!handed) {
        for (parts.items) |p| freeBatch(heap, p);
        parts.deinit(heap);
    };
    while (try br.next()) |rb_val| {
        try checkCancel();
        var rb = rb_val;
        defer rb.deinit();
        if (rb.numRows() <= 0) continue;
        const raw = try batchFromRowBatch(heap, reader, rb, picked);
        var vec = raw;
        if (job.where) |expr| {
            vec = try filterExpr(heap, raw, expr, null);
            if (vec.columns.ptr != raw.columns.ptr) freeBatchBuffers(heap, raw, false);
        }
        if (vec.len == 0) {
            freeBatch(heap, vec);
            continue;
        }
        try parts.append(heap, vec);
    }
    if (parts.items.len == 0) {
        job.batch = try emptyFromPicked(heap, reader, picked);
    } else if (parts.items.len == 1) {
        job.batch = parts.items[0];
    } else {
        job.batch = try concatBatches(heap, parts.items);
        for (parts.items) |p| freeBatch(heap, p);
    }
    handed = true;
    parts.deinit(heap);
}

fn parallelParquetScan(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    stats: ?*ScanStats,
    vec_size: i32,
) !?Batch {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.single_threaded) return null;
    if (file.bytes != null or aws.isRemote(file.path)) return null;
    const width = scanWidth();
    if (width < 2) return null;

    const plan = blk: {
        var reader = try parquet.openLocation(allocator, t, file.path);
        defer reader.close();
        const n = reader.numRowGroups();
        const picked = try pickParquetColumns(allocator, reader, wanted);
        defer allocator.free(picked);
        if (picked.len == 0) return null;
        const nrows = reader.numRows();
        const by_row = n < 2 and nrows >= 65536 and columnsHaveOffsetIndex(reader, picked);
        if (n < 2 and !by_row) return null;
        break :blk .{ .nrg = n, .nrows = nrows, .by_row = by_row };
    };

    const n: usize = if (plan.by_row) width else @min(width, @as(usize, @intCast(plan.nrg)));
    const jobs = try allocator.alloc(PScan, n);
    defer allocator.free(jobs);
    const heap = std.heap.smp_allocator;
    defer for (jobs) |job| freeBatch(heap, job.batch);

    if (plan.by_row) {
        const base: i64 = @divFloor(plan.nrows, @as(i64, @intCast(n)));
        var extra: i64 = @mod(plan.nrows, @as(i64, @intCast(n)));
        var off: i64 = 0;
        for (jobs) |*job| {
            const span = base + if (extra > 0) blk: {
                extra -= 1;
                break :blk @as(i64, 1);
            } else 0;
            job.* = .{
                .path = file.path,
                .wanted = wanted,
                .where = where,
                .vec_size = vec_size,
                .lo = 0,
                .hi = 0,
                .row_off = off,
                .row_len = span,
                .stats = stats,
                .cancel = bound_cancel,
            };
            off += span;
        }
    } else {
        const base: i32 = @intCast(@divFloor(plan.nrg, @as(i32, @intCast(n))));
        const extra: i32 = @intCast(@mod(plan.nrg, @as(i32, @intCast(n))));
        for (jobs, 0..) |*job, i| {
            const span = base + if (i < @as(usize, @intCast(extra))) @as(i32, 1) else 0;
            const lo: i32 = @as(i32, @intCast(i)) * base + @min(@as(i32, @intCast(i)), extra);
            job.* = .{
                .path = file.path,
                .wanted = wanted,
                .where = where,
                .vec_size = vec_size,
                .lo = lo,
                .hi = lo + span,
                .stats = stats,
                .cancel = bound_cancel,
            };
        }
    }

    var group: std.Io.Group = .init;
    var joined = false;
    defer if (!joined) group.cancel(t.io);
    for (jobs) |*job| {
        group.concurrent(t.io, pScanJob, .{job}) catch |err| switch (err) {
            error.ConcurrencyUnavailable => try pScanJobInner(job),
        };
    }
    group.await(t.io) catch |err| {
        joined = true;
        if (err == error.Canceled) return error.QueryCanceled;
        return err;
    };
    joined = true;
    for (jobs) |job| if (job.err) |e| return e;

    var parts: std.ArrayList(Batch) = .empty;
    defer parts.deinit(allocator);
    for (jobs) |job| {
        if (job.batch.columns.len == 0) continue;
        try parts.append(allocator, job.batch);
    }
    if (parts.items.len == 0) return null;
    return try concatBatches(allocator, parts.items);
}

fn scanParquet(
    allocator: std.mem.Allocator,
    t: vfs.Transport,
    file: iceberg.DataFile,
    wanted: ?[]const []const u8,
    where: ?*const sql.BoolExpr,
    stats: ?*ScanStats,
) !Batch {
    if (file.bytes == null) {
        if (try parallelParquetScan(allocator, t, file, wanted, where, stats, 65536)) |batch| return batch;
    }
    var reader = if (file.bytes) |b|
        try parquet.openMemory(allocator, b)
    else
        try parquet.openLocation(allocator, t, file.path);
    defer reader.close();

    const picked = try pickParquetColumns(allocator, reader, wanted);
    if (picked.len == 0) {
        return .{ .columns = &.{}, .len = @intCast(reader.numRows()) };
    }
    var rg_filter: RgFilter = .{};
    defer rg_filter.deinit();
    var br = try beginParquetBatch(reader, picked, 65536, where, stats, &rg_filter);
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

fn arrowBitIsNull(bitmap: ?[*]const u8, i: usize) bool {
    const bits = bitmap orelse return false;
    const byte = bits[i / 8];
    const bit: u8 = @as(u8, 1) << @intCast(i % 8);
    return byte & bit == 0;
}

fn fillListAsUtf8(
    allocator: std.mem.Allocator,
    reader: parquet.Reader,
    rb: parquet.RowBatch,
    batch_col: i32,
    file_col: i32,
    name: []const u8,
) !Column {
    const list = rb.columnList(batch_col) catch return error.UnsupportedNested;
    const n: usize = @intCast(@max(list.n_lists, 0));
    var col: Column = .{ .name = name, .data_type = .utf8, .len = n };
    col.utf8.offsets = try allocator.alloc(u32, n + 1);
    col.utf8.offsets[0] = 0;
    col.utf8.bytes = &.{};
    var any_null = false;
    const valid = try allocator.alloc(u8, n);
    @memset(valid, 1);
    const meta = reader.column(file_col) orelse return error.ParquetOpenFailed;
    var row: usize = 0;
    while (row < n) : (row += 1) {
        if (arrowBitIsNull(list.list_validity, row)) {
            valid[row] = 0;
            any_null = true;
            try appendUtf8(&col, "", row, allocator);
            continue;
        }
        const start: usize = @intCast(list.offsets[row]);
        const end: usize = @intCast(list.offsets[row + 1]);
        var buf: std.ArrayList(u8) = .empty;
        try buf.append(allocator, '[');
        var k = start;
        var first = true;
        while (k < end) : (k += 1) {
            if (!first) try buf.appendSlice(allocator, ", ");
            first = false;
            if (arrowBitIsNull(list.value_validity, k)) {
                try buf.appendSlice(allocator, "null");
                continue;
            }
            switch (meta.physical_type) {
                .int32 => {
                    const v = loadUnaligned(i32, list.values, k);
                    var tmp: [24]u8 = undefined;
                    const s = try std.fmt.bufPrint(&tmp, "{d}", .{v});
                    try buf.appendSlice(allocator, s);
                },
                .int64 => {
                    const v = loadUnaligned(i64, list.values, k);
                    var tmp: [32]u8 = undefined;
                    const s = try std.fmt.bufPrint(&tmp, "{d}", .{v});
                    try buf.appendSlice(allocator, s);
                },
                .byte_array => {
                    const ba = loadUnaligned(parquet.ByteArray, list.values, k);
                    try buf.append(allocator, '"');
                    if (ba.length > 0) try buf.appendSlice(allocator, ba.data[0..@intCast(ba.length)]);
                    try buf.append(allocator, '"');
                },
                else => return error.UnsupportedNested,
            }
        }
        try buf.append(allocator, ']');
        try appendUtf8(&col, buf.items, row, allocator);
    }
    col.valid = if (any_null) valid else &.{};
    return col;
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
        const ok = try evalBool(allocator, input, row, expr, query);
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

fn evalBool(allocator: std.mem.Allocator, input: Batch, row: usize, expr: *const sql.BoolExpr, query: ?sql.Query) !bool {
    return switch (expr.*) {
        .cmp => |c| try evalCmp(allocator, input, row, c, query),
        .isnull => |p| blk: {
            const idx = try resolveCmpLeft(input, p.left, query);
            const n = input.columns[idx].isNull(row);
            break :blk if (p.negated) !n else n;
        },
        .like => |p| blk: {
            const m = try evalLike(input, row, p, query);
            break :blk if (p.negated) !m else m;
        },
        .in_list => |p| blk: {
            const m = try evalInList(input, row, p, query);
            break :blk if (p.negated) !m else m;
        },
        .in_query, .cmp_query, .exists => error.UnsupportedSql,
        .between => |p| blk: {
            const m = try evalBetween(input, row, p, query);
            break :blk if (p.negated) !m else m;
        },
        .@"and" => |b| try evalBool(allocator, input, row, b.left, query) and try evalBool(allocator, input, row, b.right, query),
        .@"or" => |b| try evalBool(allocator, input, row, b.left, query) or try evalBool(allocator, input, row, b.right, query),
    };
}

fn evalLike(input: Batch, row: usize, pred: sql.Like, query: ?sql.Query) !bool {
    const idx = try resolveCmpLeft(input, pred.left, query);
    const col = input.columns[idx];
    if (col.isNull(row)) return false;
    if (col.data_type != .utf8) return error.TypeMismatch;
    return likeMatch(col.strAt(row), pred.pattern);
}

fn evalInList(input: Batch, row: usize, pred: sql.InList, query: ?sql.Query) !bool {
    const lhs = try cellFromCmpLeft(input, row, pred.left, query);
    if (lhs == .null) return false;
    for (pred.values) |lit| {
        if (lit == .null) continue;
        if (try cmpCells(.eq, lhs, cellFromLiteral(lit))) return true;
    }
    return false;
}

fn evalBetween(input: Batch, row: usize, pred: sql.Between, query: ?sql.Query) !bool {
    if (pred.lo == .null or pred.hi == .null) return false;
    const lhs = try cellFromCmpLeft(input, row, pred.left, query);
    if (lhs == .null) return false;
    const ge = try cmpCells(.ge, lhs, cellFromLiteral(pred.lo));
    const le = try cmpCells(.le, lhs, cellFromLiteral(pred.hi));
    return ge and le;
}

fn utf8Step(s: []const u8, i: usize) usize {
    if (i >= s.len) return 1;
    return std.unicode.utf8ByteSequenceLength(s[i]) catch 1;
}

fn likeMatch(str: []const u8, pat: []const u8) bool {
    var si: usize = 0;
    var pi: usize = 0;
    while (pi < pat.len) {
        if (pat[pi] == '%') {
            while (pi < pat.len and pat[pi] == '%') pi += 1;
            if (pi == pat.len) return true;
            while (si <= str.len) {
                if (likeMatch(str[si..], pat[pi..])) return true;
                if (si == str.len) break;
                si += utf8Step(str, si);
            }
            return false;
        }
        if (si >= str.len) return false;
        if (pat[pi] == '_') {
            si += utf8Step(str, si);
            pi += 1;
            continue;
        }
        const plen = utf8Step(pat, pi);
        const slen = utf8Step(str, si);
        if (plen != slen or pi + plen > pat.len or si + slen > str.len) return false;
        if (!std.mem.eql(u8, pat[pi..][0..plen], str[si..][0..slen])) return false;
        pi += plen;
        si += slen;
    }
    return si == str.len;
}

const CaseResolved = union(enum) {
    none,
    int: i64,
    float: f64,
    str: []const u8,
};

fn caseName(c: sql.Case) []const u8 {
    return c.alias orelse "case";
}

fn isIntDt(dt: DataType) bool {
    return dt == .int32 or dt == .int64;
}

fn isFloatDt(dt: DataType) bool {
    return dt == .float32 or dt == .float64;
}

fn unifyDt(a: DataType, b: DataType) !DataType {
    if (a == b) return a;
    if (isIntDt(a) and isIntDt(b)) return .int64;
    if ((isIntDt(a) or isFloatDt(a)) and (isIntDt(b) or isFloatDt(b))) return .float64;
    return error.TypeMismatch;
}

fn typeOfCaseValue(input: Batch, v: sql.CaseValue) !?DataType {
    return switch (v) {
        .null => null,
        .literal => |lit| switch (lit) {
            .null => null,
            .int => .int64,
            .float => .float64,
            .string => .utf8,
        },
        .column => |name| input.columns[try input.lookup(name)].data_type,
    };
}

fn unifyCaseType(input: Batch, c: sql.Case) !DataType {
    var acc: ?DataType = null;
    for (c.arms) |arm| {
        if (try typeOfCaseValue(input, arm.then)) |dt| {
            acc = if (acc) |cur| try unifyDt(cur, dt) else dt;
        }
    }
    if (try typeOfCaseValue(input, c.else_value)) |dt| {
        acc = if (acc) |cur| try unifyDt(cur, dt) else dt;
    }
    return acc orelse .int64;
}

fn resolveCaseValue(input: Batch, row: usize, v: sql.CaseValue) !CaseResolved {
    return switch (v) {
        .null => .none,
        .literal => |lit| switch (lit) {
            .null => .none,
            .int => |n| .{ .int = n },
            .float => |n| .{ .float = n },
            .string => |s| .{ .str = s },
        },
        .column => |name| blk: {
            const col = input.columns[try input.lookup(name)];
            if (col.isNull(row)) break :blk .none;
            break :blk switch (col.data_type) {
                .int32 => .{ .int = col.i32s[row] },
                .int64, .timestamp, .timestamptz => .{ .int = col.i64s[row] },
                .float32 => .{ .float = col.f32s[row] },
                .float64 => .{ .float = col.f64s[row] },
                .utf8 => .{ .str = col.strAt(row) },
                else => error.TypeMismatch,
            };
        },
    };
}

fn pickCase(allocator: std.mem.Allocator, input: Batch, row: usize, c: sql.Case) !CaseResolved {
    for (c.arms) |arm| {
        if (try evalBool(allocator, input, row, arm.when, null)) {
            return resolveCaseValue(input, row, arm.then);
        }
    }
    return resolveCaseValue(input, row, c.else_value);
}

fn resolvedAsI64(v: CaseResolved) !i64 {
    return switch (v) {
        .none => 0,
        .int => |n| n,
        .float => |n| @intFromFloat(n),
        .str => error.TypeMismatch,
    };
}

fn resolvedAsF64(v: CaseResolved) !f64 {
    return switch (v) {
        .none => 0,
        .int => |n| @floatFromInt(n),
        .float => |n| n,
        .str => error.TypeMismatch,
    };
}

fn evalCase(allocator: std.mem.Allocator, input: Batch, c: sql.Case) !Column {
    const n = input.len;
    const name = caseName(c);
    const dt = try unifyCaseType(input, c);
    var dst: Column = .{ .name = name, .data_type = dt, .len = n };
    const valid = try allocator.alloc(u8, n);
    var any_null = false;
    var row: usize = 0;
    if (dt == .utf8) {
        var strs = try allocator.alloc([]const u8, n);
        var nbytes: usize = 0;
        while (row < n) : (row += 1) {
            const picked = try pickCase(allocator, input, row, c);
            switch (picked) {
                .none => {
                    strs[row] = "";
                    valid[row] = 0;
                    any_null = true;
                },
                .str => |s| {
                    strs[row] = s;
                    valid[row] = 1;
                    nbytes += s.len;
                },
                else => return error.TypeMismatch,
            }
        }
        const bytes = try allocator.alloc(u8, nbytes);
        const offsets = try allocator.alloc(u32, n + 1);
        var off: u32 = 0;
        row = 0;
        while (row < n) : (row += 1) {
            offsets[row] = off;
            const s = strs[row];
            if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
            off += @intCast(s.len);
        }
        offsets[n] = off;
        dst.utf8 = .{ .offsets = offsets, .bytes = bytes };
    } else {
        try allocTyped(allocator, &dst, n);
        while (row < n) : (row += 1) {
            const picked = try pickCase(allocator, input, row, c);
            if (picked == .none) {
                valid[row] = 0;
                any_null = true;
                continue;
            }
            valid[row] = 1;
            switch (dt) {
                .int32 => dst.i32s[row] = @intCast(try resolvedAsI64(picked)),
                .int64, .timestamp, .timestamptz => dst.i64s[row] = try resolvedAsI64(picked),
                .float32 => dst.f32s[row] = @floatCast(try resolvedAsF64(picked)),
                .float64 => dst.f64s[row] = try resolvedAsF64(picked),
                else => return error.TypeMismatch,
            }
        }
    }
    dst.valid = if (any_null) valid else &.{};
    return dst;
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

fn evalCmp(allocator: std.mem.Allocator, input: Batch, row: usize, pred: sql.Cmp, query: ?sql.Query) !bool {
    const lhs = try evalValue(allocator, input, row, pred.left, query);
    const rhs = try evalValue(allocator, input, row, pred.right, query);
    return cmpCells(pred.op, lhs, rhs);
}

const Cell = union(enum) {
    null,
    bool: bool,
    i64: i64,
    f64: f64,
    utf8: []const u8,
    uuid: [16]u8,
    decimal: struct { v: i128, scale: i32 },
};

const EvalError = error{
    ColumnNotFound,
    AmbiguousColumn,
    TypeMismatch,
    InvalidSyntax,
    OutOfMemory,
    UnsupportedSql,
};

fn cellFromLiteral(lit: sql.Literal) Cell {
    return switch (lit) {
        .int => |v| .{ .i64 = v },
        .float => |v| .{ .f64 = v },
        .string => |s| .{ .utf8 = s },
        .null => .null,
    };
}

fn cellFromColumn(col: Column, row: usize) Cell {
    if (col.isNull(row)) return .null;
    return switch (col.data_type) {
        .boolean => .{ .bool = col.bools[row] != 0 },
        .int32 => .{ .i64 = col.i32s[row] },
        .int64, .timestamp, .timestamptz => .{ .i64 = col.i64s[row] },
        .float32 => .{ .f64 = col.f32s[row] },
        .float64 => .{ .f64 = col.f64s[row] },
        .utf8 => .{ .utf8 = col.strAt(row) },
        .uuid => .{ .uuid = col.uuids[row] },
        .decimal128 => .{ .decimal = .{ .v = col.i128s[row], .scale = col.decimal_scale } },
    };
}

fn cellFromCmpLeft(input: Batch, row: usize, left: sql.CmpLeft, query: ?sql.Query) !Cell {
    const idx = try resolveCmpLeft(input, left, query);
    return cellFromColumn(input.columns[idx], row);
}

fn evalValue(allocator: std.mem.Allocator, input: Batch, row: usize, expr: *const sql.Expr, query: ?sql.Query) EvalError!Cell {
    return switch (expr.*) {
        .literal => |lit| cellFromLiteral(lit),
        .column => |name| blk: {
            const idx = try input.lookup(name);
            break :blk cellFromColumn(input.columns[idx], row);
        },
        .agg => |a| blk: {
            const idx = try resolveCmpLeft(input, .{ .agg = .{ .kind = a.kind, .arg = a.arg } }, query);
            break :blk cellFromColumn(input.columns[idx], row);
        },
        .unary_minus => |arg| blk: {
            const v = try evalValue(allocator, input, row, arg, query);
            break :blk try cellNeg(v);
        },
        .binary => |b| blk: {
            const l = try evalValue(allocator, input, row, b.left, query);
            const r = try evalValue(allocator, input, row, b.right, query);
            break :blk try cellArith(b.op, l, r);
        },
        .call => |c| try evalCall(allocator, input, row, c, query),
        .subquery => error.UnsupportedSql,
    };
}

fn cellNeg(v: Cell) !Cell {
    return switch (v) {
        .null => .null,
        .i64 => |n| .{ .i64 = -n },
        .f64 => |n| .{ .f64 = -n },
        .decimal => |d| .{ .decimal = .{ .v = -d.v, .scale = d.scale } },
        else => error.TypeMismatch,
    };
}

fn asF64(v: Cell) !?f64 {
    return switch (v) {
        .null => null,
        .i64 => |n| @floatFromInt(n),
        .f64 => |n| n,
        .decimal => |d| @as(f64, @floatFromInt(d.v)) / @as(f64, @floatFromInt(pow10i128(d.scale))),
        else => error.TypeMismatch,
    };
}

fn asI64(v: Cell) !?i64 {
    return switch (v) {
        .null => null,
        .i64 => |n| n,
        .f64 => |n| @intFromFloat(n),
        else => error.TypeMismatch,
    };
}

fn cellArith(op: sql.ArithOp, lhs: Cell, rhs: Cell) !Cell {
    if (lhs == .null or rhs == .null) return .null;
    const use_float = lhs == .f64 or rhs == .f64 or lhs == .decimal or rhs == .decimal or op == .div;
    if (op == .mod) {
        const a = try asI64(lhs) orelse return .null;
        const b = try asI64(rhs) orelse return .null;
        if (b == 0) return error.TypeMismatch;
        return .{ .i64 = @rem(a, b) };
    }
    if (use_float) {
        const a = try asF64(lhs) orelse return .null;
        const b = try asF64(rhs) orelse return .null;
        if (op == .div and b == 0) return .null;
        const out: f64 = switch (op) {
            .add => a + b,
            .sub => a - b,
            .mul => a * b,
            .div => a / b,
            .mod => return error.TypeMismatch,
        };
        return .{ .f64 = out };
    }
    const a = try asI64(lhs) orelse return .null;
    const b = try asI64(rhs) orelse return .null;
    const out: i64 = switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => if (b == 0) return .null else @divTrunc(a, b),
        .mod => return error.TypeMismatch,
    };
    return .{ .i64 = out };
}

fn evalCall(allocator: std.mem.Allocator, input: Batch, row: usize, c: sql.Call, query: ?sql.Query) EvalError!Cell {
    if (c.args.len == 0) return error.InvalidSyntax;
    const a0 = try evalValue(allocator, input, row, c.args[0], query);
    switch (c.kind) {
        .abs => return switch (a0) {
            .null => .null,
            .i64 => |n| .{ .i64 = absI64(n) },
            .f64 => |n| .{ .f64 = @abs(n) },
            .decimal => |d| .{ .decimal = .{ .v = if (d.v < 0) -d.v else d.v, .scale = d.scale } },
            else => error.TypeMismatch,
        },
        .round => return switch (a0) {
            .null => .null,
            .i64 => a0,
            .f64 => |n| .{ .f64 = @round(n) },
            else => error.TypeMismatch,
        },
        .cast => {
            const ty = c.cast_type orelse return error.InvalidSyntax;
            return castCell(a0, ty);
        },
        .coalesce => {
            var i: usize = 0;
            while (i < c.args.len) : (i += 1) {
                const v = try evalValue(allocator, input, row, c.args[i], query);
                if (v != .null) return v;
            }
            return .null;
        },
        .lower => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const out = try allocator.alloc(u8, s.len);
            for (s, 0..) |ch, i| {
                out[i] = if (ch >= 'A' and ch <= 'Z') ch + 32 else ch;
            }
            return .{ .utf8 = out };
        },
        .substr => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const start_v = try evalValue(allocator, input, row, c.args[1], query);
            const start = try asI64(start_v) orelse return .null;
            var count: ?i64 = null;
            if (c.args.len > 2) {
                const len_v = try evalValue(allocator, input, row, c.args[2], query);
                count = try asI64(len_v) orelse return .null;
            }
            return .{ .utf8 = substrUtf8(s, start, count) };
        },
        .concat => {
            var buf: std.ArrayList(u8) = .empty;
            for (c.args) |arg| {
                const v = try evalValue(allocator, input, row, arg, query);
                const s = switch (v) {
                    .null => return .null,
                    .utf8 => |s| s,
                    else => return error.TypeMismatch,
                };
                try buf.appendSlice(allocator, s);
            }
            return .{ .utf8 = try buf.toOwnedSlice(allocator) };
        },
        .date_trunc => {
            const unit = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const ts = try evalValue(allocator, input, row, c.args[1], query);
            const micros = try asI64(ts) orelse return .null;
            return .{ .i64 = try dateTruncMicros(unit, micros) };
        },
        .upper => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const out = try allocator.alloc(u8, s.len);
            for (s, 0..) |ch, i| {
                out[i] = if (ch >= 'a' and ch <= 'z') ch - 32 else ch;
            }
            return .{ .utf8 = out };
        },
        .length => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            return .{ .i64 = @intCast(s.len) };
        },
        .trim, .ltrim, .rtrim => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            return .{ .utf8 = trimAscii(s, c.kind) };
        },
        .replace => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const from_v = try evalValue(allocator, input, row, c.args[1], query);
            const to_v = try evalValue(allocator, input, row, c.args[2], query);
            const from = switch (from_v) {
                .null => return .null,
                .utf8 => |x| x,
                else => return error.TypeMismatch,
            };
            const to = switch (to_v) {
                .null => return .null,
                .utf8 => |x| x,
                else => return error.TypeMismatch,
            };
            return .{ .utf8 = try replaceAll(allocator, s, from, to) };
        },
        .extract => {
            const unit = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const ts = try evalValue(allocator, input, row, c.args[1], query);
            const micros = try asI64(ts) orelse return .null;
            return .{ .i64 = try extractField(unit, micros) };
        },
        .year, .month, .day, .hour, .minute, .second => {
            const micros = try asI64(a0) orelse return .null;
            const unit: []const u8 = switch (c.kind) {
                .year => "year",
                .month => "month",
                .day => "day",
                .hour => "hour",
                .minute => "minute",
                .second => "second",
                else => unreachable,
            };
            return .{ .i64 = try extractField(unit, micros) };
        },
        .left, .right => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const n_v = try evalValue(allocator, input, row, c.args[1], query);
            const n = try asI64(n_v) orelse return .null;
            return .{ .utf8 = leftRightUtf8(s, n, c.kind == .right) };
        },
        .starts_with, .ends_with, .contains => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const p_v = try evalValue(allocator, input, row, c.args[1], query);
            const p = switch (p_v) {
                .null => return .null,
                .utf8 => |x| x,
                else => return error.TypeMismatch,
            };
            const ok: bool = switch (c.kind) {
                .starts_with => std.mem.startsWith(u8, s, p),
                .ends_with => std.mem.endsWith(u8, s, p),
                .contains => std.mem.indexOf(u8, s, p) != null,
                else => unreachable,
            };
            return .{ .bool = ok };
        },
        .position => {
            const s = switch (a0) {
                .null => return .null,
                .utf8 => |s| s,
                else => return error.TypeMismatch,
            };
            const p_v = try evalValue(allocator, input, row, c.args[1], query);
            const p = switch (p_v) {
                .null => return .null,
                .utf8 => |x| x,
                else => return error.TypeMismatch,
            };
            if (p.len == 0) return .{ .i64 = 1 };
            if (std.mem.indexOf(u8, s, p)) |i| return .{ .i64 = @intCast(i + 1) };
            return .{ .i64 = 0 };
        },
        .ceil, .floor => {
            if (a0 == .null) return .null;
            if (a0 == .i64) return a0;
            const n = try asF64(a0) orelse return .null;
            const out = if (c.kind == .ceil) @ceil(n) else @floor(n);
            return .{ .f64 = out };
        },
        .sign => {
            if (a0 == .null) return .null;
            const n = try asF64(a0) orelse return .null;
            const s: i64 = if (n < 0) -1 else if (n > 0) 1 else 0;
            return .{ .i64 = s };
        },
        .greatest, .least => {
            var best: ?Cell = null;
            const want_min = c.kind == .least;
            for (c.args) |arg| {
                const v = try evalValue(allocator, input, row, arg, query);
                if (v == .null) continue;
                if (best) |b| {
                    const ord = try cellOrd(b, v);
                    if (want_min) {
                        if (ord == .gt) best = v;
                    } else if (ord == .lt) best = v;
                } else {
                    best = v;
                }
            }
            return best orelse .null;
        },
    }
}

fn leftRightUtf8(s: []const u8, n: i64, from_right: bool) []const u8 {
    if (n <= 0 or s.len == 0) return s[0..0];
    const take: usize = @intCast(n);
    if (take >= s.len) return s;
    if (from_right) return s[s.len - take ..];
    return s[0..take];
}

fn cellOrd(a: Cell, b: Cell) EvalError!std.math.Order {
    if (a == .utf8 and b == .utf8) return std.mem.order(u8, a.utf8, b.utf8);
    const af = asF64(a) catch return error.TypeMismatch;
    const bf = asF64(b) catch return error.TypeMismatch;
    const x = af orelse return error.TypeMismatch;
    const y = bf orelse return error.TypeMismatch;
    if (x < y) return .lt;
    if (x > y) return .gt;
    return .eq;
}

fn isAsciiSpace(ch: u8) bool {
    return ch == ' ' or ch == '\t' or ch == '\n' or ch == '\r';
}

fn trimAscii(s: []const u8, kind: sql.ScalarKind) []const u8 {
    var a: usize = 0;
    var b: usize = s.len;
    if (kind == .trim or kind == .ltrim) {
        while (a < b and isAsciiSpace(s[a])) a += 1;
    }
    if (kind == .trim or kind == .rtrim) {
        while (b > a and isAsciiSpace(s[b - 1])) b -= 1;
    }
    return s[a..b];
}

fn replaceAll(allocator: std.mem.Allocator, s: []const u8, from: []const u8, to: []const u8) ![]const u8 {
    if (from.len == 0) return s;
    var buf: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < s.len) {
        if (i + from.len <= s.len and std.mem.eql(u8, s[i..][0..from.len], from)) {
            try buf.appendSlice(allocator, to);
            i += from.len;
        } else {
            try buf.append(allocator, s[i]);
            i += 1;
        }
    }
    return buf.toOwnedSlice(allocator);
}

fn extractField(unit: []const u8, micros: i64) EvalError!i64 {
    const days = @divFloor(micros, us_per_day);
    const civil = civilFromUnixDays(days);
    if (eqlUnit(unit, "year") or eqlUnit(unit, "years")) return civil.year;
    if (eqlUnit(unit, "month") or eqlUnit(unit, "months")) return @intCast(civil.month);
    if (eqlUnit(unit, "day") or eqlUnit(unit, "days")) return @intCast(civil.day);
    const tod = micros - days * us_per_day;
    if (eqlUnit(unit, "hour") or eqlUnit(unit, "hours")) return @divFloor(tod, us_per_hour);
    if (eqlUnit(unit, "minute") or eqlUnit(unit, "minutes")) return @divFloor(@mod(tod, us_per_hour), us_per_minute);
    if (eqlUnit(unit, "second") or eqlUnit(unit, "seconds")) return @divFloor(@mod(tod, us_per_minute), us_per_second);
    return error.InvalidSyntax;
}

fn substrUtf8(s: []const u8, start: i64, count: ?i64) []const u8 {
    if (s.len == 0) return s;
    const begin: usize = if (start <= 1) 0 else blk: {
        const i: usize = @intCast(start - 1);
        break :blk if (i > s.len) s.len else i;
    };
    if (begin >= s.len) return s[s.len..];
    if (count) |n| {
        if (n <= 0) return s[begin..begin];
        const take: usize = @intCast(n);
        const end = @min(s.len, begin + take);
        return s[begin..end];
    }
    return s[begin..];
}

const us_per_second: i64 = 1_000_000;
const us_per_minute: i64 = 60 * us_per_second;
const us_per_hour: i64 = 60 * us_per_minute;
const us_per_day: i64 = 24 * us_per_hour;

fn dateTruncMicros(unit: []const u8, micros: i64) EvalError!i64 {
    if (eqlUnit(unit, "second") or eqlUnit(unit, "seconds"))
        return @divFloor(micros, us_per_second) * us_per_second;
    if (eqlUnit(unit, "minute") or eqlUnit(unit, "minutes"))
        return @divFloor(micros, us_per_minute) * us_per_minute;
    if (eqlUnit(unit, "hour") or eqlUnit(unit, "hours"))
        return @divFloor(micros, us_per_hour) * us_per_hour;
    if (eqlUnit(unit, "day") or eqlUnit(unit, "days"))
        return @divFloor(micros, us_per_day) * us_per_day;
    const days = @divFloor(micros, us_per_day);
    const civil = civilFromUnixDays(days);
    if (eqlUnit(unit, "month") or eqlUnit(unit, "months"))
        return unixDaysFromCivil(civil.year, civil.month, 1) * us_per_day;
    if (eqlUnit(unit, "year") or eqlUnit(unit, "years"))
        return unixDaysFromCivil(civil.year, 1, 1) * us_per_day;
    return error.InvalidSyntax;
}

fn eqlUnit(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn civilFromUnixDays(z: i64) struct { year: i64, month: u32, day: u32 } {
    const z0 = z + 719468;
    const era = @divFloor(z0, 146097);
    const doe: i64 = z0 - era * 146097;
    const yoe: i64 = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    var y = yoe + era * 400;
    const doy: i64 = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp: i64 = @divFloor(5 * doy + 2, 153);
    const d: i64 = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m: i64 = mp + (if (mp < 10) @as(i64, 3) else -9);
    if (m <= 2) y += 1;
    return .{ .year = y, .month = @intCast(m), .day = @intCast(d) };
}

fn unixDaysFromCivil(y: i64, m: u32, d: u32) i64 {
    var yy = y;
    const mm: i64 = m;
    if (mm <= 2) yy -= 1;
    const era = @divFloor(yy, 400);
    const yoe = yy - era * 400;
    const doy = @divFloor(153 * (mm + (if (mm > 2) @as(i64, -3) else 9)) + 2, 5) + @as(i64, d) - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn castCell(v: Cell, ty: sql.CastType) !Cell {
    if (v == .null) return .null;
    return switch (ty) {
        .int64 => .{ .i64 = try asI64(v) orelse return .null },
        .float64 => .{ .f64 = try asF64(v) orelse return .null },
        .boolean => switch (v) {
            .bool => |b| .{ .bool = b },
            .i64 => |n| .{ .bool = n != 0 },
            else => error.TypeMismatch,
        },
        .utf8 => error.TypeMismatch,
    };
}

fn cmpCells(op: sql.CmpOp, lhs: Cell, rhs: Cell) !bool {
    if (lhs == .null or rhs == .null) return false;
    if (lhs == .uuid or rhs == .uuid) {
        const a = switch (lhs) {
            .uuid => |u| u,
            .utf8 => |s| try parseUuid(s),
            else => return error.TypeMismatch,
        };
        const b = switch (rhs) {
            .uuid => |u| u,
            .utf8 => |s| try parseUuid(s),
            else => return error.TypeMismatch,
        };
        const ord = std.mem.order(u8, &a, &b);
        return switch (op) {
            .eq => ord == .eq,
            .ne => ord != .eq,
            .lt => ord == .lt,
            .le => ord != .gt,
            .gt => ord == .gt,
            .ge => ord != .lt,
        };
    }
    if (lhs == .utf8 or rhs == .utf8) {
        const a = switch (lhs) {
            .utf8 => |s| s,
            else => return error.TypeMismatch,
        };
        const b = switch (rhs) {
            .utf8 => |s| s,
            else => return error.TypeMismatch,
        };
        const ord = std.mem.order(u8, a, b);
        return switch (op) {
            .eq => ord == .eq,
            .ne => ord != .eq,
            .lt => ord == .lt,
            .le => ord != .gt,
            .gt => ord == .gt,
            .ge => ord != .lt,
        };
    }
    const use_float = lhs == .f64 or rhs == .f64 or lhs == .decimal or rhs == .decimal;
    if (use_float) {
        const a = try asF64(lhs) orelse return false;
        const b = try asF64(rhs) orelse return false;
        return switch (op) {
            .eq => a == b,
            .ne => a != b,
            .lt => a < b,
            .le => a <= b,
            .gt => a > b,
            .ge => a >= b,
        };
    }
    const a = try asI64(lhs) orelse return false;
    const b = try asI64(rhs) orelse return false;
    return cmpOrd(a, op, b);
}

fn cellType(v: Cell) DataType {
    return switch (v) {
        .null, .i64 => .int64,
        .bool => .boolean,
        .f64 => .float64,
        .utf8 => .utf8,
        .uuid => .uuid,
        .decimal => .decimal128,
    };
}

fn evalComputed(allocator: std.mem.Allocator, input: Batch, c: sql.Computed, query: sql.Query) !Column {
    const name = c.alias orelse c.sql_text;
    var ty: DataType = .int64;
    var row: usize = 0;
    while (row < input.len) : (row += 1) {
        const v = try evalValue(allocator, input, row, c.expr, query);
        if (v != .null) {
            ty = cellType(v);
            break;
        }
    }
    var dst: Column = .{ .name = name, .data_type = ty, .len = input.len };
    const valid = try allocator.alloc(u8, input.len);
    switch (ty) {
        .int64, .timestamp, .timestamptz => dst.i64s = try allocator.alloc(i64, input.len),
        .float64 => dst.f64s = try allocator.alloc(f64, input.len),
        .boolean => dst.bools = try allocator.alloc(u8, input.len),
        .utf8 => dst.utf8 = .{
            .offsets = try allocator.alloc(u32, input.len + 1),
            .bytes = &.{},
        },
        .uuid => dst.uuids = try allocator.alloc([16]u8, input.len),
        .decimal128 => dst.i128s = try allocator.alloc(i128, input.len),
        else => return error.TypeMismatch,
    }
    var utf8_bytes: std.ArrayList(u8) = .empty;
    row = 0;
    while (row < input.len) : (row += 1) {
        const v = try evalValue(allocator, input, row, c.expr, query);
        if (v == .null) {
            valid[row] = 0;
            switch (ty) {
                .int64, .timestamp, .timestamptz => dst.i64s[row] = 0,
                .float64 => dst.f64s[row] = 0,
                .boolean => dst.bools[row] = 0,
                .utf8 => dst.utf8.offsets[row] = @intCast(utf8_bytes.items.len),
                .uuid => dst.uuids[row] = @splat(0),
                .decimal128 => dst.i128s[row] = 0,
                else => {},
            }
            continue;
        }
        valid[row] = 1;
        switch (ty) {
            .int64, .timestamp, .timestamptz => dst.i64s[row] = try asI64(v) orelse 0,
            .float64 => dst.f64s[row] = try asF64(v) orelse 0,
            .boolean => dst.bools[row] = switch (v) {
                .bool => |b| if (b) 1 else 0,
                .i64 => |n| if (n != 0) 1 else 0,
                else => return error.TypeMismatch,
            },
            .utf8 => {
                dst.utf8.offsets[row] = @intCast(utf8_bytes.items.len);
                try utf8_bytes.appendSlice(allocator, switch (v) {
                    .utf8 => |s| s,
                    else => return error.TypeMismatch,
                });
            },
            .uuid => dst.uuids[row] = switch (v) {
                .uuid => |u| u,
                else => return error.TypeMismatch,
            },
            .decimal128 => {
                const d = switch (v) {
                    .decimal => |x| x,
                    else => return error.TypeMismatch,
                };
                dst.i128s[row] = d.v;
                dst.decimal_scale = d.scale;
            },
            else => return error.TypeMismatch,
        }
    }
    if (ty == .utf8) {
        dst.utf8.offsets[input.len] = @intCast(utf8_bytes.items.len);
        dst.utf8.bytes = try utf8_bytes.toOwnedSlice(allocator);
    }
    var any_null = false;
    for (valid) |b| {
        if (b == 0) {
            any_null = true;
            break;
        }
    }
    dst.valid = if (any_null) valid else &.{};
    return dst;
}

fn evalGroupComputed(
    allocator: std.mem.Allocator,
    input: Batch,
    first_rows: []const usize,
    c: sql.Computed,
    query: sql.Query,
) !Column {
    const subset = try gatherRows(allocator, input, first_rows);
    return evalComputed(allocator, subset, c, query);
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

fn identTail(name: []const u8) []const u8 {
    return if (std.mem.lastIndexOfScalar(u8, name, '.')) |d| name[d + 1 ..] else name;
}

fn identQual(name: []const u8) ?[]const u8 {
    const d = std.mem.lastIndexOfScalar(u8, name, '.') orelse return null;
    if (d == 0) return null;
    return name[0..d];
}

fn sameGroupKey(group_key: []const u8, qual: ?[]const u8, name: []const u8) bool {
    if (!std.ascii.eqlIgnoreCase(identTail(group_key), name)) return false;
    const gq = identQual(group_key);
    if (gq == null or qual == null) return true;
    return std.ascii.eqlIgnoreCase(gq.?, qual.?);
}

fn groupHas(query: sql.Query, qual: ?[]const u8, name: []const u8) bool {
    for (query.group_by) |g| {
        if (sameGroupKey(g, qual, name)) return true;
    }
    return false;
}

fn inGroupBy(query: sql.Query, name: []const u8) bool {
    return groupHas(query, identQual(name), identTail(name));
}

fn inGroupedOutput(query: sql.Query, name: []const u8) bool {
    if (inGroupBy(query, name)) return true;
    for (query.items) |item| {
        switch (item) {
            .column => |c| {
                if (std.ascii.eqlIgnoreCase(c.name, name)) return true;
                if (c.alias) |a| {
                    if (std.ascii.eqlIgnoreCase(a, name)) return true;
                }
            },
            .agg => |a| {
                if (a.alias) |al| {
                    if (std.ascii.eqlIgnoreCase(al, name)) return true;
                }
            },
            .grouping => |g| {
                if (std.ascii.eqlIgnoreCase(g.name, name)) return true;
                if (g.alias) |al| {
                    if (std.ascii.eqlIgnoreCase(al, name)) return true;
                }
            },
            else => {},
        }
    }
    return false;
}

fn validateWindowInAgg(query: sql.Query, w: sql.Window) !void {
    if (w.arg) |arg| {
        if (!inGroupedOutput(query, arg)) return error.InvalidSyntax;
    }
    for (w.spec.partition_by) |p| {
        if (!inGroupedOutput(query, p)) return error.InvalidSyntax;
    }
    for (w.spec.order_by) |ob| {
        if (!inGroupedOutput(query, ob.column)) return error.InvalidSyntax;
    }
}

fn validateAgg(query: sql.Query) !void {
    if (query.isStar() and query.needsAgg()) return error.UnsupportedSql;
    if (query.hasAgg() and query.group_by.len == 0) {
        for (query.items) |item| {
            switch (item) {
                .agg, .window => {},
                else => return error.InvalidSyntax,
            }
        }
        return;
    }
    for (query.items) |item| {
        switch (item) {
            .star => return error.UnsupportedSql,
            .column => |c| if (!groupHas(query, c.qualifier, c.name)) return error.InvalidSyntax,
            .scalar => return error.UnsupportedSql,
            .literal => return error.UnsupportedSql,
            .case => return error.UnsupportedSql,
            .expr => {},
            .agg => {},
            .window => |w| try validateWindowInAgg(query, w),
            .grouping => |g| if (!inGroupBy(query, g.name)) return error.InvalidSyntax,
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
    distinct: bool = false,
    seen: std.StringHashMapUnmanaged(void) = .empty,
};

const Group = struct {
    first_row: usize,
    states: []AggState,
    set_i: usize = 0,
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
        if (state.kind != .count or state.distinct) return error.InvalidSyntax;
        state.count += 1;
        return;
    };
    if (c.isNull(row)) return;
    if (state.distinct) {
        const a = str_alloc orelse return error.InvalidSyntax;
        const key = try encodeColKey(a, c, row);
        const gop = try state.seen.getOrPut(a, key);
        if (gop.found_existing) {
            a.free(key);
            return;
        }
    }
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

fn encodeColKey(allocator: std.mem.Allocator, col: Column, row: usize) ![]u8 {
    var cols = [_]Column{col};
    const input = Batch{ .columns = &cols, .len = row + 1 };
    const idxs = [_]usize{0};
    return encodeKey(allocator, input, row, &idxs);
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

fn aggOutLen(query: sql.Query) usize {
    var n: usize = 0;
    if (query.hasWindow()) n += query.group_by.len;
    for (query.items) |item| {
        if (item != .window) n += 1;
    }
    return n;
}

fn makeStates(allocator: std.mem.Allocator, query: sql.Query) ![]AggState {
    const n = countAggs(query);
    const states = try allocator.alloc(AggState, n);
    var i: usize = 0;
    for (query.items) |item| {
        if (item == .agg) {
            states[i] = .{ .kind = item.agg.kind, .distinct = item.agg.distinct };
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

fn setHasName(set: []const []const u8, name: []const u8) bool {
    for (set) |g| {
        if (sameGroupKey(g, identQual(name), identTail(name))) return true;
    }
    return false;
}

fn exprCoveredBySet(e: *const sql.Expr, set: []const []const u8) bool {
    return switch (e.*) {
        .literal => true,
        .column => |n| setHasName(set, n),
        .binary => |b| exprCoveredBySet(b.left, set) and exprCoveredBySet(b.right, set),
        .unary_minus => |arg| exprCoveredBySet(arg, set),
        .call => |c| blk: {
            for (c.args) |arg| if (!exprCoveredBySet(arg, set)) break :blk false;
            break :blk true;
        },
        .agg, .subquery => true,
    };
}

fn encodeSetKey(
    allocator: std.mem.Allocator,
    input: Batch,
    row: usize,
    set_i: usize,
    idxs: []const usize,
) ![]u8 {
    const body = try encodeKey(allocator, input, row, idxs);
    defer allocator.free(body);
    var buf: std.ArrayList(u8) = .empty;
    var prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &prefix, @intCast(set_i), .little);
    try buf.appendSlice(allocator, &prefix);
    try buf.appendSlice(allocator, body);
    return buf.toOwnedSlice(allocator);
}

fn nullsLike(allocator: std.mem.Allocator, src: Column, n: usize, name: []const u8) !Column {
    var dst: Column = .{
        .name = name,
        .data_type = src.data_type,
        .len = n,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    try allocTyped(allocator, &dst, n);
    if (src.data_type == .utf8) {
        dst.utf8.offsets = try allocator.alloc(u32, n + 1);
        @memset(dst.utf8.offsets, 0);
        dst.utf8.bytes = &.{};
    }
    if (n > 0) {
        dst.valid = try allocator.alloc(u8, n);
        @memset(dst.valid, 0);
    }
    return dst;
}

fn groupingColumn(allocator: std.mem.Allocator, name: []const u8, n: usize, bits: []const u8) !Column {
    const i64s = try allocator.alloc(i64, n);
    if (bits.len == n) {
        for (bits, 0..) |bit, i| i64s[i] = bit;
    } else if (n > 0) {
        @memset(i64s, 0);
    }
    return .{ .name = name, .data_type = .int64, .len = n, .i64s = i64s };
}

fn aggregateGroupingSets(allocator: std.mem.Allocator, input: Batch, query: sql.Query) !Batch {
    try validateAgg(query);

    var groups: std.StringArrayHashMapUnmanaged(Group) = .empty;
    if (input.len == 0) {
        for (query.grouping_sets, 0..) |set, set_i| {
            if (set.len != 0) continue;
            const states = try makeStates(allocator, query);
            const key = try allocator.alloc(u8, 4);
            std.mem.writeInt(u32, key[0..4], @intCast(set_i), .little);
            try groups.put(allocator, key, .{ .first_row = 0, .states = states, .set_i = set_i });
        }
    } else {
        const idx_per_set = try allocator.alloc([]const usize, query.grouping_sets.len);
        for (query.grouping_sets, 0..) |set, si| {
            const idxs = try allocator.alloc(usize, set.len);
            for (set, 0..) |name, i| idxs[i] = try input.lookup(name);
            idx_per_set[si] = idxs;
        }
        var row: usize = 0;
        while (row < input.len) : (row += 1) {
            for (idx_per_set, 0..) |idxs, set_i| {
                const encoded = try encodeSetKey(allocator, input, row, set_i, idxs);
                if (groups.getPtr(encoded)) |g| {
                    try feedRow(g.states, input, query, row, allocator);
                    allocator.free(encoded);
                } else {
                    const states = try makeStates(allocator, query);
                    try feedRow(states, input, query, row, allocator);
                    try groups.put(allocator, encoded, .{ .first_row = row, .states = states, .set_i = set_i });
                }
            }
        }
    }

    const n_groups = groups.count();
    const first_rows = try allocator.alloc(usize, n_groups);
    for (groups.values(), 0..) |g, i| first_rows[i] = g.first_row;
    const blank_keys = input.len == 0 and n_groups > 0;

    const out_cols = try allocator.alloc(Column, aggOutLen(query));
    var col_i: usize = 0;
    if (query.hasWindow()) {
        for (query.group_by) |gname| {
            const src_i = try input.lookup(gname);
            if (blank_keys) {
                out_cols[col_i] = try nullsLike(allocator, input.columns[src_i], n_groups, gname);
            } else {
                var col = try copyKeyColumn(allocator, input.columns[src_i], first_rows, gname);
                for (groups.values(), 0..) |g, gi| {
                    if (!setHasName(query.grouping_sets[g.set_i], gname)) try markNull(&col, gi, allocator);
                }
                out_cols[col_i] = col;
            }
            col_i += 1;
        }
    }
    var agg_i: usize = 0;
    for (query.items) |item| {
        switch (item) {
            .star, .scalar, .literal, .case => return error.UnsupportedSql,
            .window => {},
            .expr => |ex| {
                if (blank_keys) {
                    const template = try evalGroupComputed(allocator, input, &.{}, ex, query);
                    const name = ex.alias orelse template.name;
                    out_cols[col_i] = try nullsLike(allocator, template, n_groups, name);
                } else {
                    var col = try evalGroupComputed(allocator, input, first_rows, ex, query);
                    for (groups.values(), 0..) |g, gi| {
                        if (!exprCoveredBySet(ex.expr, query.grouping_sets[g.set_i])) {
                            try markNull(&col, gi, allocator);
                        }
                    }
                    out_cols[col_i] = col;
                }
                col_i += 1;
            },
            .column => |c| {
                const key = if (c.qualifier) |q|
                    try std.fmt.allocPrint(allocator, "{s}.{s}", .{ q, c.name })
                else
                    c.name;
                const src_i = try input.lookup(key);
                const name = c.alias orelse key;
                if (blank_keys) {
                    out_cols[col_i] = try nullsLike(allocator, input.columns[src_i], n_groups, name);
                } else {
                    var col = try copyKeyColumn(allocator, input.columns[src_i], first_rows, name);
                    for (groups.values(), 0..) |g, gi| {
                        if (!setHasName(query.grouping_sets[g.set_i], key)) try markNull(&col, gi, allocator);
                    }
                    out_cols[col_i] = col;
                }
                col_i += 1;
            },
            .grouping => |g| {
                const bits = try allocator.alloc(u8, n_groups);
                if (blank_keys) {
                    @memset(bits, 1);
                } else {
                    for (groups.values(), 0..) |grp, gi| {
                        bits[gi] = if (setHasName(query.grouping_sets[grp.set_i], g.name)) 0 else 1;
                    }
                }
                out_cols[col_i] = try groupingColumn(allocator, g.alias orelse g.name, n_groups, bits);
                col_i += 1;
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
                    for (groups.values(), 0..) |grp, gi| {
                        try writeAggCell(&col, grp.states[agg_i], gi, allocator);
                    }
                }
                out_cols[col_i] = col;
                col_i += 1;
                agg_i += 1;
            },
        }
    }
    return .{ .columns = out_cols, .len = n_groups };
}

fn aggregateBatch(allocator: std.mem.Allocator, input: Batch, query: sql.Query) !Batch {
    if (query.grouping_sets.len > 0) return aggregateGroupingSets(allocator, input, query);
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
                try feedRow(g.states, input, query, row, allocator);
            } else {
                const states = try makeStates(allocator, query);
                try feedRow(states, input, query, row, allocator);
                try groups.put(allocator, "", .{ .first_row = row, .states = states });
            }
            continue;
        }

        const encoded = try encodeKey(allocator, input, row, group_idxs);
        if (groups.getPtr(encoded)) |g| {
            try feedRow(g.states, input, query, row, allocator);
            allocator.free(encoded);
        } else {
            const states = try makeStates(allocator, query);
            try feedRow(states, input, query, row, allocator);
            try groups.put(allocator, encoded, .{ .first_row = row, .states = states });
        }
    }

    const n_groups = groups.count();
    if (n_groups == 0 and query.group_by.len == 0 and query.hasAgg()) {
        const out_cols = try allocator.alloc(Column, aggOutLen(query));
        var col_i: usize = 0;
        for (query.items) |item| {
            switch (item) {
                .window => {},
                .agg => |agg| {
                    const src = try aggColumn(input, agg);
                    const name = try aggName(allocator, agg);
                    const empty_state = AggState{ .kind = agg.kind };
                    var col = try finalizeAgg(allocator, empty_state, src, name, 1);
                    try writeAggCell(&col, empty_state, 0, allocator);
                    out_cols[col_i] = col;
                    col_i += 1;
                },
                else => return error.UnsupportedSql,
            }
        }
        return .{ .columns = out_cols, .len = 1 };
    }

    const first_rows = try allocator.alloc(usize, n_groups);
    for (groups.values(), 0..) |g, i| first_rows[i] = g.first_row;

    const out_cols = try allocator.alloc(Column, aggOutLen(query));
    var col_i: usize = 0;
    if (query.hasWindow()) {
        for (query.group_by) |gname| {
            const src_i = try input.lookup(gname);
            out_cols[col_i] = try copyKeyColumn(allocator, input.columns[src_i], first_rows, gname);
            col_i += 1;
        }
    }
    var agg_i: usize = 0;
    for (query.items) |item| {
        switch (item) {
            .star, .scalar, .literal, .case => return error.UnsupportedSql,
            .expr => |ex| {
                out_cols[col_i] = try evalGroupComputed(allocator, input, first_rows, ex, query);
                col_i += 1;
            },
            .window => {},
            .column => |c| {
                const key = if (c.qualifier) |q|
                    try std.fmt.allocPrint(allocator, "{s}.{s}", .{ q, c.name })
                else
                    c.name;
                const src_i = try input.lookup(key);
                const name = c.alias orelse key;
                out_cols[col_i] = try copyKeyColumn(allocator, input.columns[src_i], first_rows, name);
                col_i += 1;
            },
            .grouping => |g| {
                out_cols[col_i] = try groupingColumn(allocator, g.alias orelse g.name, n_groups, &.{});
                col_i += 1;
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
                col_i += 1;
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
        .lower => "lower",
        .upper => "upper",
        .length => "length",
        .trim => "trim",
        .ltrim => "ltrim",
        .rtrim => "rtrim",
        .replace => "replace",
        .substr => "substr",
        .concat => "concat",
        .date_trunc => "date_trunc",
        .extract => "extract",
        .year => "year",
        .month => "month",
        .day => "day",
        .hour => "hour",
        .minute => "minute",
        .second => "second",
        .left => "left",
        .right => "right",
        .starts_with => "starts_with",
        .ends_with => "ends_with",
        .contains => "contains",
        .position => "strpos",
        .ceil => "ceil",
        .floor => "floor",
        .sign => "sign",
        .greatest => "greatest",
        .least => "least",
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
        .lower, .upper, .length, .trim, .ltrim, .rtrim, .replace, .substr, .concat, .date_trunc, .extract, .year, .month, .day, .hour, .minute, .second, .left, .right, .starts_with, .ends_with, .contains, .position, .ceil, .floor, .sign, .greatest, .least => error.UnsupportedSql,
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

fn gatherRowsMaybe(allocator: std.mem.Allocator, input: Batch, refs: []const JoinRef) !Batch {
    const columns = try allocator.alloc(Column, input.columns.len);
    for (input.columns, 0..) |src, ci| {
        columns[ci] = try gatherColumnMaybe(allocator, src, refs);
    }
    return .{ .columns = columns, .len = refs.len };
}

fn gatherColumnMaybe(allocator: std.mem.Allocator, src: Column, refs: []const JoinRef) !Column {
    var dst: Column = .{
        .name = src.name,
        .data_type = src.data_type,
        .len = refs.len,
        .decimal_precision = src.decimal_precision,
        .decimal_scale = src.decimal_scale,
    };
    switch (src.data_type) {
        .boolean => {
            dst.bools = try allocator.alloc(u8, refs.len);
            for (refs, 0..) |ref, i| dst.bools[i] = if (ref.present) src.bools[ref.idx] else 0;
        },
        .int32 => {
            dst.i32s = try allocator.alloc(i32, refs.len);
            for (refs, 0..) |ref, i| dst.i32s[i] = if (ref.present) src.i32s[ref.idx] else 0;
        },
        .int64, .timestamp, .timestamptz => {
            dst.i64s = try allocator.alloc(i64, refs.len);
            for (refs, 0..) |ref, i| dst.i64s[i] = if (ref.present) src.i64s[ref.idx] else 0;
        },
        .float32 => {
            dst.f32s = try allocator.alloc(f32, refs.len);
            for (refs, 0..) |ref, i| dst.f32s[i] = if (ref.present) src.f32s[ref.idx] else 0;
        },
        .float64 => {
            dst.f64s = try allocator.alloc(f64, refs.len);
            for (refs, 0..) |ref, i| dst.f64s[i] = if (ref.present) src.f64s[ref.idx] else 0;
        },
        .utf8 => {
            var nbytes: usize = 0;
            for (refs) |ref| {
                if (ref.present) nbytes += src.strAt(ref.idx).len;
            }
            const bytes = try allocator.alloc(u8, nbytes);
            const offsets = try allocator.alloc(u32, refs.len + 1);
            var off: u32 = 0;
            for (refs, 0..) |ref, i| {
                offsets[i] = off;
                if (ref.present) {
                    const s = src.strAt(ref.idx);
                    if (s.len > 0) @memcpy(bytes[off..][0..s.len], s);
                    off += @intCast(s.len);
                }
            }
            offsets[refs.len] = off;
            dst.utf8 = .{ .offsets = offsets, .bytes = bytes };
        },
        .uuid => {
            dst.uuids = try allocator.alloc([16]u8, refs.len);
            for (refs, 0..) |ref, i| {
                dst.uuids[i] = if (ref.present) src.uuids[ref.idx] else [_]u8{0} ** 16;
            }
        },
        .decimal128 => {
            dst.i128s = try allocator.alloc(i128, refs.len);
            for (refs, 0..) |ref, i| dst.i128s[i] = if (ref.present) src.i128s[ref.idx] else 0;
        },
    }
    dst.valid = try mapValidMaybe(allocator, src, refs);
    return dst;
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
    if (std.ascii.eqlIgnoreCase(name, "string") or std.ascii.eqlIgnoreCase(name, "binary") or
        std.ascii.eqlIgnoreCase(name, "list")) return .utf8;
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
        for (p.columns, parts[0].columns) |col, tmpl| {
            if (col.data_type != tmpl.data_type) return error.SchemaMismatch;
        }
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

fn mapValidMaybe(allocator: std.mem.Allocator, src: Column, refs: []const JoinRef) ![]u8 {
    var any_null = false;
    for (refs) |ref| {
        if (!ref.present or src.isNull(ref.idx)) {
            any_null = true;
            break;
        }
    }
    if (!any_null) return &.{};
    const out = try allocator.alloc(u8, refs.len);
    for (refs, 0..) |ref, i| {
        out[i] = if (!ref.present) 0 else if (src.valid.len == 0) 1 else src.valid[ref.idx];
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
            .string, .null => return error.TypeMismatch,
        }),
        .int64, .timestamp, .timestamptz => dst.i64s[row] = switch (lit) {
            .int => |v| v,
            .float => |v| @intFromFloat(v),
            .string, .null => return error.TypeMismatch,
        },
        .float32 => dst.f32s[row] = switch (lit) {
            .int => |v| @floatFromInt(v),
            .float => |v| @floatCast(v),
            .string, .null => return error.TypeMismatch,
        },
        .float64 => dst.f64s[row] = switch (lit) {
            .int => |v| @floatFromInt(v),
            .float => |v| v,
            .string, .null => return error.TypeMismatch,
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
        .null => "",
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
