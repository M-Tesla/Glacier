//! Smoke tool: open a Parquet file via FileSource + carquet and print schema.
//! No args: write uncompressed + snappy fixtures under /tmp and read them.

const std = @import("std");
const glacier = @import("glacier");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.next();
    if (it.next()) |path| {
        try dump(gpa, io, path);
        return;
    }

    try glacier.parquet.writeI64Fixture("/tmp/glacier-smoke.parquet", .uncompressed);
    try dump(gpa, io, "/tmp/glacier-smoke.parquet");
    try glacier.parquet.writeI64Fixture("/tmp/glacier-smoke-snappy.parquet", .snappy);
    try dump(gpa, io, "/tmp/glacier-smoke-snappy.parquet");
}

fn dump(gpa: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    var reader = glacier.parquet.openPath(gpa, io, path) catch |err| {
        std.debug.print("open failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer reader.close();

    std.debug.print("file: {s}\n", .{path});
    std.debug.print("rows: {d}\n", .{reader.numRows()});
    std.debug.print("row_groups: {d}\n", .{reader.numRowGroups()});
    std.debug.print("columns: {d}\n", .{reader.numColumns()});

    const n_cols = reader.numColumns();
    var i: i32 = 0;
    while (i < n_cols) : (i += 1) {
        if (reader.column(i)) |col| {
            std.debug.print("  [{d}] {s}  {s}\n", .{ i, col.name, @tagName(col.physical_type) });
        }
    }

    var sample: [8]i64 = undefined;
    const got = reader.readI64Prefix(0, &sample);
    if (got > 0) {
        std.debug.print("col0 prefix:", .{});
        var s: i64 = 0;
        while (s < got) : (s += 1) std.debug.print(" {d}", .{sample[@intCast(s)]});
        std.debug.print("\n", .{});
    }
}
