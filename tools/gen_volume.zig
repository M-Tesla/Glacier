//! Write a sales-shaped parquet with N rows for RAM/CPU probes.

const std = @import("std");
const glacier = @import("glacier");

pub fn main(init: std.process.Init) !void {
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.next();
    const path = it.next() orelse return error.MissingPath;
    const n = if (it.next()) |s| try std.fmt.parseInt(usize, s, 10) else 100_000;

    const gpa = init.gpa;
    const ids = try gpa.alloc(i64, n);
    defer gpa.free(ids);
    const prices = try gpa.alloc(i64, n);
    defer gpa.free(prices);
    const cats = try gpa.alloc([*:0]const u8, n);
    defer gpa.free(cats);
    const pool = [_][:0]const u8{ "fruit", "veg", "dairy" };
    for (0..n) |i| {
        ids[i] = @intCast(i + 1);
        prices[i] = @intCast((i % 97) + 1);
        cats[i] = pool[i % 3].ptr;
    }

    var zbuf: [1024]u8 = undefined;
    const zpath = try std.fmt.bufPrintZ(&zbuf, "{s}", .{path});
    if (std.fs.path.dirname(path)) |dir| {
        std.Io.Dir.cwd().createDirPath(init.io, dir) catch {};
    }
    try glacier.parquet.writeSalesRows(zpath, ids, prices, cats);
    std.debug.print("wrote {s} rows={d}\n", .{ path, n });
}
