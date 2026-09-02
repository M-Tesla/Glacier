//! Write tests/formats/sales.parquet and an Iceberg prune table.

const std = @import("std");
const glacier = @import("glacier");

pub fn main(init: std.process.Init) !void {
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.next();
    const parquet_path = it.next() orelse "tests/formats/sales.parquet";
    const iceberg_dir = it.next() orelse "tests/iceberg_prune";

    if (std.fs.path.dirname(parquet_path)) |dir| {
        try std.Io.Dir.cwd().createDirPath(init.io, dir);
    }
    var zbuf: [1024]u8 = undefined;
    const zpath = try std.fmt.bufPrintZ(&zbuf, "{s}", .{parquet_path});
    try glacier.session.writeSalesFixture(zpath);
    std.debug.print("wrote {s}\n", .{parquet_path});

    var avro_buf: [1024]u8 = undefined;
    const avro_path = if (std.mem.endsWith(u8, parquet_path, ".parquet"))
        try std.fmt.bufPrintZ(&avro_buf, "{s}.avro", .{parquet_path[0 .. parquet_path.len - ".parquet".len]})
    else
        try std.fmt.bufPrintZ(&avro_buf, "{s}.avro", .{parquet_path});
    const ids = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const prices = [_]i64{ 50, 80, 100, 120, 150, 200, 90, 110, 300, 75 };
    const cats = [_][*:0]const u8{ "fruit", "fruit", "veg", "veg", "fruit", "dairy", "fruit", "veg", "dairy", "fruit" };
    try glacier.avro.writeSalesRows(avro_path, &ids, &prices, &cats);
    std.debug.print("wrote {s}\n", .{avro_path});

    var codec_buf: [1024]u8 = undefined;
    const deflate_p = try std.fmt.bufPrintZ(&codec_buf, "{s}/sales_deflate.avro", .{if (std.fs.path.dirname(parquet_path)) |dir| dir else "tests/formats"});
    try glacier.avro.writeSalesRowsCodec(deflate_p, &ids, &prices, &cats, .deflate);
    std.debug.print("wrote {s}\n", .{deflate_p});
    const snappy_p = try std.fmt.bufPrintZ(&codec_buf, "{s}/sales_snappy.avro", .{if (std.fs.path.dirname(parquet_path)) |dir| dir else "tests/formats"});
    try glacier.avro.writeSalesRowsCodec(snappy_p, &ids, &prices, &cats, .snappy);
    std.debug.print("wrote {s}\n", .{snappy_p});

    try glacier.iceberg.writePruneFixture(init.gpa, init.io, iceberg_dir);
    std.debug.print("wrote {s}\n", .{iceberg_dir});

    const formats_dir = if (std.fs.path.dirname(parquet_path)) |dir| dir else "tests/formats";
    try std.Io.Dir.cwd().createDirPath(init.io, formats_dir);
    var pbuf: [1024]u8 = undefined;
    const types_p = try std.fmt.bufPrintZ(&pbuf, "{s}/types.parquet", .{formats_dir});
    try glacier.parquet.writeTypesFixture(types_p);
    std.debug.print("wrote {s}\n", .{types_p});

    const rgs_p = try std.fmt.bufPrintZ(&pbuf, "{s}/row_groups.parquet", .{formats_dir});
    try glacier.parquet.writeRowGroupsFixture(rgs_p);
    std.debug.print("wrote {s}\n", .{rgs_p});

    const nested_p = try std.fmt.bufPrintZ(&pbuf, "{s}/nested.parquet", .{formats_dir});
    try glacier.parquet.writeNestedFixture(nested_p);
    std.debug.print("wrote {s}\n", .{nested_p});

    const gzip_p = try std.fmt.bufPrintZ(&pbuf, "{s}/i64_gzip.parquet", .{formats_dir});
    try glacier.parquet.writeI64Fixture(gzip_p, .gzip);
    std.debug.print("wrote {s}\n", .{gzip_p});

    const lz4_p = try std.fmt.bufPrintZ(&pbuf, "{s}/i64_lz4.parquet", .{formats_dir});
    try glacier.parquet.writeI64Fixture(lz4_p, .lz4);
    std.debug.print("wrote {s}\n", .{lz4_p});

    const zstd_p = try std.fmt.bufPrintZ(&pbuf, "{s}/i64_zstd.parquet", .{formats_dir});
    try glacier.parquet.writeI64Fixture(zstd_p, .zstd);
    std.debug.print("wrote {s}\n", .{zstd_p});

    const logical_p = try std.fmt.bufPrintZ(&pbuf, "{s}/logical.parquet", .{formats_dir});
    try glacier.parquet.writeLogicalFixture(logical_p);
    std.debug.print("wrote {s}\n", .{logical_p});

    const nulls_p = try std.fmt.bufPrintZ(&pbuf, "{s}/nulls.parquet", .{formats_dir});
    try glacier.parquet.writeNullsFixture(nulls_p);
    std.debug.print("wrote {s}\n", .{nulls_p});
}
