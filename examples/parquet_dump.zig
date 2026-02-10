// ═══════════════════════════════════════════════════════════════════════════
// PARQUET-DUMP - Simple Parquet file inspector
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const glacier = @import("glacier");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip executable name
    _ = args.skip();

    const file_path = args.next() orelse {
        std.debug.print("Usage: parquet-dump <file.parquet>\n", .{});
        std.process.exit(1);
    };

    std.debug.print("Opening Parquet file: {s}\n", .{file_path});
    std.debug.print("{s}\n", .{"─" ** 60});

    var reader = try glacier.parquet.Reader.open(allocator, file_path);
    defer reader.close();

    std.debug.print("File size: {} bytes\n", .{reader.file_size});

    // Read metadata
    try reader.readMetadata();

    const num_rows = reader.getNumRows();
    std.debug.print("Number of rows: {}\n", .{num_rows});

    if (reader.metadata) |meta| {
        for (meta.row_groups, 0..) |rg, i| {
            std.debug.print("\nRow Group {d}:\n", .{i});
            std.debug.print("  Rows: {d}\n", .{rg.num_rows});
            std.debug.print("  Bytes: {d}\n", .{rg.total_byte_size});
            
            for (rg.columns, 0..) |col, j| {
                std.debug.print("  Column {d}:\n", .{j});
                std.debug.print("    Type: {}\n", .{col.meta_data.type});
                std.debug.print("    Encodings: {any}\n", .{col.meta_data.encodings});
                std.debug.print("    Data Page Offset: {d}\n", .{col.meta_data.data_page_offset});
                if (col.meta_data.dictionary_page_offset) |off| {
                    std.debug.print("    Dictionary Page Offset: {d}\n", .{off});
                } else {
                    std.debug.print("    Dictionary Page Offset: NULL\n", .{});
                }
            }
        }
    }

    std.debug.print("\n✓ Successfully inspected Parquet file!\n", .{});
}
