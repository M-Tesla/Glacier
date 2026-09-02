//! Remote objects land on disk (GLACIER_TEMP/cache), not in the heap.
//! Carquet then opens the cached path like a local parquet.

const std = @import("std");
const FileSource = @import("source.zig").FileSource;
const Transport = @import("source.zig").Transport;

const chunk_cap: usize = 4 * 1024 * 1024;

pub fn tempRoot() []const u8 {
    if (std.c.getenv("GLACIER_TEMP")) |p| {
        const s = std.mem.span(p);
        if (s.len > 0) return s;
    }
    return "/tmp/glacier";
}

pub fn filePath(allocator: std.mem.Allocator, location: []const u8) ![]u8 {
    const dir = try std.fs.path.join(allocator, &.{ tempRoot(), "cache" });
    defer allocator.free(dir);
    var hex: [64]u8 = undefined;
    hashHex(location, &hex);
    return std.fs.path.join(allocator, &.{ dir, &hex });
}

pub fn materialize(t: Transport, location: []const u8) ![]u8 {
    var source = try FileSource.openLocation(t, location);
    defer source.close();
    const size = try source.size();

    const cache_dir = try std.fs.path.join(t.allocator, &.{ tempRoot(), "cache" });
    defer t.allocator.free(cache_dir);
    try std.Io.Dir.cwd().createDirPath(t.io, cache_dir);

    const dest = try filePath(t.allocator, location);
    errdefer t.allocator.free(dest);
    if (cachedOk(t.io, dest, size)) return dest;

    const part = try std.fmt.allocPrint(t.allocator, "{s}.part", .{dest});
    defer t.allocator.free(part);

    {
        var file = try std.Io.Dir.cwd().createFile(t.io, part, .{});
        defer file.close(t.io);

        if (size > 0) {
            const chunk_len: usize = @intCast(@min(chunk_cap, size));
            const chunk = try t.allocator.alloc(u8, chunk_len);
            defer t.allocator.free(chunk);
            var off: u64 = 0;
            while (off < size) {
                const n: usize = @intCast(@min(chunk.len, size - off));
                const got = try source.read(off, chunk[0..n]);
                if (got != n) return error.UnexpectedEndOfFile;
                try file.writePositionalAll(t.io, chunk[0..n], off);
                off += n;
            }
        }
    }
    try std.Io.Dir.cwd().rename(part, std.Io.Dir.cwd(), dest, t.io);
    return dest;
}

fn cachedOk(io: std.Io, path: []const u8, size: u64) bool {
    const st = std.Io.Dir.cwd().statFile(io, path, .{}) catch return false;
    return st.size == size;
}

fn hashHex(location: []const u8, out: *[64]u8) void {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(location, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    @memcpy(out, &hex);
}

test "cache key is stable for a URL" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const a = da.allocator();
    const p1 = try filePath(a, "http://127.0.0.1/sales.parquet");
    defer a.free(p1);
    const p2 = try filePath(a, "http://127.0.0.1/sales.parquet");
    defer a.free(p2);
    try std.testing.expectEqualStrings(p1, p2);
    try std.testing.expect(std.mem.indexOf(u8, p1, "cache") != null);
    try std.testing.expectEqual(@as(usize, 64), std.fs.path.basename(p1).len);
}
