//! Remote objects land on disk (GLACIER_TEMP/cache), not in the heap.
//! The reader opens the cached path once the ranges are written.
//! Several ranges stay in flight. The last range goes out in the first wave.

const std = @import("std");
const builtin = @import("builtin");
const FileSource = @import("source.zig").FileSource;
const Transport = @import("source.zig").Transport;

const chunk_default: usize = 4 * 1024 * 1024;
const window_default: usize = 4;
const window_cap: usize = 8;
const use_prefetch = !builtin.single_threaded and builtin.cpu.arch != .wasm32;

/// Tests set this so a small object still takes more than one range.
var http_chunk_override: std.atomic.Value(usize) = .init(0);
var http_window_override: std.atomic.Value(usize) = .init(0);

fn chunkCap() usize {
    const over = http_chunk_override.load(.acquire);
    if (over != 0) return over;
    if (comptime builtin.cpu.arch == .wasm32) return chunk_default;
    if (std.c.getenv("GLACIER_HTTP_CHUNK")) |raw| {
        const text = std.mem.span(raw);
        if (text.len != 0) {
            const n = std.fmt.parseInt(usize, text, 10) catch return chunk_default;
            if (n > 0) return n;
        }
    }
    return chunk_default;
}

fn httpWindow() usize {
    const over = http_window_override.load(.acquire);
    if (over != 0) return @min(over, window_cap);
    if (comptime builtin.cpu.arch == .wasm32) return 1;
    if (std.c.getenv("GLACIER_HTTP_WINDOW")) |raw| {
        const text = std.mem.span(raw);
        if (text.len != 0) {
            const n = std.fmt.parseInt(usize, text, 10) catch return window_default;
            if (n > 0) return @min(n, window_cap);
        }
    }
    return window_default;
}

pub fn tempRoot() []const u8 {
    if (comptime @import("builtin").cpu.arch == .wasm32) return "/tmp/glacier";
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

const PrefetchJob = struct {
    location: []const u8,
    offset: u64,
    dest: []u8,
    n: usize = 0,
    failed: bool = true,
};

fn prefetchChunk(job: *PrefetchJob, base: Transport) void {
    prefetchChunkInner(job, base) catch return;
    job.failed = false;
}

fn prefetchChunkInner(job: *PrefetchJob, base: Transport) !void {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.single_threaded) return error.HttpRequestFailed;
    const alloc = std.heap.smp_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var client: std.http.Client = .{ .allocator = alloc, .io = io };
    defer client.deinit();
    var remote = base;
    remote.allocator = alloc;
    remote.io = io;
    remote.http = &client;
    var source = try FileSource.openLocation(remote, job.location);
    defer source.close();
    const got = try source.read(job.offset, job.dest);
    if (got != job.dest.len) return error.UnexpectedEndOfFile;
    job.n = got;
}

const HttpRange = struct {
    off: u64,
    n: usize,
};

fn planRanges(allocator: std.mem.Allocator, size: u64) ![]HttpRange {
    const chunk = chunkCap();
    var list: std.ArrayList(HttpRange) = .empty;
    var off: u64 = 0;
    while (off < size) {
        const n: usize = @intCast(@min(@as(u64, chunk), size - off));
        try list.append(allocator, .{ .off = off, .n = n });
        off += n;
    }
    if (list.items.len > 1) {
        const last = list.items[list.items.len - 1];
        var i: usize = list.items.len - 1;
        while (i > 0) : (i -= 1) list.items[i] = list.items[i - 1];
        list.items[0] = last;
    }
    return list.toOwnedSlice(allocator);
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
            const ranges = try planRanges(t.allocator, size);
            defer t.allocator.free(ranges);
            const depth: usize = if (comptime use_prefetch)
                @min(httpWindow(), ranges.len)
            else
                1;
            const bufs = try t.allocator.alloc([]u8, depth);
            for (bufs) |*slot| slot.* = try t.allocator.alloc(u8, chunkCap());
            defer {
                for (bufs) |slot| t.allocator.free(slot);
                t.allocator.free(bufs);
            }

            var i: usize = 0;
            while (i < ranges.len) {
                const wave = @min(depth, ranges.len - i);
                if (comptime use_prefetch) {
                    if (wave > 1) {
                        var jobs: [window_cap]PrefetchJob = undefined;
                        var threads: [window_cap]?std.Thread = .{null} ** window_cap;
                        errdefer {
                            for (threads) |th| if (th) |live| live.join();
                        }
                        for (0..wave) |s| {
                            const r = ranges[i + s];
                            jobs[s] = .{
                                .location = location,
                                .offset = r.off,
                                .dest = bufs[s][0..r.n],
                                .failed = true,
                            };
                            threads[s] = try std.Thread.spawn(.{}, prefetchChunk, .{ &jobs[s], t });
                        }
                        for (0..wave) |s| {
                            threads[s].?.join();
                            threads[s] = null;
                            if (jobs[s].failed) return error.HttpRequestFailed;
                            try file.writePositionalAll(t.io, jobs[s].dest[0..jobs[s].n], jobs[s].offset);
                        }
                        i += wave;
                        continue;
                    }
                }
                const r = ranges[i];
                const got = try source.read(r.off, bufs[0][0..r.n]);
                if (got != r.n) return error.UnexpectedEndOfFile;
                try file.writePositionalAll(t.io, bufs[0][0..r.n], r.off);
                i += 1;
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

test "s3 and gs prefetch the next chunk while the current one is written" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    if (builtin.cpu.arch == .wasm32 or builtin.single_threaded) return error.SkipZigTest;
    const aws = @import("../kernel/aws.zig");
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const py =
        \\from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        \\import sys, threading, time
        \\LOG = sys.argv[1]
        \\N = 5000
        \\payload = bytes(i % 251 for i in range(N))
        \\lock = threading.Lock()
        \\inflight = 0
        \\class H(BaseHTTPRequestHandler):
        \\    protocol_version = "HTTP/1.1"
        \\    def log_message(self, *args):
        \\        pass
        \\    def _ok(self):
        \\        auth = self.headers.get("Authorization") or ""
        \\        if auth.startswith("AWS4-HMAC-SHA256 ") and "x-amz-date" in self.headers:
        \\            return True
        \\        return auth == "Bearer glacier-test-token"
        \\    def do_HEAD(self):
        \\        if not self._ok():
        \\            self.send_response(403); self.end_headers(); return
        \\        self.send_response(200)
        \\        self.send_header("Content-Length", str(N))
        \\        self.send_header("Accept-Ranges", "bytes")
        \\        self.end_headers()
        \\    def do_GET(self):
        \\        global inflight
        \\        if not self._ok():
        \\            self.send_response(403); self.end_headers(); return
        \\        with lock:
        \\            inflight += 1
        \\            seen = inflight
        \\        try:
        \\            rng = self.headers.get("Range")
        \\            start = 0
        \\            if rng and rng.startswith("bytes="):
        \\                spec = rng[6:]
        \\                start_s, _, end_s = spec.partition("-")
        \\                start = int(start_s)
        \\                last = int(end_s) if end_s else N - 1
        \\                chunk = payload[start:last + 1]
        \\            else:
        \\                chunk = payload
        \\                last = N - 1
        \\            with open(LOG, "a") as f:
        \\                f.write("%d %d\n" % (start, seen))
        \\            time.sleep(0.4)
        \\            if rng and rng.startswith("bytes="):
        \\                self.send_response(206)
        \\                self.send_header("Content-Range", "bytes %d-%d/%d" % (start, last, N))
        \\                self.send_header("Content-Length", str(len(chunk)))
        \\                self.end_headers()
        \\                self.wfile.write(chunk)
        \\            else:
        \\                self.send_response(200)
        \\                self.send_header("Content-Length", str(N))
        \\                self.end_headers()
        \\                self.wfile.write(payload)
        \\        finally:
        \\            with lock:
        \\                inflight -= 1
        \\httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        \\print(httpd.server_address[1], flush=True)
        \\httpd.serve_forever()
    ;

    const log_path = "/tmp/glacier-prefetch-log.txt";
    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "-c", py, log_path },
        .stdout = .pipe,
        .stderr = .ignore,
        .stdin = .ignore,
    }) catch return error.SkipZigTest;
    defer child.kill(io);

    var line_buf: [64]u8 = undefined;
    var stdout_reader = child.stdout.?.readerStreaming(io, &line_buf);
    const line = try stdout_reader.interface.takeDelimiterExclusive('\n');
    const port = try std.fmt.parseInt(u16, std.mem.trim(u8, line, " \r\n"), 10);
    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{port});
    defer gpa.free(endpoint);

    var payload: [5000]u8 = undefined;
    for (&payload, 0..) |*b, i| b.* = @intCast(i % 251);

    http_chunk_override.store(1000, .release);
    defer http_chunk_override.store(0, .release);
    http_window_override.store(4, .release);
    defer http_window_override.store(0, .release);

    var client: std.http.Client = .{ .allocator = gpa, .io = io };
    defer client.deinit();
    const creds = aws.Credentials{
        .access_key = "AKIATEST",
        .secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        .region = "us-east-1",
    };

    const s3_url = try std.fmt.allocPrint(gpa, "s3://glacier-test/prefetch-{d}.bin", .{port});
    defer gpa.free(s3_url);
    const gs_url = try std.fmt.allocPrint(gpa, "gs://glacier-test/prefetch-{d}.bin", .{port});
    defer gpa.free(gs_url);
    const cases = [_]struct { url: []const u8, gs: bool }{
        .{ .url = s3_url, .gs = false },
        .{ .url = gs_url, .gs = true },
    };
    for (cases) |case| {
        {
            var logf = try std.Io.Dir.cwd().createFile(io, log_path, .{});
            logf.close(io);
        }
        const t = Transport{
            .allocator = gpa,
            .io = io,
            .http = &client,
            .s3_creds = if (case.gs) null else creds,
            .s3_endpoint = if (case.gs) null else endpoint,
            .gcs_token = if (case.gs) "glacier-test-token" else null,
            .gcs_endpoint = if (case.gs) endpoint else null,
        };
        const dest = try materialize(t, case.url);
        defer gpa.free(dest);
        var cached = try FileSource.openPath(io, dest);
        defer cached.close();
        const got = try cached.readAll(gpa);
        defer gpa.free(got);
        try std.testing.expectEqualSlices(u8, &payload, got);

        var log_file = try std.Io.Dir.cwd().openFile(io, log_path, .{});
        defer log_file.close(io);
        const st = try log_file.stat(io);
        const text = try gpa.alloc(u8, @intCast(st.size));
        defer gpa.free(text);
        const n = try log_file.readPositionalAll(io, text, 0);
        try std.testing.expectEqual(text.len, n);
        var max_seen: usize = 0;
        var tail_first = false;
        var line_i: usize = 0;
        var it = std.mem.splitScalar(u8, text, '\n');
        while (it.next()) |row| {
            const s = std.mem.trim(u8, row, " \r");
            if (s.len == 0) continue;
            const sp = std.mem.indexOfScalar(u8, s, ' ') orelse return error.TestUnexpectedResult;
            const start = try std.fmt.parseInt(usize, s[0..sp], 10);
            const seen = try std.fmt.parseInt(usize, std.mem.trim(u8, s[sp + 1 ..], " "), 10);
            if (seen > max_seen) max_seen = seen;
            if (line_i < 4 and start >= 4000) tail_first = true;
            line_i += 1;
        }
        try std.testing.expect(max_seen >= 4);
        try std.testing.expect(tail_first);
    }
}
