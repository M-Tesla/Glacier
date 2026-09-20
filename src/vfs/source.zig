//! FileSource: the only I/O type format readers talk to.
//!
//! Backends: `Io.File`, `File.MemoryMap`, borrowed memory, HTTP Range
//! (`std.http.Client`). S3 is HTTP + SigV4. GCS `gs://` is HTTPS + Bearer.

const std = @import("std");
const Io = std.Io;
const aws = @import("../kernel/aws.zig");

pub const Transport = struct {
    allocator: std.mem.Allocator,
    io: Io,
    http: *std.http.Client,
    /// Vended S3 keys from a REST catalog `config` map. When set, skip env/profile.
    s3_creds: ?aws.Credentials = null,
    s3_endpoint: ?[]const u8 = null,
    s3_region: ?[]const u8 = null,
    /// Vended GCS token (`gcs.oauth2.token`) or `GOOGLE_OAUTH_ACCESS_TOKEN`.
    gcs_token: ?[]const u8 = null,
    gcs_user_project: ?[]const u8 = null,
};

pub const FileSource = struct {
    backend: union(enum) {
        file: struct { io: Io, file: Io.File },
        mmap: struct { io: Io, file: Io.File, map: Io.File.MemoryMap },
        memory: []const u8,
        http: Http,
    },

    const Sign = struct {
        access_key: []u8,
        secret_key: []u8,
        session_token: ?[]u8,
        region: []u8,
    };

    const Http = struct {
        allocator: std.mem.Allocator,
        client: *std.http.Client,
        url: []u8,
        len: u64,
        sign: ?Sign,
        bearer: ?[]u8,
        gcs_project: ?[]u8,
    };

    pub fn openPath(io: Io, path: []const u8) Io.File.OpenError!FileSource {
        const file = try Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
        return .{ .backend = .{ .file = .{ .io = io, .file = file } } };
    }

    /// Map the file when the Io implementation allows it. `Threaded` with
    /// `disable_memory_mapping` (WASM) still succeeds: the map is a heap copy.
    /// Empty files or mmap failure fall back to the `file` backend.
    pub fn openMapped(io: Io, path: []const u8) Io.File.OpenError!FileSource {
        const file = try Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
        errdefer file.close(io);
        const st = file.stat(io) catch
            return .{ .backend = .{ .file = .{ .io = io, .file = file } } };
        const len = std.math.cast(usize, st.size) orelse
            return .{ .backend = .{ .file = .{ .io = io, .file = file } } };
        if (len == 0)
            return .{ .backend = .{ .file = .{ .io = io, .file = file } } };
        const map = file.createMemoryMap(io, .{
            .len = len,
            .protection = .{ .read = true, .write = false },
        }) catch return .{ .backend = .{ .file = .{ .io = io, .file = file } } };
        return .{ .backend = .{ .mmap = .{ .io = io, .file = file, .map = map } } };
    }

    /// Caller keeps `bytes` alive for the lifetime of this source and any
    /// reader opened from it (unless the wrap copies via `readAll`).
    pub fn fromMemory(bytes: []const u8) FileSource {
        return .{ .backend = .{ .memory = bytes } };
    }

    /// HTTP Range via `std.http.Client`. `client` must outlive the source.
    /// Servers that ignore `Range` yield `error.HttpRangeUnsupported`.
    /// HTTPS uses std TLS. S3 URLs are signed (SigV4). `gs://` uses Bearer.
    pub fn openHttp(allocator: std.mem.Allocator, client: *std.http.Client, url: []const u8) !FileSource {
        const owned = try allocator.dupe(u8, url);
        return openHttpOwned(allocator, client, owned, .{});
    }

    pub fn openS3(t: Transport, path: []const u8) !FileSource {
        if (t.s3_creds) |creds| {
            const resolved = aws.Credentials{
                .access_key = creds.access_key,
                .secret_key = creds.secret_key,
                .session_token = creds.session_token,
                .region = t.s3_region orelse creds.region,
            };
            return openS3Resolved(t, path, resolved, t.s3_endpoint);
        }
        if (aws.loadCredentials(t.allocator, t.io)) |creds| {
            defer {
                t.allocator.free(creds.access_key);
                t.allocator.free(creds.secret_key);
                if (creds.session_token) |tok| t.allocator.free(tok);
                t.allocator.free(creds.region);
            }
            const endpoint = aws.endpointFromEnv(t.allocator);
            defer if (endpoint) |e| t.allocator.free(e);
            return openS3Resolved(t, path, creds, endpoint);
        } else |err| switch (err) {
            error.AwsCredentialsMissing => {},
            else => return err,
        }
        const loc = try aws.parseS3(path);
        const region = try aws.defaultRegion(t.allocator, t.io);
        defer t.allocator.free(region);
        const endpoint = aws.endpointFromEnv(t.allocator);
        defer if (endpoint) |e| t.allocator.free(e);
        const url = try aws.httpUrlForS3(t.allocator, loc, region, endpoint);
        return openHttpOwned(t.allocator, t.http, url, .{});
    }

    pub fn openS3Resolved(
        t: Transport,
        path: []const u8,
        creds: aws.Credentials,
        endpoint: ?[]const u8,
    ) !FileSource {
        const loc = try aws.parseS3(path);
        const url = try aws.httpUrlForS3(t.allocator, loc, creds.region, endpoint);
        const sign = dupeSign(t.allocator, creds) catch |err| {
            t.allocator.free(url);
            return err;
        };
        return openHttpOwned(t.allocator, t.http, url, .{ .sign = sign });
    }

    pub fn openGs(t: Transport, path: []const u8) !FileSource {
        const loc = try aws.parseGs(path);
        const url = try aws.httpUrlForGs(t.allocator, loc);
        errdefer t.allocator.free(url);
        const bearer = try dupeOptional(t.allocator, gcsToken(t));
        errdefer if (bearer) |b| t.allocator.free(b);
        const project = try dupeOptional(t.allocator, t.gcs_user_project);
        return openHttpOwned(t.allocator, t.http, url, .{ .bearer = bearer, .gcs_project = project });
    }

    pub fn openLocation(t: Transport, path: []const u8) !FileSource {
        if (aws.isS3(path)) return openS3(t, path);
        if (aws.isGs(path)) return openGs(t, path);
        if (aws.isHttp(path)) return openHttp(t.allocator, t.http, path);
        return openMapped(t.io, path);
    }

    const HttpAuth = struct {
        sign: ?FileSource.Sign = null,
        bearer: ?[]u8 = null,
        gcs_project: ?[]u8 = null,
    };

    fn openHttpOwned(
        allocator: std.mem.Allocator,
        client: *std.http.Client,
        owned_url: []u8,
        auth: HttpAuth,
    ) !FileSource {
        errdefer {
            allocator.free(owned_url);
            if (auth.sign) |s| freeSign(allocator, s);
            if (auth.bearer) |b| allocator.free(b);
            if (auth.gcs_project) |p| allocator.free(p);
        }
        var h = Http{
            .allocator = allocator,
            .client = client,
            .url = owned_url,
            .len = 0,
            .sign = auth.sign,
            .bearer = auth.bearer,
            .gcs_project = auth.gcs_project,
        };
        h.len = try httpDiscoverLength(h);
        return .{ .backend = .{ .http = h } };
    }

    pub fn close(self: *FileSource) void {
        switch (self.backend) {
            .file => |f| f.file.close(f.io),
            .mmap => |*m| {
                m.map.destroy(m.io);
                m.file.close(m.io);
            },
            .memory => {},
            .http => |h| {
                h.allocator.free(h.url);
                if (h.sign) |s| freeSign(h.allocator, s);
                if (h.bearer) |b| h.allocator.free(b);
                if (h.gcs_project) |p| h.allocator.free(p);
            },
        }
        self.* = undefined;
    }

    /// Contiguous bytes when the backend already has them. `null` for file/http.
    pub fn view(self: FileSource) ?[]const u8 {
        return switch (self.backend) {
            .memory => |m| m,
            .mmap => |m| m.map.memory,
            .file, .http => null,
        };
    }

    pub fn size(self: FileSource) !u64 {
        return switch (self.backend) {
            .file => |f| (try f.file.stat(f.io)).size,
            .mmap => |m| m.map.memory.len,
            .memory => |m| m.len,
            .http => |h| h.len,
        };
    }

    pub fn read(self: FileSource, offset: u64, dest: []u8) !usize {
        switch (self.backend) {
            .file => |f| return f.file.readPositionalAll(f.io, dest, offset),
            .mmap => |m| return copySlice(m.map.memory, offset, dest),
            .memory => |mem| return copySlice(mem, offset, dest),
            .http => |h| return httpRead(h, offset, dest),
        }
    }

    /// Caller owns the returned slice.
    pub fn readAll(self: FileSource, allocator: std.mem.Allocator) ![]u8 {
        switch (self.backend) {
            .http => |h| return httpGetAll(h, allocator),
            else => {
                const n: usize = @intCast(try self.size());
                const buf = try allocator.alloc(u8, n);
                errdefer allocator.free(buf);
                const got = try self.read(0, buf);
                if (got != n) return error.UnexpectedEndOfFile;
                return buf;
            },
        }
    }

    /// Last 8 bytes: 4-byte little-endian footer length + `PAR1`.
    pub fn readParquetFooter(self: FileSource) !u32 {
        const n = try self.size();
        if (n < 8) return error.InvalidParquetFooter;
        var tail: [8]u8 = undefined;
        const got = try self.read(n - 8, &tail);
        if (got != 8) return error.InvalidParquetFooter;
        return parseParquetFooter(&tail);
    }
};

pub fn parseParquetFooter(tail: *const [8]u8) !u32 {
    if (!std.mem.eql(u8, tail[4..8], "PAR1")) return error.InvalidParquetFooter;
    return std.mem.readInt(u32, tail[0..4], .little);
}

/// `Content-Range: bytes 0-7/1234` → total size 1234.
pub fn parseContentRangeTotal(value: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, value, " \t");
    const spec = if (std.ascii.startsWithIgnoreCase(trimmed, "bytes "))
        trimmed["bytes ".len..]
    else
        return error.InvalidContentRange;
    const slash = std.mem.lastIndexOfScalar(u8, spec, '/') orelse return error.InvalidContentRange;
    const total_s = spec[slash + 1 ..];
    if (total_s.len == 0 or std.mem.eql(u8, total_s, "*")) return error.InvalidContentRange;
    return std.fmt.parseInt(u64, total_s, 10) catch error.InvalidContentRange;
}

/// `bytes=START-END` or `bytes=START-`.
pub fn parseByteRange(value: []const u8, total: u64) ?struct { start: u64, last: u64 } {
    if (total == 0) return null;
    const spec = if (std.mem.startsWith(u8, value, "bytes="))
        value["bytes=".len..]
    else
        return null;
    const dash = std.mem.indexOfScalar(u8, spec, '-') orelse return null;
    if (dash == 0) return null;
    const start = std.fmt.parseInt(u64, spec[0..dash], 10) catch return null;
    const last = if (dash + 1 >= spec.len)
        total - 1
    else
        std.fmt.parseInt(u64, spec[dash + 1 ..], 10) catch return null;
    if (start >= total or start > last) return null;
    return .{ .start = start, .last = @min(last, total - 1) };
}

fn copySlice(src: []const u8, offset: u64, dest: []u8) !usize {
    if (offset > src.len) return error.UnexpectedEndOfFile;
    const start: usize = @intCast(offset);
    const n = @min(dest.len, src.len - start);
    if (n > 0) @memcpy(dest[0..n], src[start..][0..n]);
    return n;
}

fn headerValue(head_bytes: []const u8, name: []const u8) ?[]const u8 {
    var it = std.http.HeaderIterator.init(head_bytes);
    while (it.next()) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
    }
    return null;
}

const HttpResult = struct {
    status: std.http.Status,
    content_length: ?u64,
    content_range_total: ?u64,
    n: usize,
};

fn httpExchange(
    h: FileSource.Http,
    method: std.http.Method,
    extra: []const std.http.Header,
    dest: []u8,
) !HttpResult {
    var date_buf: [16]u8 = undefined;
    var date_hdr: std.http.Header = undefined;
    var hash_hdr: std.http.Header = undefined;
    var auth_hdr: std.http.Header = undefined;
    var token_hdr: std.http.Header = undefined;
    var signed_extra: [8]std.http.Header = undefined;
    var extra_all: []const std.http.Header = extra;
    var ua_headers: [8]std.http.Header = undefined;
    var auth_owned: ?[]u8 = null;
    defer if (auth_owned) |p| h.allocator.free(p);

    if (h.sign) |s| {
        const amz_date = aws.nowAmzDate(h.client.io, &date_buf);
        const creds = aws.Credentials{
            .access_key = s.access_key,
            .secret_key = s.secret_key,
            .session_token = s.session_token,
            .region = s.region,
        };
        const auth = try aws.authorization(h.allocator, .{
            .method = methodName(method),
            .host = urlHost(h.url),
            .path = urlPath(h.url),
            .query = urlQuery(h.url),
            .extra_headers = extra,
            .amz_date = amz_date,
            .region = s.region,
            .creds = creds,
        });
        auth_owned = auth;
        var n: usize = 0;
        for (extra) |e| {
            signed_extra[n] = e;
            n += 1;
        }
        date_hdr = .{ .name = "x-amz-date", .value = amz_date };
        hash_hdr = .{ .name = "x-amz-content-sha256", .value = aws.unsigned_payload };
        auth_hdr = .{ .name = "authorization", .value = auth };
        signed_extra[n] = date_hdr;
        n += 1;
        signed_extra[n] = hash_hdr;
        n += 1;
        signed_extra[n] = auth_hdr;
        n += 1;
        if (s.session_token) |tok| {
            token_hdr = .{ .name = "x-amz-security-token", .value = tok };
            signed_extra[n] = token_hdr;
            n += 1;
        }
        extra_all = signed_extra[0..n];
    } else {
        var n: usize = 0;
        ua_headers[n] = .{ .name = "user-agent", .value = "glacier/0.2" };
        n += 1;
        if (h.bearer) |tok| {
            auth_owned = try std.fmt.allocPrint(h.allocator, "Bearer {s}", .{tok});
            ua_headers[n] = .{ .name = "authorization", .value = auth_owned.? };
            n += 1;
        }
        if (h.gcs_project) |proj| {
            ua_headers[n] = .{ .name = "x-goog-user-project", .value = proj };
            n += 1;
        }
        for (extra) |e| {
            ua_headers[n] = e;
            n += 1;
        }
        extra_all = ua_headers[0..n];
    }

    const uri = try std.Uri.parse(h.url);
    var req = try h.client.request(method, uri, .{
        .keep_alive = false,
        .headers = .{ .accept_encoding = .omit },
        .extra_headers = extra_all,
        .redirect_behavior = @enumFromInt(8),
    });
    defer req.deinit();
    try req.sendBodiless();
    // Hugging Face 302s to a signed CDN URL much larger than 256 bytes.
    var redirect_buf: [16384]u8 = undefined;
    var response = try req.receiveHead(&redirect_buf);

    var range_total: ?u64 = null;
    if (headerValue(response.head.bytes, "content-range")) |cr| {
        range_total = parseContentRangeTotal(cr) catch null;
    }

    var n: usize = 0;
    if (dest.len > 0 and method.responseHasBody()) {
        var transfer_buffer: [512]u8 = undefined;
        const body = response.reader(&transfer_buffer);
        n = body.readSliceShort(dest) catch |err| switch (err) {
            error.ReadFailed => return error.HttpRequestFailed,
            else => |e| return e,
        };
    }

    return .{
        .status = response.head.status,
        .content_length = response.head.content_length,
        .content_range_total = range_total,
        .n = n,
    };
}

fn httpDiscoverLength(h: FileSource.Http) !u64 {
    const head = httpExchange(h, .HEAD, &.{}, &.{}) catch
        return httpLengthFromRange(h);
    switch (head.status) {
        .ok => if (head.content_length) |len| return len,
        else => {},
    }
    return httpLengthFromRange(h);
}

fn httpLengthFromRange(h: FileSource.Http) !u64 {
    var probe: [1]u8 = undefined;
    const got = try httpExchange(h, .GET, &.{
        .{ .name = "range", .value = "bytes=0-0" },
    }, &probe);
    if (got.content_range_total) |total| return total;
    if (got.status == .ok) {
        if (got.content_length) |len| return len;
        if (got.n == 1) return 1;
    }
    return error.HttpRequestFailed;
}

fn httpRead(h: FileSource.Http, offset: u64, dest: []u8) !usize {
    if (dest.len == 0) return 0;
    if (offset > h.len) return error.UnexpectedEndOfFile;
    if (offset == h.len) return 0;
    const last = @min(offset + dest.len - 1, h.len - 1);
    var range_buf: [80]u8 = undefined;
    const range_val = std.fmt.bufPrint(&range_buf, "bytes={d}-{d}", .{ offset, last }) catch
        return error.HttpRequestFailed;
    const want: usize = @intCast(last - offset + 1);
    const got = try httpExchange(h, .GET, &.{
        .{ .name = "range", .value = range_val },
    }, dest[0..want]);
    switch (got.status) {
        .partial_content => return got.n,
        .ok => {
            if (offset == 0 and want == h.len) return got.n;
            return error.HttpRangeUnsupported;
        },
        else => return error.HttpRequestFailed,
    }
}

fn httpGetAll(h: FileSource.Http, allocator: std.mem.Allocator) ![]u8 {
    const n: usize = @intCast(h.len);
    const buf = try allocator.alloc(u8, n);
    errdefer allocator.free(buf);
    const got = try httpExchange(h, .GET, &.{}, buf);
    if (got.status != .ok and got.status != .partial_content) return error.HttpRequestFailed;
    if (got.n != n) return error.UnexpectedEndOfFile;
    return buf;
}

fn methodName(method: std.http.Method) []const u8 {
    return switch (method) {
        .GET => "GET",
        .HEAD => "HEAD",
        .PUT => "PUT",
        .POST => "POST",
        .DELETE => "DELETE",
        else => "GET",
    };
}

fn urlHost(url: []const u8) []const u8 {
    const after = if (std.mem.indexOf(u8, url, "://")) |i| url[i + 3 ..] else url;
    const slash = std.mem.indexOfScalar(u8, after, '/') orelse after.len;
    return after[0..slash];
}

fn urlPath(url: []const u8) []const u8 {
    const after = if (std.mem.indexOf(u8, url, "://")) |i| url[i + 3 ..] else url;
    const slash = std.mem.indexOfScalar(u8, after, '/') orelse return "/";
    const rest = after[slash..];
    const q = std.mem.indexOfScalar(u8, rest, '?') orelse rest.len;
    const path = rest[0..q];
    return if (path.len == 0) "/" else path;
}

fn urlQuery(url: []const u8) []const u8 {
    const q = std.mem.indexOfScalar(u8, url, '?') orelse return "";
    const rest = url[q + 1 ..];
    const hash = std.mem.indexOfScalar(u8, rest, '#') orelse rest.len;
    return rest[0..hash];
}

fn dupeSign(allocator: std.mem.Allocator, creds: aws.Credentials) !FileSource.Sign {
    const access = try allocator.dupe(u8, creds.access_key);
    errdefer allocator.free(access);
    const secret = try allocator.dupe(u8, creds.secret_key);
    errdefer allocator.free(secret);
    const token = if (creds.session_token) |t| try allocator.dupe(u8, t) else null;
    errdefer if (token) |t| allocator.free(t);
    const region = try allocator.dupe(u8, creds.region);
    return .{
        .access_key = access,
        .secret_key = secret,
        .session_token = token,
        .region = region,
    };
}

fn freeSign(allocator: std.mem.Allocator, s: FileSource.Sign) void {
    allocator.free(s.access_key);
    allocator.free(s.secret_key);
    if (s.session_token) |t| allocator.free(t);
    allocator.free(s.region);
}

fn dupeOptional(allocator: std.mem.Allocator, s: ?[]const u8) !?[]u8 {
    const v = s orelse return null;
    if (v.len == 0) return null;
    return try allocator.dupe(u8, v);
}

fn gcsToken(t: Transport) ?[]const u8 {
    if (t.gcs_token) |tok| {
        if (tok.len > 0) return tok;
    }
    if (comptime @import("builtin").cpu.arch == .wasm32) return null;
    if (std.c.getenv("GOOGLE_OAUTH_ACCESS_TOKEN")) |p| {
        const s = std.mem.span(p);
        if (s.len > 0) return s;
    }
    if (std.c.getenv("GCS_OAUTH_TOKEN")) |p| {
        const s = std.mem.span(p);
        if (s.len > 0) return s;
    }
    return null;
}

pub fn joinLocation(allocator: std.mem.Allocator, base: []const u8, rel: []const u8) ![]u8 {
    if (std.mem.indexOf(u8, rel, "://") != null) return allocator.dupe(u8, rel);
    var rest = rel;
    if (std.mem.startsWith(u8, rest, "file://")) {
        rest = rest["file://".len..];
        while (rest.len > 0 and rest[0] == '/') {
            if (rest.len > 2 and rest[2] == ':') break;
            if (std.fs.path.isAbsolute(rest)) break;
            rest = rest[1..];
        }
    }
    if (aws.isRemote(base)) {
        const b = std.mem.trimEnd(u8, base, "/");
        const r = std.mem.trimStart(u8, rest, "/");
        if (r.len == 0) return allocator.dupe(u8, b);
        return std.fmt.allocPrint(allocator, "{s}/{s}", .{ b, r });
    }
    if (std.fs.path.isAbsolute(rest)) return allocator.dupe(u8, rest);
    return std.fs.path.join(allocator, &.{ base, rest });
}

fn stripFileScheme(path: []const u8) []const u8 {
    if (std.ascii.startsWithIgnoreCase(path, "file://")) return path["file://".len..];
    return path;
}

/// Local `createFile`, or a single HTTP PUT for `s3://` (SigV4) / `gs://` (Bearer).
/// No multipart. `http(s)://` destinations are refused.
pub fn putLocation(t: Transport, path: []const u8, bytes: []const u8) !void {
    const loc = stripFileScheme(path);
    if (aws.isS3(loc)) return putS3(t, loc, bytes);
    if (aws.isGs(loc)) return putGs(t, loc, bytes);
    if (aws.isHttp(loc)) return error.WriteUnsupported;
    try putLocal(t.io, loc, bytes);
}

fn putLocal(io: Io, path: []const u8, bytes: []const u8) !void {
    if (std.fs.path.dirname(path)) |d| {
        if (d.len > 0) try Io.Dir.cwd().createDirPath(io, d);
    }
    try Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

fn putS3(t: Transport, path: []const u8, bytes: []const u8) !void {
    if (t.s3_creds) |creds| {
        const resolved = aws.Credentials{
            .access_key = creds.access_key,
            .secret_key = creds.secret_key,
            .session_token = creds.session_token,
            .region = t.s3_region orelse creds.region,
        };
        return putS3Resolved(t, path, resolved, t.s3_endpoint, bytes);
    }
    const creds = try aws.loadCredentials(t.allocator, t.io);
    defer {
        t.allocator.free(creds.access_key);
        t.allocator.free(creds.secret_key);
        if (creds.session_token) |tok| t.allocator.free(tok);
        t.allocator.free(creds.region);
    }
    var env_ep: ?[]u8 = null;
    defer if (env_ep) |e| t.allocator.free(e);
    const endpoint: ?[]const u8 = if (t.s3_endpoint) |e| e else blk: {
        env_ep = aws.endpointFromEnv(t.allocator);
        break :blk env_ep;
    };
    const resolved = aws.Credentials{
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
        .region = t.s3_region orelse creds.region,
    };
    return putS3Resolved(t, path, resolved, endpoint, bytes);
}

fn putS3Resolved(
    t: Transport,
    path: []const u8,
    creds: aws.Credentials,
    endpoint: ?[]const u8,
    bytes: []const u8,
) !void {
    const loc = try aws.parseS3(path);
    const url = try aws.httpUrlForS3(t.allocator, loc, creds.region, endpoint);
    defer t.allocator.free(url);
    var hash_buf = aws.sha256Hex(bytes);
    var date_buf: [16]u8 = undefined;
    const amz_date = aws.nowAmzDate(t.io, &date_buf);
    const auth = try aws.authorization(t.allocator, .{
        .method = "PUT",
        .host = urlHost(url),
        .path = urlPath(url),
        .query = urlQuery(url),
        .payload_hash = &hash_buf,
        .amz_date = amz_date,
        .region = creds.region,
        .creds = creds,
    });
    defer t.allocator.free(auth);
    var hdrs: [8]std.http.Header = undefined;
    var n: usize = 0;
    hdrs[n] = .{ .name = "x-amz-date", .value = amz_date };
    n += 1;
    hdrs[n] = .{ .name = "x-amz-content-sha256", .value = &hash_buf };
    n += 1;
    hdrs[n] = .{ .name = "authorization", .value = auth };
    n += 1;
    if (creds.session_token) |tok| {
        hdrs[n] = .{ .name = "x-amz-security-token", .value = tok };
        n += 1;
    }
    try putHttp(t, url, hdrs[0..n], bytes);
}

fn putGs(t: Transport, path: []const u8, bytes: []const u8) !void {
    const token = gcsToken(t) orelse return error.AccessDenied;
    const loc = try aws.parseGs(path);
    const url = try aws.httpUrlForGs(t.allocator, loc);
    defer t.allocator.free(url);
    const auth = try std.fmt.allocPrint(t.allocator, "Bearer {s}", .{token});
    defer t.allocator.free(auth);
    var hdrs: [4]std.http.Header = undefined;
    var n: usize = 0;
    hdrs[n] = .{ .name = "authorization", .value = auth };
    n += 1;
    if (t.gcs_user_project) |proj| {
        if (proj.len > 0) {
            hdrs[n] = .{ .name = "x-goog-user-project", .value = proj };
            n += 1;
        }
    }
    try putHttp(t, url, hdrs[0..n], bytes);
}

fn putHttp(
    t: Transport,
    url: []const u8,
    extra_headers: []const std.http.Header,
    bytes: []const u8,
) !void {
    var aw: std.Io.Writer.Allocating = .init(t.allocator);
    defer aw.deinit();
    const result = t.http.fetch(.{
        .location = .{ .url = url },
        .method = .PUT,
        .payload = bytes,
        .extra_headers = extra_headers,
        .response_writer = &aw.writer,
        .keep_alive = false,
        .headers = .{ .accept_encoding = .omit },
    }) catch return error.ObjectPutFailed;
    switch (result.status) {
        .ok, .created, .no_content => {},
        .unauthorized, .forbidden => return error.AccessDenied,
        else => return error.ObjectPutFailed,
    }
}

test "fromMemory round-trips bytes" {
    const bytes = "hello glacier";
    var src = FileSource.fromMemory(bytes);
    defer src.close();
    try std.testing.expectEqual(@as(u64, bytes.len), try src.size());
    var scratch: [5]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 5), try src.read(6, &scratch));
    try std.testing.expectEqualStrings("glaci", &scratch);
    const copy = try src.readAll(std.testing.allocator);
    defer std.testing.allocator.free(copy);
    try std.testing.expectEqualStrings(bytes, copy);
}

test "parseParquetFooter rejects bad magic" {
    const bad = [_]u8{ 1, 0, 0, 0, 'N', 'O', 'P', 'E' };
    try std.testing.expectError(error.InvalidParquetFooter, parseParquetFooter(&bad));
    const ok = [_]u8{ 42, 0, 0, 0, 'P', 'A', 'R', '1' };
    try std.testing.expectEqual(@as(u32, 42), try parseParquetFooter(&ok));
}

test "parseByteRange and Content-Range" {
    const r = parseByteRange("bytes=8-15", 100).?;
    try std.testing.expectEqual(@as(u64, 8), r.start);
    try std.testing.expectEqual(@as(u64, 15), r.last);
    const open_end = parseByteRange("bytes=16-", 24).?;
    try std.testing.expectEqual(@as(u64, 16), open_end.start);
    try std.testing.expectEqual(@as(u64, 23), open_end.last);
    try std.testing.expectEqual(@as(u64, 1234), try parseContentRangeTotal("bytes 0-7/1234"));
    try std.testing.expectEqual(@as(u64, 24), try parseContentRangeTotal("bytes 16-23/24"));
}

test "MemoryMap round-trips and view" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const path = "/tmp/glacier-mmap.bin";
    const payload = "mmap-payload-PAR1";
    try Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = payload });

    var src = try FileSource.openMapped(io, path);
    defer src.close();
    try std.testing.expectEqual(.mmap, std.meta.activeTag(src.backend));
    try std.testing.expectEqualStrings(payload, src.view().?);
    try std.testing.expectEqual(@as(u64, payload.len), try src.size());
    var scratch: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try src.read(payload.len - 4, &scratch));
    try std.testing.expectEqualStrings("PAR1", &scratch);
}

test "MemoryMap works with disable_memory_mapping" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{ .disable_memory_mapping = true });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "/tmp/glacier-mmap-fallback.bin";
    const payload = "heap-copy-still-a-view";
    try Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = payload });

    var src = try FileSource.openMapped(io, path);
    defer src.close();
    try std.testing.expectEqualStrings(payload, src.view().?);
}

test "readParquetFooter from memory and mmap" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var payload: [24]u8 = undefined;
    @memset(&payload, 'x');
    std.mem.writeInt(u32, payload[16..20], 99, .little);
    @memcpy(payload[20..24], "PAR1");

    var mem = FileSource.fromMemory(&payload);
    defer mem.close();
    try std.testing.expectEqual(@as(u32, 99), try mem.readParquetFooter());

    const path = "/tmp/glacier-footer.parquet";
    try Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = &payload });
    var mapped = try FileSource.openMapped(io, path);
    defer mapped.close();
    try std.testing.expectEqual(@as(u32, 99), try mapped.readParquetFooter());
}

test "HTTP Range GET reads parquet footer" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var payload: [24]u8 = undefined;
    @memset(&payload, 'x');
    std.mem.writeInt(u32, payload[16..20], 99, .little);
    @memcpy(payload[20..24], "PAR1");
    const hex = std.fmt.bytesToHex(&payload, .lower);

    const py =
        \\from http.server import BaseHTTPRequestHandler, HTTPServer
        \\import sys
        \\payload = bytes.fromhex(sys.argv[1])
        \\class H(BaseHTTPRequestHandler):
        \\    def log_message(self, *args):
        \\        pass
        \\    def do_HEAD(self):
        \\        self.send_response(200)
        \\        self.send_header("Content-Length", str(len(payload)))
        \\        self.send_header("Accept-Ranges", "bytes")
        \\        self.end_headers()
        \\    def do_GET(self):
        \\        rng = self.headers.get("Range")
        \\        if rng and rng.startswith("bytes="):
        \\            spec = rng[6:]
        \\            start_s, _, end_s = spec.partition("-")
        \\            start = int(start_s)
        \\            last = int(end_s) if end_s else len(payload) - 1
        \\            chunk = payload[start:last + 1]
        \\            self.send_response(206)
        \\            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, last, len(payload)))
        \\            self.send_header("Content-Length", str(len(chunk)))
        \\            self.end_headers()
        \\            self.wfile.write(chunk)
        \\        else:
        \\            self.send_response(200)
        \\            self.send_header("Content-Length", str(len(payload)))
        \\            self.end_headers()
        \\            self.wfile.write(payload)
        \\httpd = HTTPServer(("127.0.0.1", 0), H)
        \\print(httpd.server_address[1], flush=True)
        \\httpd.serve_forever()
    ;

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "-c", py, &hex },
        .stdout = .pipe,
        .stderr = .ignore,
        .stdin = .ignore,
    }) catch return error.SkipZigTest;
    defer child.kill(io);

    var line_buf: [64]u8 = undefined;
    var stdout_reader = child.stdout.?.readerStreaming(io, &line_buf);
    const line = stdout_reader.interface.takeDelimiterExclusive('\n') catch return error.SkipZigTest;
    const port = std.fmt.parseInt(u16, std.mem.trim(u8, line, " \r\n"), 10) catch return error.SkipZigTest;
    try std.testing.expect(port != 0);

    const url = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}/file.parquet", .{port});
    defer gpa.free(url);

    var client: std.http.Client = .{ .allocator = gpa, .io = io };
    defer client.deinit();

    var src = try FileSource.openHttp(gpa, &client, url);
    defer src.close();
    try std.testing.expectEqual(@as(u64, payload.len), try src.size());
    var prefix: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try src.read(0, &prefix));
    try std.testing.expectEqualStrings("xxxx", &prefix);
    try std.testing.expectEqual(@as(u32, 99), try src.readParquetFooter());
}

test "joinLocation keeps s3 and http schemes" {
    const a = std.testing.allocator;
    const p = try joinLocation(a, "s3://bucket/table", "metadata/v1.metadata.json");
    defer a.free(p);
    try std.testing.expectEqualStrings("s3://bucket/table/metadata/v1.metadata.json", p);
    const abs = try joinLocation(a, "s3://bucket/table", "s3://other/file.parquet");
    defer a.free(abs);
    try std.testing.expectEqualStrings("s3://other/file.parquet", abs);
    const gs = try joinLocation(a, "gs://lake/table", "data/part.parquet");
    defer a.free(gs);
    try std.testing.expectEqualStrings("gs://lake/table/data/part.parquet", gs);
}

test "putLocation writes a local file" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client: std.http.Client = .{ .allocator = gpa, .io = io };
    defer client.deinit();
    const t = Transport{ .allocator = gpa, .io = io, .http = &client };
    const path = "/tmp/glacier_put_local/dir/obj.bin";
    std.Io.Dir.cwd().deleteTree(io, "/tmp/glacier_put_local") catch {};
    try putLocation(t, path, "hello-put");
    var src = try FileSource.openPath(io, path);
    defer src.close();
    const got = try src.readAll(gpa);
    defer gpa.free(got);
    try std.testing.expectEqualStrings("hello-put", got);
}

test "S3 SigV4 Range GET against path-style mock" {
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var payload: [24]u8 = undefined;
    @memset(&payload, 'x');
    std.mem.writeInt(u32, payload[16..20], 99, .little);
    @memcpy(payload[20..24], "PAR1");
    const hex = std.fmt.bytesToHex(&payload, .lower);

    const py =
        \\from http.server import BaseHTTPRequestHandler, HTTPServer
        \\import sys
        \\payload = bytes.fromhex(sys.argv[1])
        \\class H(BaseHTTPRequestHandler):
        \\    def log_message(self, *args):
        \\        pass
        \\    def _ok_auth(self):
        \\        auth = self.headers.get("Authorization") or ""
        \\        return auth.startswith("AWS4-HMAC-SHA256 ") and "x-amz-date" in self.headers
        \\    def do_HEAD(self):
        \\        if not self._ok_auth():
        \\            self.send_response(403); self.end_headers(); return
        \\        self.send_response(200)
        \\        self.send_header("Content-Length", str(len(payload)))
        \\        self.send_header("Accept-Ranges", "bytes")
        \\        self.end_headers()
        \\    def do_GET(self):
        \\        if not self._ok_auth():
        \\            self.send_response(403); self.end_headers(); return
        \\        rng = self.headers.get("Range")
        \\        if rng and rng.startswith("bytes="):
        \\            spec = rng[6:]
        \\            start_s, _, end_s = spec.partition("-")
        \\            start = int(start_s)
        \\            last = int(end_s) if end_s else len(payload) - 1
        \\            chunk = payload[start:last + 1]
        \\            self.send_response(206)
        \\            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, last, len(payload)))
        \\            self.send_header("Content-Length", str(len(chunk)))
        \\            self.end_headers()
        \\            self.wfile.write(chunk)
        \\        else:
        \\            self.send_response(200)
        \\            self.send_header("Content-Length", str(len(payload)))
        \\            self.end_headers()
        \\            self.wfile.write(payload)
        \\httpd = HTTPServer(("127.0.0.1", 0), H)
        \\print(httpd.server_address[1], flush=True)
        \\httpd.serve_forever()
    ;

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "-c", py, &hex },
        .stdout = .pipe,
        .stderr = .ignore,
        .stdin = .ignore,
    }) catch return error.SkipZigTest;
    defer child.kill(io);

    var line_buf: [64]u8 = undefined;
    var stdout_reader = child.stdout.?.readerStreaming(io, &line_buf);
    const line = stdout_reader.interface.takeDelimiterExclusive('\n') catch return error.SkipZigTest;
    const port = std.fmt.parseInt(u16, std.mem.trim(u8, line, " \r\n"), 10) catch return error.SkipZigTest;

    var client: std.http.Client = .{ .allocator = gpa, .io = io };
    defer client.deinit();
    const t = Transport{ .allocator = gpa, .io = io, .http = &client };
    const creds = aws.Credentials{
        .access_key = "AKIATEST",
        .secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        .region = "us-east-1",
    };
    const endpoint = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}", .{port});
    defer gpa.free(endpoint);
    var src = try FileSource.openS3Resolved(t, "s3://glacier-test/file.parquet", creds, endpoint);
    defer src.close();
    try std.testing.expectEqual(@as(u64, payload.len), try src.size());
    try std.testing.expectEqual(@as(u32, 99), try src.readParquetFooter());
}
