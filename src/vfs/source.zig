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
    /// Overrides `https://storage.googleapis.com`. `GCS_ENDPOINT_URL` is the fallback.
    gcs_endpoint: ?[]const u8 = null,
    /// Bearer for `az://`. `AZURE_STORAGE_TOKEN` is the fallback.
    azure_token: ?[]const u8 = null,
    /// Account host for `az://`. `AZURE_ENDPOINT_URL` is the fallback.
    azure_endpoint: ?[]const u8 = null,
    /// Multipart part size in bytes. 0 uses `GLACIER_PUT_PART` or 8 MiB.
    put_part: usize = 0,
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
        const url = try aws.httpUrlForGs(t.allocator, loc, t.gcs_endpoint);
        const bearer = dupeOptional(t.allocator, gcsToken(t)) catch |err| {
            t.allocator.free(url);
            return err;
        };
        const project = dupeOptional(t.allocator, t.gcs_user_project) catch |err| {
            t.allocator.free(url);
            if (bearer) |b| t.allocator.free(b);
            return err;
        };
        return openHttpOwned(t.allocator, t.http, url, .{ .bearer = bearer, .gcs_project = project });
    }

    pub fn openAz(t: Transport, path: []const u8) !FileSource {
        const loc = try aws.parseAz(path);
        const url = try aws.httpUrlForAz(t.allocator, loc, t.azure_endpoint);
        const bearer = dupeOptional(t.allocator, azureToken(t)) catch |err| {
            t.allocator.free(url);
            return err;
        };
        return openHttpOwned(t.allocator, t.http, url, .{ .bearer = bearer });
    }

    pub fn openLocation(t: Transport, path: []const u8) !FileSource {
        if (aws.isS3(path)) return openS3(t, path);
        if (aws.isGs(path)) return openGs(t, path);
        if (aws.isAz(path)) return openAz(t, path);
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

fn httpReadBody(h: FileSource.Http, allocator: std.mem.Allocator) ![]u8 {
    var date_buf: [16]u8 = undefined;
    var date_hdr: std.http.Header = undefined;
    var hash_hdr: std.http.Header = undefined;
    var auth_hdr: std.http.Header = undefined;
    var token_hdr: std.http.Header = undefined;
    var signed_extra: [8]std.http.Header = undefined;
    var extra_all: []const std.http.Header = &.{};
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
            .method = "GET",
            .host = urlHost(h.url),
            .path = urlPath(h.url),
            .query = urlQuery(h.url),
            .extra_headers = &.{},
            .amz_date = amz_date,
            .region = s.region,
            .creds = creds,
        });
        auth_owned = auth;
        var n: usize = 0;
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
        extra_all = ua_headers[0..n];
    }

    const uri = try std.Uri.parse(h.url);
    var req = try h.client.request(.GET, uri, .{
        .keep_alive = false,
        .headers = .{ .accept_encoding = .omit },
        .extra_headers = extra_all,
        .redirect_behavior = @enumFromInt(8),
    });
    defer req.deinit();
    try req.sendBodiless();
    var redirect_buf: [16384]u8 = undefined;
    var response = try req.receiveHead(&redirect_buf);
    if (response.head.status != .ok) return error.HttpRequestFailed;

    var transfer_buffer: [512]u8 = undefined;
    var body = response.reader(&transfer_buffer);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var tmp: [8192]u8 = undefined;
    while (out.items.len < 4 * 1024 * 1024) {
        const n = body.readSliceShort(&tmp) catch |err| switch (err) {
            error.ReadFailed => return error.HttpRequestFailed,
            else => |e| return e,
        };
        if (n == 0) break;
        try out.appendSlice(allocator, tmp[0..n]);
    }
    if (out.items.len >= 4 * 1024 * 1024) return error.HttpRequestFailed;
    return out.toOwnedSlice(allocator);
}

const ListPage = struct {
    names: [][]u8,
    token: ?[]u8 = null,
    truncated: bool = false,
};

fn xmlSpan(xml: []const u8, open: []const u8, close: []const u8) ?[]const u8 {
    const at = std.mem.indexOf(u8, xml, open) orelse return null;
    const start = at + open.len;
    const end = std.mem.indexOfPos(u8, xml, start, close) orelse return null;
    return xml[start..end];
}

fn xmlUnescape(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    if (std.mem.indexOfScalar(u8, raw, '&') == null) return allocator.dupe(u8, raw);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < raw.len) {
        if (raw[i] != '&') {
            try out.append(allocator, raw[i]);
            i += 1;
            continue;
        }
        const rest = raw[i..];
        if (std.mem.startsWith(u8, rest, "&amp;")) {
            try out.append(allocator, '&');
            i += 5;
        } else if (std.mem.startsWith(u8, rest, "&lt;")) {
            try out.append(allocator, '<');
            i += 4;
        } else if (std.mem.startsWith(u8, rest, "&gt;")) {
            try out.append(allocator, '>');
            i += 4;
        } else if (std.mem.startsWith(u8, rest, "&quot;")) {
            try out.append(allocator, '"');
            i += 6;
        } else if (std.mem.startsWith(u8, rest, "&apos;")) {
            try out.append(allocator, '\'');
            i += 6;
        } else {
            try out.append(allocator, raw[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

fn directChild(prefix: []const u8, full: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, full, prefix)) return null;
    var rest = full[prefix.len..];
    if (rest.len > 0 and rest[rest.len - 1] == '/') rest = rest[0 .. rest.len - 1];
    if (rest.len == 0 or rest[0] == '.') return null;
    if (std.mem.indexOfScalar(u8, rest, '/') != null) return null;
    return rest;
}

fn parseListPage(allocator: std.mem.Allocator, xml: []const u8, prefix: []const u8) !ListPage {
    var names: std.ArrayList([]u8) = .empty;
    errdefer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }
    var i: usize = 0;
    const open = "<Prefix>";
    const close = "</Prefix>";
    while (std.mem.indexOfPos(u8, xml, i, open)) |at| {
        const start = at + open.len;
        const end = std.mem.indexOfPos(u8, xml, start, close) orelse break;
        const raw = xml[start..end];
        i = end + close.len;
        const text = try xmlUnescape(allocator, raw);
        const child = directChild(prefix, text) orelse {
            allocator.free(text);
            continue;
        };
        const owned = try allocator.dupe(u8, child);
        allocator.free(text);
        errdefer allocator.free(owned);
        try names.append(allocator, owned);
    }
    i = 0;
    const name_open = "<Name>";
    const name_close = "</Name>";
    while (std.mem.indexOfPos(u8, xml, i, name_open)) |at| {
        const start = at + name_open.len;
        const end = std.mem.indexOfPos(u8, xml, start, name_close) orelse break;
        const raw = xml[start..end];
        i = end + name_close.len;
        const text = try xmlUnescape(allocator, raw);
        const child = directChild(prefix, text) orelse {
            allocator.free(text);
            continue;
        };
        const owned = try allocator.dupe(u8, child);
        allocator.free(text);
        errdefer allocator.free(owned);
        var seen = false;
        for (names.items) |prev| {
            if (std.mem.eql(u8, prev, owned)) seen = true;
        }
        if (seen) {
            allocator.free(owned);
            continue;
        }
        try names.append(allocator, owned);
    }
    const truncated = std.mem.indexOf(u8, xml, "<IsTruncated>true</IsTruncated>") != null or
        std.mem.indexOf(u8, xml, "<NextMarker>") != null;
    const token = if (xmlSpan(xml, "<NextContinuationToken>", "</NextContinuationToken>")) |raw|
        try xmlUnescape(allocator, raw)
    else if (xmlSpan(xml, "<NextMarker>", "</NextMarker>")) |raw|
        try xmlUnescape(allocator, raw)
    else
        null;
    errdefer if (token) |tok| allocator.free(tok);
    return .{
        .names = try names.toOwnedSlice(allocator),
        .token = token,
        .truncated = truncated and token != null,
    };
}

const StoreKind = enum { s3, gs, az };

fn storeKind(location: []const u8) StoreKind {
    if (aws.isGs(location)) return .gs;
    if (aws.isAz(location)) return .az;
    return .s3;
}

fn listPrefixOnce(t: Transport, allocator: std.mem.Allocator, location: []const u8) ![][]u8 {
    const kind = storeKind(location);
    const loc = switch (kind) {
        .gs => try aws.parseGs(location),
        .az => try aws.parseAz(location),
        .s3 => try aws.parseS3(location),
    };
    var prefix_owned: ?[]u8 = null;
    defer if (prefix_owned) |p| allocator.free(p);
    const prefix: []const u8 = if (loc.key.len == 0)
        ""
    else if (loc.key[loc.key.len - 1] == '/')
        loc.key
    else blk: {
        prefix_owned = try std.fmt.allocPrint(allocator, "{s}/", .{loc.key});
        break :blk prefix_owned.?;
    };

    var endpoint_owned: ?[]u8 = null;
    defer if (endpoint_owned) |e| allocator.free(e);
    const endpoint: ?[]const u8 = switch (kind) {
        .gs => t.gcs_endpoint,
        .az => t.azure_endpoint,
        .s3 => if (t.s3_creds != null)
            t.s3_endpoint
        else blk: {
            endpoint_owned = aws.endpointFromEnv(allocator);
            break :blk endpoint_owned;
        },
    };

    var region_owned: ?[]u8 = null;
    defer if (region_owned) |r| allocator.free(r);
    const region: []const u8 = if (kind != .s3)
        "us-east-1"
    else if (t.s3_region) |r|
        r
    else if (t.s3_creds) |c|
        c.region
    else blk: {
        region_owned = try aws.defaultRegion(allocator, t.io);
        break :blk region_owned.?;
    };

    var names: std.ArrayList([]u8) = .empty;
    errdefer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }
    var token: ?[]u8 = null;
    defer if (token) |tok| allocator.free(tok);
    var page_i: usize = 0;
    while (page_i < 32) : (page_i += 1) {
        const url = switch (kind) {
            .az => try aws.azureListUrl(allocator, loc, endpoint, prefix, token, true),
            else => try aws.listObjectsUrl(allocator, loc, region, endpoint, prefix, token, kind == .gs, true),
        };
        defer allocator.free(url);
        const body = try getAuthed(t, allocator, url, kind);
        defer allocator.free(body);
        const page = try parseListPage(allocator, body, prefix);
        defer allocator.free(page.names);
        for (page.names) |n| try names.append(allocator, n);
        if (token) |old| allocator.free(old);
        token = page.token;
        if (!page.truncated) break;
    }
    return names.items;
}

fn getAuthed(t: Transport, allocator: std.mem.Allocator, url: []const u8, kind: StoreKind) ![]u8 {
    const owned_url = try allocator.dupe(u8, url);
    var sign: ?FileSource.Sign = null;
    var bearer: ?[]u8 = null;
    var project: ?[]u8 = null;
    var creds_tmp: ?aws.Credentials = null;
    defer {
        allocator.free(owned_url);
        if (sign) |s| freeSign(allocator, s);
        if (bearer) |b| allocator.free(b);
        if (project) |p| allocator.free(p);
        if (creds_tmp) |c| {
            allocator.free(c.access_key);
            allocator.free(c.secret_key);
            if (c.session_token) |tok| allocator.free(tok);
            allocator.free(c.region);
        }
    }
    if (kind == .gs) {
        bearer = try dupeOptional(allocator, gcsToken(t));
    } else if (kind == .az) {
        bearer = try dupeOptional(allocator, azureToken(t));
    } else if (t.s3_creds) |c| {
        sign = try dupeSign(allocator, .{
            .access_key = c.access_key,
            .secret_key = c.secret_key,
            .session_token = c.session_token,
            .region = t.s3_region orelse c.region,
        });
    } else if (aws.loadCredentials(allocator, t.io)) |c| {
        creds_tmp = c;
        sign = try dupeSign(allocator, c);
    } else |err| switch (err) {
        error.AwsCredentialsMissing => {},
        else => return err,
    }
    project = try dupeOptional(allocator, if (kind == .gs) t.gcs_user_project else null);
    const h = FileSource.Http{
        .allocator = allocator,
        .client = t.http,
        .url = owned_url,
        .len = 0,
        .sign = sign,
        .bearer = bearer,
        .gcs_project = project,
    };
    return httpReadBody(h, allocator);
}

/// One level of child prefixes under an `s3://`, `gs://`, or `az://` location.
pub fn listPrefixNames(t: Transport, allocator: std.mem.Allocator, location: []const u8) ![][]u8 {
    return listPrefixOnce(t, allocator, location);
}

fn parseKeyPage(allocator: std.mem.Allocator, xml: []const u8) !struct { keys: [][]u8, token: ?[]u8, truncated: bool } {
    var keys: std.ArrayList([]u8) = .empty;
    errdefer {
        for (keys.items) |k| allocator.free(k);
        keys.deinit(allocator);
    }
    const tags = [_][2][]const u8{ .{ "<Key>", "</Key>" }, .{ "<Name>", "</Name>" } };
    for (tags) |tag| {
        var i: usize = 0;
        while (std.mem.indexOfPos(u8, xml, i, tag[0])) |at| {
            const start = at + tag[0].len;
            const end = std.mem.indexOfPos(u8, xml, start, tag[1]) orelse break;
            i = end + tag[1].len;
            const text = try xmlUnescape(allocator, xml[start..end]);
            errdefer allocator.free(text);
            if (text.len == 0 or text[text.len - 1] == '/') {
                allocator.free(text);
                continue;
            }
            try keys.append(allocator, text);
        }
        if (keys.items.len > 0) break;
    }
    const token = if (xmlSpan(xml, "<NextContinuationToken>", "</NextContinuationToken>")) |raw|
        try xmlUnescape(allocator, raw)
    else if (xmlSpan(xml, "<NextMarker>", "</NextMarker>")) |raw|
        try xmlUnescape(allocator, raw)
    else
        null;
    const truncated = (std.mem.indexOf(u8, xml, "<IsTruncated>true</IsTruncated>") != null or token != null) and token != null;
    return .{ .keys = try keys.toOwnedSlice(allocator), .token = token, .truncated = truncated };
}

/// Object keys under `location` (no delimiter). Each name is a full `s3://` / `gs://` / `az://` path.
pub fn listObjectKeys(t: Transport, allocator: std.mem.Allocator, location: []const u8) ![][]u8 {
    const kind = storeKind(location);
    const loc = switch (kind) {
        .gs => try aws.parseGs(location),
        .az => try aws.parseAz(location),
        .s3 => try aws.parseS3(location),
    };
    var prefix_owned: ?[]u8 = null;
    defer if (prefix_owned) |p| allocator.free(p);
    const prefix: []const u8 = if (loc.key.len == 0)
        ""
    else if (loc.key[loc.key.len - 1] == '/')
        loc.key
    else blk: {
        prefix_owned = try std.fmt.allocPrint(allocator, "{s}/", .{loc.key});
        break :blk prefix_owned.?;
    };
    var endpoint_owned: ?[]u8 = null;
    defer if (endpoint_owned) |e| allocator.free(e);
    const endpoint: ?[]const u8 = switch (kind) {
        .gs => t.gcs_endpoint,
        .az => t.azure_endpoint,
        .s3 => if (t.s3_creds != null) t.s3_endpoint else blk: {
            endpoint_owned = aws.endpointFromEnv(allocator);
            break :blk endpoint_owned;
        },
    };
    const region: []const u8 = if (t.s3_region) |r| r else if (t.s3_creds) |c| c.region else "us-east-1";
    const scheme: []const u8 = switch (kind) {
        .s3 => "s3",
        .gs => "gs",
        .az => "az",
    };
    var out: std.ArrayList([]u8) = .empty;
    errdefer {
        for (out.items) |k| allocator.free(k);
        out.deinit(allocator);
    }
    var token: ?[]u8 = null;
    defer if (token) |tok| allocator.free(tok);
    var page_i: usize = 0;
    while (page_i < 32) : (page_i += 1) {
        const url = switch (kind) {
            .az => try aws.azureListUrl(allocator, loc, endpoint, prefix, token, false),
            else => try aws.listObjectsUrl(allocator, loc, region, endpoint, prefix, token, kind == .gs, false),
        };
        defer allocator.free(url);
        const body = try getAuthed(t, allocator, url, kind);
        defer allocator.free(body);
        const page = try parseKeyPage(allocator, body);
        defer allocator.free(page.keys);
        for (page.keys) |key| {
            if (!std.mem.startsWith(u8, key, prefix)) {
                allocator.free(key);
                continue;
            }
            const full = try std.fmt.allocPrint(allocator, "{s}://{s}/{s}", .{ scheme, loc.bucket, key });
            allocator.free(key);
            try out.append(allocator, full);
        }
        if (token) |old| allocator.free(old);
        token = page.token;
        if (!page.truncated) break;
    }
    return out.toOwnedSlice(allocator);
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
    if (got.status == .ok or got.status == .partial_content) {
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

fn azureToken(t: Transport) ?[]const u8 {
    if (t.azure_token) |tok| {
        if (tok.len > 0) return tok;
    }
    if (comptime @import("builtin").cpu.arch == .wasm32) return null;
    if (std.c.getenv("AZURE_STORAGE_TOKEN")) |p| {
        const s = std.mem.span(p);
        if (s.len > 0) return s;
    }
    return null;
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

/// Local `createFile`, or a multipart upload for `s3://` (SigV4), `gs://` (Bearer),
/// and `az://` (block blobs, Bearer).
/// `GLACIER_PUT_PART` is the part size in bytes (default 8 MiB, at least 5 MiB). The last part is the remainder.
/// `http(s)://` destinations are refused.
pub fn putLocation(t: Transport, path: []const u8, bytes: []const u8) !void {
    const loc = stripFileScheme(path);
    if (aws.isS3(loc)) {
        try putS3(t, loc, bytes);
    } else if (aws.isGs(loc)) {
        try putGs(t, loc, bytes);
    } else if (aws.isAz(loc)) {
        try putAz(t, loc, bytes);
    } else if (aws.isHttp(loc)) {
        return error.WriteUnsupported;
    } else {
        try putLocal(t.io, loc, bytes);
    }
    if (aws.isRemote(loc)) forgetCached(t, loc);
}

pub fn deleteLocation(t: Transport, path: []const u8) !void {
    const loc = stripFileScheme(path);
    if (aws.isS3(loc)) {
        try deleteS3(t, loc);
    } else if (aws.isGs(loc)) {
        try deleteGs(t, loc);
    } else if (aws.isAz(loc)) {
        try deleteAz(t, loc);
    } else if (aws.isHttp(loc)) {
        return error.WriteUnsupported;
    } else {
        std.Io.Dir.cwd().deleteFile(t.io, loc) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
    if (aws.isRemote(loc)) forgetCached(t, loc);
}

/// Same path as `cache.filePath`. Drops a stale object after PUT or DELETE.
fn forgetCached(t: Transport, location: []const u8) void {
    if (comptime @import("builtin").cpu.arch == .wasm32) return;
    const dest = cachedFilePath(t.allocator, location) catch return;
    defer t.allocator.free(dest);
    std.Io.Dir.cwd().deleteFile(t.io, dest) catch {};
}

fn cachedFilePath(allocator: std.mem.Allocator, location: []const u8) ![]u8 {
    const root = blk: {
        if (std.c.getenv("GLACIER_TEMP")) |p| {
            const s = std.mem.span(p);
            if (s.len > 0) break :blk s;
        }
        break :blk "/tmp/glacier";
    };
    const dir = try std.fs.path.join(allocator, &.{ root, "cache" });
    defer allocator.free(dir);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(location, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.fs.path.join(allocator, &.{ dir, &hex });
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
    try putMultipart(t, url, bytes, creds, null, null);
}

fn putGs(t: Transport, path: []const u8, bytes: []const u8) !void {
    const token = gcsToken(t) orelse return error.AccessDenied;
    const loc = try aws.parseGs(path);
    const url = try aws.httpUrlForGs(t.allocator, loc, t.gcs_endpoint);
    defer t.allocator.free(url);
    try putMultipart(t, url, bytes, null, token, t.gcs_user_project);
}

fn putAz(t: Transport, path: []const u8, bytes: []const u8) !void {
    const token = azureToken(t) orelse return error.AccessDenied;
    const loc = try aws.parseAz(path);
    const url = try aws.httpUrlForAz(t.allocator, loc, t.azure_endpoint);
    defer t.allocator.free(url);
    try putAzureBlocks(t, url, bytes, token);
}

fn deleteObject(t: Transport, url: []const u8, creds: ?aws.Credentials, bearer: ?[]const u8, project: ?[]const u8) !void {
    const ex = try objectExchange(t, .DELETE, url, creds, bearer, project, "");
    defer t.allocator.free(ex.body);
    defer if (ex.etag) |e| t.allocator.free(e);
    try expectObjectOk(ex);
}

fn deleteS3(t: Transport, path: []const u8) !void {
    if (t.s3_creds) |creds| {
        const resolved = aws.Credentials{
            .access_key = creds.access_key,
            .secret_key = creds.secret_key,
            .session_token = creds.session_token,
            .region = t.s3_region orelse creds.region,
        };
        const loc = try aws.parseS3(path);
        const url = try aws.httpUrlForS3(t.allocator, loc, resolved.region, t.s3_endpoint);
        defer t.allocator.free(url);
        return deleteObject(t, url, resolved, null, null);
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
    const loc = try aws.parseS3(path);
    const url = try aws.httpUrlForS3(t.allocator, loc, resolved.region, endpoint);
    defer t.allocator.free(url);
    return deleteObject(t, url, resolved, null, null);
}

fn deleteGs(t: Transport, path: []const u8) !void {
    const token = gcsToken(t) orelse return error.AccessDenied;
    const loc = try aws.parseGs(path);
    const url = try aws.httpUrlForGs(t.allocator, loc, t.gcs_endpoint);
    defer t.allocator.free(url);
    return deleteObject(t, url, null, token, t.gcs_user_project);
}

fn deleteAz(t: Transport, path: []const u8) !void {
    const token = azureToken(t) orelse return error.AccessDenied;
    const loc = try aws.parseAz(path);
    const url = try aws.httpUrlForAz(t.allocator, loc, t.azure_endpoint);
    defer t.allocator.free(url);
    return deleteObject(t, url, null, token, null);
}

const put_part_default: usize = 8 * 1024 * 1024;
const put_part_floor: usize = 5 * 1024 * 1024;

fn putPartCap(t: Transport) usize {
    if (t.put_part > 0) return t.put_part;
    if (comptime @import("builtin").cpu.arch != .wasm32) {
        if (std.c.getenv("GLACIER_PUT_PART")) |p| {
            const n = std.fmt.parseInt(usize, std.mem.span(p), 10) catch return put_part_default;
            if (n >= put_part_floor) return n;
        }
    }
    return put_part_default;
}

const Exchange = struct {
    status: std.http.Status,
    body: []u8,
    etag: ?[]u8 = null,
};

fn expectObjectOk(ex: Exchange) !void {
    switch (ex.status) {
        .ok, .created, .no_content => {
            if (std.mem.indexOf(u8, ex.body, "<Error>") != null) return error.ObjectPutFailed;
        },
        .unauthorized, .forbidden => return error.AccessDenied,
        else => return error.ObjectPutFailed,
    }
}

fn withQuery(allocator: std.mem.Allocator, object_url: []const u8, query: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}?{s}", .{ object_url, query });
}

fn objectExchange(
    t: Transport,
    method: std.http.Method,
    url: []const u8,
    creds: ?aws.Credentials,
    bearer: ?[]const u8,
    project: ?[]const u8,
    payload: []const u8,
) !Exchange {
    var hdrs: [8]std.http.Header = undefined;
    var n: usize = 0;
    var date_buf: [16]u8 = undefined;
    var hash_buf: [64]u8 = undefined;
    var auth_owned: ?[]u8 = null;
    defer if (auth_owned) |p| t.allocator.free(p);

    if (creds) |c| {
        hash_buf = aws.sha256Hex(payload);
        const amz_date = aws.nowAmzDate(t.io, &date_buf);
        auth_owned = try aws.authorization(t.allocator, .{
            .method = methodName(method),
            .host = urlHost(url),
            .path = urlPath(url),
            .query = urlQuery(url),
            .payload_hash = &hash_buf,
            .amz_date = amz_date,
            .region = c.region,
            .creds = c,
        });
        hdrs[n] = .{ .name = "x-amz-date", .value = amz_date };
        n += 1;
        hdrs[n] = .{ .name = "x-amz-content-sha256", .value = &hash_buf };
        n += 1;
        hdrs[n] = .{ .name = "authorization", .value = auth_owned.? };
        n += 1;
        if (c.session_token) |tok| {
            hdrs[n] = .{ .name = "x-amz-security-token", .value = tok };
            n += 1;
        }
    } else if (bearer) |tok| {
        auth_owned = try std.fmt.allocPrint(t.allocator, "Bearer {s}", .{tok});
        hdrs[n] = .{ .name = "authorization", .value = auth_owned.? };
        n += 1;
        if (project) |proj| {
            if (proj.len > 0) {
                hdrs[n] = .{ .name = "x-goog-user-project", .value = proj };
                n += 1;
            }
        }
    }

    const uri = std.Uri.parse(url) catch return error.ObjectPutFailed;
    var req = t.http.request(method, uri, .{
        .keep_alive = false,
        .headers = .{ .accept_encoding = .omit },
        .extra_headers = hdrs[0..n],
        .redirect_behavior = .unhandled,
    }) catch return error.ObjectPutFailed;
    defer req.deinit();

    if (method.requestHasBody()) {
        req.transfer_encoding = .{ .content_length = payload.len };
        var sent = req.sendBodyUnflushed(&.{}) catch return error.ObjectPutFailed;
        sent.writer.writeAll(payload) catch return error.ObjectPutFailed;
        sent.end() catch return error.ObjectPutFailed;
        req.connection.?.flush() catch return error.ObjectPutFailed;
    } else {
        req.sendBodiless() catch return error.ObjectPutFailed;
    }

    var head_buf: [8192]u8 = undefined;
    var response = req.receiveHead(&head_buf) catch return error.ObjectPutFailed;
    const etag = if (headerValue(response.head.bytes, "etag")) |v|
        t.allocator.dupe(u8, std.mem.trim(u8, v, " \t\r\n")) catch return error.ObjectPutFailed
    else
        null;
    errdefer if (etag) |e| t.allocator.free(e);

    var transfer_buffer: [512]u8 = undefined;
    var reader = response.reader(&transfer_buffer);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(t.allocator);
    var tmp: [8192]u8 = undefined;
    while (out.items.len < 1024 * 1024) {
        const got = reader.readSliceShort(&tmp) catch |err| switch (err) {
            error.ReadFailed => return error.ObjectPutFailed,
            else => |e| return e,
        };
        if (got == 0) break;
        try out.appendSlice(t.allocator, tmp[0..got]);
    }
    if (out.items.len >= 1024 * 1024) return error.ObjectPutFailed;
    return .{
        .status = response.head.status,
        .body = try out.toOwnedSlice(t.allocator),
        .etag = etag,
    };
}

fn completeXml(allocator: std.mem.Allocator, etags: []const []const u8) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    try aw.writer.writeAll("<CompleteMultipartUpload>");
    for (etags, 0..) |etag, i| {
        try aw.writer.print("<Part><PartNumber>{d}</PartNumber><ETag>", .{i + 1});
        const quoted = etag.len >= 2 and etag[0] == '"' and etag[etag.len - 1] == '"';
        if (!quoted) try aw.writer.writeByte('"');
        for (etag) |c| switch (c) {
            '&' => try aw.writer.writeAll("&amp;"),
            '<' => try aw.writer.writeAll("&lt;"),
            '>' => try aw.writer.writeAll("&gt;"),
            else => try aw.writer.writeByte(c),
        };
        if (!quoted) try aw.writer.writeByte('"');
        try aw.writer.writeAll("</ETag></Part>");
    }
    try aw.writer.writeAll("</CompleteMultipartUpload>");
    return aw.toOwnedSlice();
}

fn abortMultipart(
    t: Transport,
    object_url: []const u8,
    upload_id: []const u8,
    creds: ?aws.Credentials,
    bearer: ?[]const u8,
    project: ?[]const u8,
) void {
    const enc = aws.uriEncode(t.allocator, upload_id, true) catch return;
    defer t.allocator.free(enc);
    const query = std.fmt.allocPrint(t.allocator, "uploadId={s}", .{enc}) catch return;
    defer t.allocator.free(query);
    const url = withQuery(t.allocator, object_url, query) catch return;
    defer t.allocator.free(url);
    const ex = objectExchange(t, .DELETE, url, creds, bearer, project, "") catch return;
    t.allocator.free(ex.body);
    if (ex.etag) |e| t.allocator.free(e);
}

fn putMultipart(
    t: Transport,
    object_url: []const u8,
    bytes: []const u8,
    creds: ?aws.Credentials,
    bearer: ?[]const u8,
    project: ?[]const u8,
) !void {
    const upload_id = blk: {
        const url = try withQuery(t.allocator, object_url, "uploads");
        defer t.allocator.free(url);
        const ex = try objectExchange(t, .POST, url, creds, bearer, project, "");
        defer t.allocator.free(ex.body);
        defer if (ex.etag) |e| t.allocator.free(e);
        try expectObjectOk(ex);
        const span = xmlSpan(ex.body, "<UploadId>", "</UploadId>") orelse return error.ObjectPutFailed;
        const raw = std.mem.trim(u8, span, " \t\r\n");
        if (raw.len == 0) return error.ObjectPutFailed;
        break :blk try xmlUnescape(t.allocator, raw);
    };
    defer t.allocator.free(upload_id);
    var finished = false;
    errdefer if (!finished) abortMultipart(t, object_url, upload_id, creds, bearer, project);

    var etags: std.ArrayList([]u8) = .empty;
    defer {
        for (etags.items) |e| t.allocator.free(e);
        etags.deinit(t.allocator);
    }
    const cap = putPartCap(t);
    var off: usize = 0;
    var part_no: usize = 1;
    while (off < bytes.len or part_no == 1) {
        const n = if (off >= bytes.len) 0 else @min(cap, bytes.len - off);
        const chunk = if (n == 0) bytes[0..0] else bytes[off..][0..n];
        const enc = try aws.uriEncode(t.allocator, upload_id, true);
        defer t.allocator.free(enc);
        const query = try std.fmt.allocPrint(t.allocator, "partNumber={d}&uploadId={s}", .{ part_no, enc });
        defer t.allocator.free(query);
        const url = try withQuery(t.allocator, object_url, query);
        defer t.allocator.free(url);
        const ex = try objectExchange(t, .PUT, url, creds, bearer, project, chunk);
        defer t.allocator.free(ex.body);
        try expectObjectOk(ex);
        const etag = ex.etag orelse return error.ObjectPutFailed;
        try etags.append(t.allocator, etag);
        off += n;
        part_no += 1;
        if (off >= bytes.len) break;
    }

    const xml = try completeXml(t.allocator, etags.items);
    defer t.allocator.free(xml);
    const enc = try aws.uriEncode(t.allocator, upload_id, true);
    defer t.allocator.free(enc);
    const query = try std.fmt.allocPrint(t.allocator, "uploadId={s}", .{enc});
    defer t.allocator.free(query);
    const url = try withQuery(t.allocator, object_url, query);
    defer t.allocator.free(url);
    const ex = try objectExchange(t, .POST, url, creds, bearer, project, xml);
    defer t.allocator.free(ex.body);
    defer if (ex.etag) |e| t.allocator.free(e);
    try expectObjectOk(ex);
    finished = true;
}

fn putAzureBlocks(t: Transport, object_url: []const u8, bytes: []const u8, bearer: []const u8) !void {
    var ids: std.ArrayList([]u8) = .empty;
    defer {
        for (ids.items) |id| t.allocator.free(id);
        ids.deinit(t.allocator);
    }
    const cap = putPartCap(t);
    var off: usize = 0;
    var part_no: usize = 1;
    while (off < bytes.len or part_no == 1) {
        const n = if (off >= bytes.len) 0 else @min(cap, bytes.len - off);
        const chunk = if (n == 0) bytes[0..0] else bytes[off..][0..n];
        var raw: [16]u8 = undefined;
        const label = std.fmt.bufPrint(&raw, "part-{d:0>5}", .{part_no}) catch return error.ObjectPutFailed;
        const id = try t.allocator.alloc(u8, std.base64.standard.Encoder.calcSize(label.len));
        _ = std.base64.standard.Encoder.encode(id, label);
        const enc = try aws.uriEncode(t.allocator, id, true);
        defer t.allocator.free(enc);
        const query = try std.fmt.allocPrint(t.allocator, "blockid={s}&comp=block", .{enc});
        defer t.allocator.free(query);
        const url = try withQuery(t.allocator, object_url, query);
        defer t.allocator.free(url);
        const ex = try objectExchange(t, .PUT, url, null, bearer, null, chunk);
        defer t.allocator.free(ex.body);
        defer if (ex.etag) |e| t.allocator.free(e);
        try expectObjectOk(ex);
        try ids.append(t.allocator, id);
        off += n;
        part_no += 1;
        if (off >= bytes.len) break;
    }
    var aw: std.Io.Writer.Allocating = .init(t.allocator);
    defer aw.deinit();
    try aw.writer.writeAll("<BlockList>");
    for (ids.items) |id| {
        try aw.writer.writeAll("<Latest>");
        try aw.writer.writeAll(id);
        try aw.writer.writeAll("</Latest>");
    }
    try aw.writer.writeAll("</BlockList>");
    const xml = try aw.toOwnedSlice();
    defer t.allocator.free(xml);
    const url = try withQuery(t.allocator, object_url, "comp=blocklist");
    defer t.allocator.free(url);
    const ex = try objectExchange(t, .PUT, url, null, bearer, null, xml);
    defer t.allocator.free(ex.body);
    defer if (ex.etag) |e| t.allocator.free(e);
    try expectObjectOk(ex);
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

test "complete multipart xml keeps part numbers and etags" {
    var etags = [_][]const u8{ "\"part-1\"", "ab&c" };
    const xml = try completeXml(std.testing.allocator, &etags);
    defer std.testing.allocator.free(xml);
    try std.testing.expectEqualStrings(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"part-1\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"ab&amp;c\"</ETag></Part></CompleteMultipartUpload>",
        xml,
    );
}

test "list page keeps one directory level" {
    const xml =
        \\<ListBucketResult>
        \\<Prefix>sales/</Prefix>
        \\<CommonPrefixes><Prefix>sales/prune/</Prefix></CommonPrefixes>
        \\<CommonPrefixes><Prefix>sales/meta&amp;data/</Prefix></CommonPrefixes>
        \\<IsTruncated>true</IsTruncated>
        \\<NextContinuationToken>tok&amp;en</NextContinuationToken>
        \\</ListBucketResult>
    ;
    const page = try parseListPage(std.testing.allocator, xml, "sales/");
    defer {
        for (page.names) |n| std.testing.allocator.free(n);
        std.testing.allocator.free(page.names);
        if (page.token) |tok| std.testing.allocator.free(tok);
    }
    try std.testing.expect(page.truncated);
    try std.testing.expectEqualStrings("tok&en", page.token.?);
    try std.testing.expectEqual(@as(usize, 2), page.names.len);
    try std.testing.expectEqualStrings("prune", page.names[0]);
    try std.testing.expectEqualStrings("meta&data", page.names[1]);
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
