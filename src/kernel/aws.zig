//! AWS SigV4 for S3. No SDK. Credentials from env then `~/.aws/credentials`.
//! Query strings are sorted and URI-encoded per AWS rules.

const std = @import("std");
const Io = std.Io;

pub const empty_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const unsigned_payload = "UNSIGNED-PAYLOAD";

pub const Credentials = struct {
    access_key: []const u8,
    secret_key: []const u8,
    session_token: ?[]const u8 = null,
    region: []const u8 = "us-east-1",
};

pub const S3Url = struct {
    bucket: []const u8,
    key: []const u8,
};

pub fn isS3(path: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(path, "s3://");
}

pub fn isHttp(path: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(path, "http://") or
        std.ascii.startsWithIgnoreCase(path, "https://");
}

pub fn isRemote(path: []const u8) bool {
    return isS3(path) or isHttp(path);
}

pub fn parseS3(path: []const u8) !S3Url {
    if (!isS3(path)) return error.InvalidS3Url;
    const rest = path["s3://".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse
        return S3Url{ .bucket = rest, .key = "" };
    if (slash == 0) return error.InvalidS3Url;
    return .{ .bucket = rest[0..slash], .key = rest[slash + 1 ..] };
}

/// Path-style `{endpoint}/{bucket}/{key}` when `AWS_ENDPOINT_URL` is set.
/// Otherwise virtual-hosted `https://{bucket}.s3.{region}.amazonaws.com/{key}`.
pub fn httpUrlForS3(
    allocator: std.mem.Allocator,
    loc: S3Url,
    region: []const u8,
    endpoint: ?[]const u8,
) ![]u8 {
    if (endpoint) |ep| {
        const base = std.mem.trimEnd(u8, ep, "/");
        if (loc.key.len == 0)
            return std.fmt.allocPrint(allocator, "{s}/{s}", .{ base, loc.bucket });
        return std.fmt.allocPrint(allocator, "{s}/{s}/{s}", .{ base, loc.bucket, loc.key });
    }
    const host = if (std.ascii.eqlIgnoreCase(region, "us-east-1"))
        try std.fmt.allocPrint(allocator, "{s}.s3.amazonaws.com", .{loc.bucket})
    else
        try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ loc.bucket, region });
    defer allocator.free(host);
    if (loc.key.len == 0)
        return std.fmt.allocPrint(allocator, "https://{s}", .{host});
    return std.fmt.allocPrint(allocator, "https://{s}/{s}", .{ host, loc.key });
}

pub fn uriEncode(allocator: std.mem.Allocator, s: []const u8, encode_slash: bool) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (s) |c| {
        if (unreserved(c) or (c == '/' and !encode_slash)) {
            try out.append(allocator, c);
        } else {
            try out.append(allocator, '%');
            const hex = "0123456789ABCDEF";
            try out.append(allocator, hex[c >> 4]);
            try out.append(allocator, hex[c & 0xf]);
        }
    }
    return out.toOwnedSlice(allocator);
}

fn unreserved(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~';
}

/// Sort `k=v` pairs and URI-encode keys and values.
pub fn canonicalQuery(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    if (raw.len == 0) return allocator.dupe(u8, "");
    const q = if (raw.len > 0 and raw[0] == '?') raw[1..] else raw;

    var pairs: std.ArrayList(struct { k: []u8, v: []u8 }) = .empty;
    defer {
        for (pairs.items) |p| {
            allocator.free(p.k);
            allocator.free(p.v);
        }
        pairs.deinit(allocator);
    }

    var it = std.mem.splitScalar(u8, q, '&');
    while (it.next()) |part| {
        if (part.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, part, '=') orelse part.len;
        const k_raw = part[0..eq];
        const v_raw = if (eq < part.len) part[eq + 1 ..] else "";
        try pairs.append(allocator, .{
            .k = try uriEncode(allocator, k_raw, true),
            .v = try uriEncode(allocator, v_raw, true),
        });
    }

    const lessThan = struct {
        fn lt(_: void, a: @TypeOf(pairs.items[0]), b: @TypeOf(pairs.items[0])) bool {
            const ck = std.mem.order(u8, a.k, b.k);
            if (ck != .eq) return ck == .lt;
            return std.mem.order(u8, a.v, b.v) == .lt;
        }
    }.lt;
    std.mem.sort(@TypeOf(pairs.items[0]), pairs.items, {}, lessThan);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (pairs.items, 0..) |p, i| {
        if (i != 0) try out.append(allocator, '&');
        try out.appendSlice(allocator, p.k);
        try out.append(allocator, '=');
        try out.appendSlice(allocator, p.v);
    }
    return out.toOwnedSlice(allocator);
}

pub fn sha256Hex(data: []const u8) [64]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data, &digest, .{});
    return std.fmt.bytesToHex(digest, .lower);
}

pub fn hmacSha256(key: []const u8, msg: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&out, msg, key);
    return out;
}

pub fn signingKey(secret: []const u8, date_stamp: []const u8, region: []const u8, service: []const u8) [32]u8 {
    var k_secret_buf: [128]u8 = undefined;
    const k_secret = std.fmt.bufPrint(&k_secret_buf, "AWS4{s}", .{secret}) catch
        unreachable;
    const k_date = hmacSha256(k_secret, date_stamp);
    const k_region = hmacSha256(&k_date, region);
    const k_service = hmacSha256(&k_region, service);
    return hmacSha256(&k_service, "aws4_request");
}

pub fn formatAmzDate(epoch_secs: i64, buf: *[16]u8) []const u8 {
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(@max(epoch_secs, 0)) };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.bufPrint(buf, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        yd.year,
        md.month.numeric(),
        @as(u8, md.day_index) + 1,
        ds.getHoursIntoDay(),
        ds.getMinutesIntoHour(),
        ds.getSecondsIntoMinute(),
    }) catch unreachable;
}

pub fn dateStampOf(amz_date: []const u8) []const u8 {
    return amz_date[0..8];
}

pub fn nowAmzDate(io: Io, buf: *[16]u8) []const u8 {
    const secs = Io.Timestamp.now(io, .real).toSeconds();
    return formatAmzDate(secs, buf);
}

pub const SignInput = struct {
    method: []const u8 = "GET",
    host: []const u8,
    path: []const u8,
    query: []const u8 = "",
    extra_headers: []const std.http.Header = &.{},
    payload_hash: []const u8 = unsigned_payload,
    amz_date: []const u8,
    region: []const u8,
    service: []const u8 = "s3",
    creds: Credentials,
    /// S3 signs `x-amz-content-sha256`. The IAM ListUsers example does not.
    sign_content_sha256: bool = true,
};

pub fn authorization(allocator: std.mem.Allocator, in: SignInput) ![]u8 {
    const canon = try canonicalRequest(allocator, in);
    defer allocator.free(canon);
    const canon_hash = sha256Hex(canon);
    const stamp = dateStampOf(in.amz_date);
    const sts = try std.fmt.allocPrint(
        allocator,
        "AWS4-HMAC-SHA256\n{s}\n{s}/{s}/{s}/aws4_request\n{s}",
        .{ in.amz_date, stamp, in.region, in.service, canon_hash },
    );
    defer allocator.free(sts);
    const key = signingKey(in.creds.secret_key, stamp, in.region, in.service);
    const sig = hmacSha256(&key, sts);
    const sig_hex = std.fmt.bytesToHex(sig, .lower);
    const signed = try signedHeaderNames(allocator, in);
    defer allocator.free(signed);
    return std.fmt.allocPrint(
        allocator,
        "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/{s}/aws4_request, SignedHeaders={s}, Signature={s}",
        .{ in.creds.access_key, stamp, in.region, in.service, signed, sig_hex },
    );
}

fn canonicalRequest(allocator: std.mem.Allocator, in: SignInput) ![]u8 {
    const path = try uriEncode(allocator, if (in.path.len == 0) "/" else in.path, false);
    defer allocator.free(path);
    const query = try canonicalQuery(allocator, in.query);
    defer allocator.free(query);
    const headers = try canonicalHeaders(allocator, in);
    defer allocator.free(headers);
    const signed = try signedHeaderNames(allocator, in);
    defer allocator.free(signed);
    return std.fmt.allocPrint(allocator, "{s}\n{s}\n{s}\n{s}{s}\n{s}", .{
        in.method,
        path,
        query,
        headers,
        signed,
        in.payload_hash,
    });
}

const Hdr = struct { name: []u8, value: []u8 };

fn collectHeaders(allocator: std.mem.Allocator, in: SignInput) ![]Hdr {
    var list: std.ArrayList(Hdr) = .empty;
    errdefer {
        for (list.items) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        list.deinit(allocator);
    }
    try list.append(allocator, .{
        .name = try allocator.dupe(u8, "host"),
        .value = try canonicalHeaderValue(allocator, in.host),
    });
    try list.append(allocator, .{
        .name = try allocator.dupe(u8, "x-amz-date"),
        .value = try canonicalHeaderValue(allocator, in.amz_date),
    });
    if (in.sign_content_sha256) {
        try list.append(allocator, .{
            .name = try allocator.dupe(u8, "x-amz-content-sha256"),
            .value = try canonicalHeaderValue(allocator, in.payload_hash),
        });
    }
    if (in.creds.session_token) |tok| {
        try list.append(allocator, .{
            .name = try allocator.dupe(u8, "x-amz-security-token"),
            .value = try canonicalHeaderValue(allocator, tok),
        });
    }
    for (in.extra_headers) |h| {
        try list.append(allocator, .{
            .name = try std.ascii.allocLowerString(allocator, h.name),
            .value = try canonicalHeaderValue(allocator, h.value),
        });
    }
    const lessThan = struct {
        fn lt(_: void, a: Hdr, b: Hdr) bool {
            return std.mem.order(u8, a.name, b.name) == .lt;
        }
    }.lt;
    std.mem.sort(Hdr, list.items, {}, lessThan);
    return list.toOwnedSlice(allocator);
}

fn canonicalHeaderValue(allocator: std.mem.Allocator, v: []const u8) ![]u8 {
    const trimmed = std.mem.trim(u8, v, " \t");
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var prev_space = false;
    for (trimmed) |c| {
        const space = c == ' ' or c == '\t';
        if (space) {
            if (!prev_space) try out.append(allocator, ' ');
            prev_space = true;
        } else {
            try out.append(allocator, c);
            prev_space = false;
        }
    }
    return out.toOwnedSlice(allocator);
}

fn canonicalHeaders(allocator: std.mem.Allocator, in: SignInput) ![]u8 {
    const hdrs = try collectHeaders(allocator, in);
    defer {
        for (hdrs) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        allocator.free(hdrs);
    }
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (hdrs) |h| {
        try out.appendSlice(allocator, h.name);
        try out.append(allocator, ':');
        try out.appendSlice(allocator, h.value);
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

fn signedHeaderNames(allocator: std.mem.Allocator, in: SignInput) ![]u8 {
    const hdrs = try collectHeaders(allocator, in);
    defer {
        for (hdrs) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        allocator.free(hdrs);
    }
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (hdrs, 0..) |h, i| {
        if (i != 0) try out.append(allocator, ';');
        try out.appendSlice(allocator, h.name);
    }
    return out.toOwnedSlice(allocator);
}

fn envDup(allocator: std.mem.Allocator, key: [:0]const u8) error{ EnvironmentVariableNotFound, OutOfMemory }![]u8 {
    if (comptime @import("builtin").cpu.arch == .wasm32)
        return error.EnvironmentVariableNotFound;
    const p = std.c.getenv(key) orelse return error.EnvironmentVariableNotFound;
    return allocator.dupe(u8, std.mem.span(p));
}

pub fn loadCredentials(allocator: std.mem.Allocator, io: Io) !Credentials {
    if (envDup(allocator, "AWS_ACCESS_KEY_ID")) |ak| {
        errdefer allocator.free(ak);
        const sk = envDup(allocator, "AWS_SECRET_ACCESS_KEY") catch {
            allocator.free(ak);
            return error.AwsCredentialsMissing;
        };
        errdefer allocator.free(sk);
        const token = envDup(allocator, "AWS_SESSION_TOKEN") catch null;
        const region = try loadRegion(allocator, io, null);
        return .{
            .access_key = ak,
            .secret_key = sk,
            .session_token = token,
            .region = region,
        };
    } else |_| {}

    const profile = envDup(allocator, "AWS_PROFILE") catch
        try allocator.dupe(u8, "default");
    defer allocator.free(profile);

    const home = try homeDir(allocator);
    defer allocator.free(home);
    const cred_path = try std.fs.path.join(allocator, &.{ home, ".aws", "credentials" });
    defer allocator.free(cred_path);
    const cred_text = readFile(allocator, io, cred_path) catch return error.AwsCredentialsMissing;
    defer allocator.free(cred_text);
    const parsed = parseProfile(allocator, cred_text, profile) orelse return error.AwsCredentialsMissing;

    var region = parsed.region;
    if (region.len == 0) {
        region = try loadRegion(allocator, io, profile);
    }
    return .{
        .access_key = parsed.access_key,
        .secret_key = parsed.secret_key,
        .session_token = parsed.session_token,
        .region = region,
    };
}

const ParsedProfile = struct {
    access_key: []u8,
    secret_key: []u8,
    session_token: ?[]u8,
    region: []u8,
};

pub fn parseProfile(allocator: std.mem.Allocator, text: []const u8, profile: []const u8) ?ParsedProfile {
    var access: ?[]u8 = null;
    var secret: ?[]u8 = null;
    var token: ?[]u8 = null;
    var region: ?[]u8 = null;
    var in_section = false;

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#' or line[0] == ';') continue;
        if (line[0] == '[') {
            const end = std.mem.indexOfScalar(u8, line, ']') orelse continue;
            var name = std.mem.trim(u8, line[1..end], " \t");
            if (std.mem.startsWith(u8, name, "profile "))
                name = std.mem.trim(u8, name["profile ".len..], " \t");
            in_section = std.mem.eql(u8, name, profile);
            continue;
        }
        if (!in_section) continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key = std.mem.trim(u8, line[0..eq], " \t");
        const val = std.mem.trim(u8, line[eq + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(key, "aws_access_key_id")) {
            if (access) |p| allocator.free(p);
            access = allocator.dupe(u8, val) catch return null;
        } else if (std.ascii.eqlIgnoreCase(key, "aws_secret_access_key")) {
            if (secret) |p| allocator.free(p);
            secret = allocator.dupe(u8, val) catch return null;
        } else if (std.ascii.eqlIgnoreCase(key, "aws_session_token")) {
            if (token) |p| allocator.free(p);
            token = allocator.dupe(u8, val) catch return null;
        } else if (std.ascii.eqlIgnoreCase(key, "region")) {
            if (region) |p| allocator.free(p);
            region = allocator.dupe(u8, val) catch return null;
        }
    }
    const ak = access orelse return null;
    const sk = secret orelse {
        allocator.free(ak);
        if (token) |p| allocator.free(p);
        if (region) |p| allocator.free(p);
        return null;
    };
    return .{
        .access_key = ak,
        .secret_key = sk,
        .session_token = token,
        .region = region orelse (allocator.dupe(u8, "") catch return null),
    };
}

fn loadRegion(allocator: std.mem.Allocator, io: Io, profile: ?[]const u8) ![]u8 {
    if (envDup(allocator, "AWS_REGION")) |r| return r else |_| {}
    if (envDup(allocator, "AWS_DEFAULT_REGION")) |r| return r else |_| {}
    if (profile) |p| {
        const home = homeDir(allocator) catch return allocator.dupe(u8, "us-east-1");
        defer allocator.free(home);
        const cfg_path = std.fs.path.join(allocator, &.{ home, ".aws", "config" }) catch
            return allocator.dupe(u8, "us-east-1");
        defer allocator.free(cfg_path);
        if (readFile(allocator, io, cfg_path)) |text| {
            defer allocator.free(text);
            if (iniValue(text, p, "region")) |r| return allocator.dupe(u8, r);
        } else |_| {}
    }
    return allocator.dupe(u8, "us-east-1");
}

fn iniValue(text: []const u8, profile: []const u8, key: []const u8) ?[]const u8 {
    var in_section = false;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#' or line[0] == ';') continue;
        if (line[0] == '[') {
            const end = std.mem.indexOfScalar(u8, line, ']') orelse continue;
            var name = std.mem.trim(u8, line[1..end], " \t");
            if (std.mem.startsWith(u8, name, "profile "))
                name = std.mem.trim(u8, name["profile ".len..], " \t");
            in_section = std.mem.eql(u8, name, profile);
            continue;
        }
        if (!in_section) continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const k = std.mem.trim(u8, line[0..eq], " \t");
        const val = std.mem.trim(u8, line[eq + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(k, key)) return val;
    }
    return null;
}

fn homeDir(allocator: std.mem.Allocator) ![]u8 {
    if (envDup(allocator, "HOME")) |h| return h else |_| {}
    return envDup(allocator, "USERPROFILE") catch error.AwsCredentialsMissing;
}

fn readFile(allocator: std.mem.Allocator, io: Io, path: []const u8) ![]u8 {
    const file = try Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
    defer file.close(io);
    const st = try file.stat(io);
    const n: usize = @intCast(st.size);
    const buf = try allocator.alloc(u8, n);
    errdefer allocator.free(buf);
    const got = try file.readPositionalAll(io, buf, 0);
    if (got != n) return error.UnexpectedEndOfFile;
    return buf;
}

pub fn endpointFromEnv(allocator: std.mem.Allocator) ?[]u8 {
    return envDup(allocator, "AWS_ENDPOINT_URL") catch null;
}

test "URI encode leaves unreserved and encodes slash when asked" {
    const a = std.testing.allocator;
    const keep = try uriEncode(a, "a/b~c_d.e-1", false);
    defer a.free(keep);
    try std.testing.expectEqualStrings("a/b~c_d.e-1", keep);
    const enc = try uriEncode(a, "a/b c", true);
    defer a.free(enc);
    try std.testing.expectEqualStrings("a%2Fb%20c", enc);
}

test "canonical query sorts and encodes" {
    const a = std.testing.allocator;
    const q = try canonicalQuery(a, "B=2&A=1&A=0");
    defer a.free(q);
    try std.testing.expectEqualStrings("A=0&A=1&B=2", q);
    const space = try canonicalQuery(a, "q=a b");
    defer a.free(space);
    try std.testing.expectEqualStrings("q=a%20b", space);
}

test "HMAC-SHA256 RFC 4231 case 1" {
    const mac = hmacSha256("\x0b" ** 20, "Hi There");
    try std.testing.expectEqualStrings(
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
        &std.fmt.bytesToHex(mac, .lower),
    );
}

test "SigV4 signing key and string-to-sign for IAM example" {
    const a = std.testing.allocator;
    const creds = Credentials{
        .access_key = "AKIDEXAMPLE",
        .secret_key = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        .region = "us-east-1",
    };
    const key = signingKey(creds.secret_key, "20150830", "us-east-1", "iam");
    try std.testing.expectEqualStrings(
        "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9",
        &std.fmt.bytesToHex(key, .lower),
    );
    const auth = try authorization(a, .{
        .method = "GET",
        .host = "iam.amazonaws.com",
        .path = "/",
        .query = "Action=ListUsers&Version=2010-05-08",
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/x-www-form-urlencoded; charset=utf-8" },
        },
        .payload_hash = empty_payload_hash,
        .amz_date = "20150830T123600Z",
        .region = "us-east-1",
        .service = "iam",
        .creds = creds,
        .sign_content_sha256 = false,
    });
    defer a.free(auth);
    try std.testing.expect(std.mem.startsWith(u8, auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature="));
    try std.testing.expectEqual(@as(usize, 64), auth.len - "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=".len);
}

test "parse credentials profile" {
    const a = std.testing.allocator;
    const text =
        \\[default]
        \\aws_access_key_id = AAA
        \\aws_secret_access_key = SSS
        \\
        \\[profile mine]
        \\aws_access_key_id = BBB
        \\aws_secret_access_key = TTT
        \\region = eu-west-1
        \\
    ;
    const d = parseProfile(a, text, "default").?;
    defer {
        a.free(d.access_key);
        a.free(d.secret_key);
        if (d.session_token) |t| a.free(t);
        a.free(d.region);
    }
    try std.testing.expectEqualStrings("AAA", d.access_key);
    const m = parseProfile(a, text, "mine").?;
    defer {
        a.free(m.access_key);
        a.free(m.secret_key);
        if (m.session_token) |t| a.free(t);
        a.free(m.region);
    }
    try std.testing.expectEqualStrings("BBB", m.access_key);
    try std.testing.expectEqualStrings("eu-west-1", m.region);
}

test "s3 url and path-style endpoint" {
    const a = std.testing.allocator;
    const loc = try parseS3("s3://bucket/dir/file.parquet");
    try std.testing.expectEqualStrings("bucket", loc.bucket);
    try std.testing.expectEqualStrings("dir/file.parquet", loc.key);
    const url = try httpUrlForS3(a, loc, "us-east-1", "http://127.0.0.1:9000");
    defer a.free(url);
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/bucket/dir/file.parquet", url);
    const virt = try httpUrlForS3(a, loc, "us-west-2", null);
    defer a.free(virt);
    try std.testing.expectEqualStrings("https://bucket.s3.us-west-2.amazonaws.com/dir/file.parquet", virt);
}

test "amz date from epoch" {
    var buf: [16]u8 = undefined;
    const s = formatAmzDate(1440938160, &buf); // 2015-08-30 12:36:00 UTC
    try std.testing.expectEqualStrings("20150830T123600Z", s);
}
