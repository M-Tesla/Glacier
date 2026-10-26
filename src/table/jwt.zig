//! One OIDC/JWT provider. RS256 only. `sub` is the GRANT principal.
//! The shared `glacier serve --token` stays the principal `bearer`.

const std = @import("std");
const json = std.json;

pub const IdentifyError = error{ NotAuthorized, OutOfMemory };

const Key = struct {
    kid: []const u8,
    n: []const u8,
    e: []const u8,
};

pub const Set = struct {
    allocator: std.mem.Allocator,
    issuer: []const u8,
    audience: []const u8,
    keys: []Key,

    pub fn deinit(self: *Set) void {
        for (self.keys) |k| {
            if (k.kid.len > 0) self.allocator.free(k.kid);
            self.allocator.free(k.n);
            self.allocator.free(k.e);
        }
        self.allocator.free(self.keys);
        if (self.issuer.len > 0) self.allocator.free(self.issuer);
        if (self.audience.len > 0) self.allocator.free(self.audience);
    }
};

extern fn glacier_jwt_rs256(
    n: [*]const u8,
    nlen: c_int,
    e: [*]const u8,
    elen: c_int,
    msg: [*]const u8,
    mlen: c_int,
    sig: [*]const u8,
    slen: c_int,
) c_int;

pub fn loadPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8, issuer: []const u8, audience: []const u8) !Set {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var rbuf: [4096]u8 = undefined;
    var reader = file.reader(io, &rbuf);
    const text = try reader.interface.allocRemaining(allocator, .limited(1024 * 1024));
    defer allocator.free(text);
    return parseJwks(allocator, text, issuer, audience);
}

pub fn loadIssuer(allocator: std.mem.Allocator, io: std.Io, issuer: []const u8, audience: []const u8) !Set {
    var client: std.http.Client = .{ .allocator = allocator, .io = io };
    defer client.deinit();
    const trimmed = std.mem.trim(u8, issuer, "/");
    const disc = try std.fmt.allocPrint(allocator, "{s}/.well-known/openid-configuration", .{trimmed});
    defer allocator.free(disc);
    const body = try httpGet(allocator, &client, disc);
    defer allocator.free(body);
    const jwks_uri = jsonString(allocator, body, "jwks_uri") catch return error.NotAuthorized;
    defer allocator.free(jwks_uri);
    const jwks = try httpGet(allocator, &client, jwks_uri);
    defer allocator.free(jwks);
    return parseJwks(allocator, jwks, issuer, audience);
}

pub fn parseJwks(allocator: std.mem.Allocator, text: []const u8, issuer: []const u8, audience: []const u8) !Set {
    const parsed = json.parseFromSlice(json.Value, allocator, text, .{ .allocate = .alloc_always }) catch return error.NotAuthorized;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return error.NotAuthorized,
    };
    const arr = switch (root.get("keys") orelse return error.NotAuthorized) {
        .array => |a| a,
        else => return error.NotAuthorized,
    };
    var keys: std.ArrayList(Key) = .empty;
    errdefer keys.deinit(allocator);
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |o| o,
            else => continue,
        };
        const kty = switch (obj.get("kty") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        if (!std.mem.eql(u8, kty, "RSA")) continue;
        const n_s = switch (obj.get("n") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const e_s = switch (obj.get("e") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const n = b64urlDecode(allocator, n_s) catch continue;
        const e = b64urlDecode(allocator, e_s) catch {
            allocator.free(n);
            continue;
        };
        const kid = switch (obj.get("kid") orelse .null) {
            .string => |s| allocator.dupe(u8, s) catch "",
            else => "",
        };
        keys.append(allocator, .{ .kid = kid, .n = n, .e = e }) catch continue;
    }
    if (keys.items.len == 0) return error.NotAuthorized;
    return .{
        .allocator = allocator,
        .issuer = if (issuer.len == 0) "" else try allocator.dupe(u8, issuer),
        .audience = if (audience.len == 0) "" else try allocator.dupe(u8, audience),
        .keys = try keys.toOwnedSlice(allocator),
    };
}

/// Shared secret matches the principal `bearer`. Otherwise the token is a JWT
/// and the returned slice is `sub`, owned by `allocator`.
pub fn identify(
    allocator: std.mem.Allocator,
    shared: ?[]const u8,
    keys: ?*const Set,
    raw: []const u8,
    now: i64,
) IdentifyError![]u8 {
    if (shared) |tok| {
        if (tokenMatch(raw, tok)) return allocator.dupe(u8, "bearer");
    }
    const set = keys orelse return error.NotAuthorized;
    const sub = subject(allocator, set, raw, now) catch return error.NotAuthorized;
    return sub;
}

fn subject(allocator: std.mem.Allocator, set: *const Set, token: []const u8, now: i64) ![]u8 {
    const dot1 = std.mem.indexOfScalar(u8, token, '.') orelse return error.NotAuthorized;
    const rest = token[dot1 + 1 ..];
    const dot2 = std.mem.indexOfScalar(u8, rest, '.') orelse return error.NotAuthorized;
    const header_b64 = token[0..dot1];
    const payload_b64 = rest[0..dot2];
    const sig_b64 = rest[dot2 + 1 ..];
    const signing = token[0 .. dot1 + 1 + dot2];
    const header = b64urlDecode(allocator, header_b64) catch return error.NotAuthorized;
    defer allocator.free(header);
    const payload = b64urlDecode(allocator, payload_b64) catch return error.NotAuthorized;
    defer allocator.free(payload);
    const sig = b64urlDecode(allocator, sig_b64) catch return error.NotAuthorized;
    defer allocator.free(sig);
    const alg = jsonString(allocator, header, "alg") catch return error.NotAuthorized;
    defer allocator.free(alg);
    if (!std.mem.eql(u8, alg, "RS256")) return error.NotAuthorized;
    const kid = jsonString(allocator, header, "kid") catch "";
    defer if (kid.len > 0) allocator.free(kid);
    const key = pick(set, kid) orelse return error.NotAuthorized;
    if (glacier_jwt_rs256(key.n.ptr, @intCast(key.n.len), key.e.ptr, @intCast(key.e.len), signing.ptr, @intCast(signing.len), sig.ptr, @intCast(sig.len)) != 1)
        return error.NotAuthorized;
    if (set.issuer.len > 0) {
        const iss = jsonString(allocator, payload, "iss") catch return error.NotAuthorized;
        defer allocator.free(iss);
        if (!std.mem.eql(u8, iss, set.issuer)) return error.NotAuthorized;
    }
    if (set.audience.len > 0) {
        if (!audMatches(allocator, payload, set.audience)) return error.NotAuthorized;
    }
    const exp = jsonInt(payload, "exp") orelse return error.NotAuthorized;
    if (exp <= now) return error.NotAuthorized;
    if (jsonInt(payload, "nbf")) |nbf| {
        if (nbf > now) return error.NotAuthorized;
    }
    const sub = jsonString(allocator, payload, "sub") catch return error.NotAuthorized;
    if (sub.len == 0 or std.ascii.eqlIgnoreCase(sub, "bearer")) {
        allocator.free(sub);
        return error.NotAuthorized;
    }
    return sub;
}

fn pick(set: *const Set, kid: []const u8) ?Key {
    if (kid.len == 0) return set.keys[0];
    for (set.keys) |k| {
        if (std.mem.eql(u8, k.kid, kid)) return k;
    }
    return if (set.keys.len == 1) set.keys[0] else null;
}

fn audMatches(allocator: std.mem.Allocator, payload: []const u8, audience: []const u8) bool {
    const parsed = json.parseFromSlice(json.Value, allocator, payload, .{ .allocate = .alloc_always }) catch return false;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return false,
    };
    const aud = root.get("aud") orelse return false;
    switch (aud) {
        .string => |s| return std.mem.eql(u8, s, audience),
        .array => |arr| {
            for (arr.items) |item| {
                if (item == .string and std.mem.eql(u8, item.string, audience)) return true;
            }
            return false;
        },
        else => return false,
    }
}

fn jsonString(allocator: std.mem.Allocator, text: []const u8, key: []const u8) ![]u8 {
    const parsed = json.parseFromSlice(json.Value, allocator, text, .{ .allocate = .alloc_always }) catch return error.NotAuthorized;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return error.NotAuthorized,
    };
    const v = switch (root.get(key) orelse return error.NotAuthorized) {
        .string => |s| s,
        else => return error.NotAuthorized,
    };
    return allocator.dupe(u8, v);
}

fn jsonInt(text: []const u8, key: []const u8) ?i64 {
    var buf: [256]u8 = undefined;
    const needle = std.fmt.bufPrint(&buf, "\"{s}\":", .{key}) catch return null;
    const at = std.mem.indexOf(u8, text, needle) orelse return null;
    var i = at + needle.len;
    while (i < text.len and (text[i] == ' ' or text[i] == '\n')) i += 1;
    var j = i;
    if (j < text.len and text[j] == '-') j += 1;
    const start = j;
    while (j < text.len and text[j] >= '0' and text[j] <= '9') j += 1;
    if (j == start) return null;
    return std.fmt.parseInt(i64, text[i..j], 10) catch null;
}

fn b64urlDecode(allocator: std.mem.Allocator, src: []const u8) ![]u8 {
    const dec = std.base64.url_safe_no_pad.Decoder;
    const n = dec.calcSizeForSlice(src) catch return error.NotAuthorized;
    const buf = try allocator.alloc(u8, n);
    dec.decode(buf, src) catch {
        allocator.free(buf);
        return error.NotAuthorized;
    };
    return buf;
}

fn tokenMatch(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var diff: u8 = 0;
    for (a, b) |x, y| diff |= x ^ y;
    return diff == 0;
}

fn httpGet(allocator: std.mem.Allocator, client: *std.http.Client, url: []const u8) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer aw.deinit();
    const result = client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .response_writer = &aw.writer,
        .keep_alive = false,
        .headers = .{ .accept_encoding = .omit },
    }) catch return error.NotAuthorized;
    if (result.status != .ok) return error.NotAuthorized;
    return aw.toOwnedSlice();
}

pub fn b64urlEncode(allocator: std.mem.Allocator, src: []const u8) ![]u8 {
    const enc = std.base64.url_safe_no_pad.Encoder;
    const n = enc.calcSize(src.len);
    const buf = try allocator.alloc(u8, n);
    _ = enc.encode(buf, src);
    return buf;
}

/// Empty key set for the public-bind check. Do not deinit.
pub fn probeSet(allocator: std.mem.Allocator) Set {
    return .{
        .allocator = allocator,
        .issuer = "",
        .audience = "",
        .keys = &.{},
    };
}

test "RS256 sub is the principal" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const ran = std.process.run(gpa, io, .{
        .argv = &.{ "python3", "-c",
            \\import base64, json, pathlib, subprocess
            \\key = "/tmp/glacier-jwt-unit.pem"
            \\jwks_path = "/tmp/glacier-jwt-unit.jwks"
            \\subprocess.check_call(["openssl", "genrsa", "-out", key, "2048"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            \\mod = subprocess.check_output(["openssl", "rsa", "-in", key, "-noout", "-modulus"], text=True)
            \\n = bytes.fromhex(mod.strip().split("=", 1)[1])
            \\def b64(b):
            \\    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()
            \\jwks = {"keys": [{"kty": "RSA", "alg": "RS256", "kid": "k1", "n": b64(n), "e": "AQAB"}]}
            \\pathlib.Path(jwks_path).write_text(json.dumps(jwks))
            \\header = b64(json.dumps({"alg": "RS256", "typ": "JWT", "kid": "k1"}, separators=(",", ":")).encode())
            \\payload = b64(json.dumps({"sub": "alice", "iss": "https://idp.example", "aud": "glacier", "exp": 2000000000}, separators=(",", ":")).encode())
            \\signing = header + "." + payload
            \\msg = "/tmp/glacier-jwt-unit.msg"
            \\sigp = "/tmp/glacier-jwt-unit.sig"
            \\pathlib.Path(msg).write_bytes(signing.encode())
            \\subprocess.check_call(["openssl", "dgst", "-sha256", "-sign", key, "-out", sigp, msg])
            \\print(signing + "." + b64(pathlib.Path(sigp).read_bytes()))
            \\payload_b = b64(json.dumps({"sub": "bearer", "iss": "https://idp.example", "aud": "glacier", "exp": 2000000000}, separators=(",", ":")).encode())
            \\signing_b = header + "." + payload_b
            \\pathlib.Path(msg).write_bytes(signing_b.encode())
            \\subprocess.check_call(["openssl", "dgst", "-sha256", "-sign", key, "-out", sigp, msg])
            \\print(signing_b + "." + b64(pathlib.Path(sigp).read_bytes()))
        },
    }) catch return error.SkipZigTest;
    defer gpa.free(ran.stdout);
    defer gpa.free(ran.stderr);
    switch (ran.term) {
        .exited => |code| if (code != 0) return error.SkipZigTest,
        else => return error.SkipZigTest,
    }
    const trimmed = std.mem.trim(u8, ran.stdout, " \r\n");
    const nl = std.mem.indexOfScalar(u8, trimmed, '\n') orelse return error.SkipZigTest;
    const token = std.mem.trim(u8, trimmed[0..nl], " \r");
    const bearer_tok = std.mem.trim(u8, trimmed[nl + 1 ..], " \r");
    var set = loadPath(gpa, io, "/tmp/glacier-jwt-unit.jwks", "https://idp.example", "glacier") catch return error.SkipZigTest;
    defer set.deinit();
    const alice = try identify(gpa, null, &set, token, 1_700_000_000);
    defer gpa.free(alice);
    try std.testing.expectEqualStrings("alice", alice);
    try std.testing.expectError(error.NotAuthorized, identify(gpa, null, &set, bearer_tok, 1_700_000_000));
    try std.testing.expectError(error.NotAuthorized, identify(gpa, null, &set, token, 2_000_000_001));
    var bad = try loadPath(gpa, io, "/tmp/glacier-jwt-unit.jwks", "https://other.example", "glacier");
    defer bad.deinit();
    try std.testing.expectError(error.NotAuthorized, identify(gpa, null, &bad, token, 1_700_000_000));
    const shared = try identify(gpa, "sekret", &set, "sekret", 1_700_000_000);
    defer gpa.free(shared);
    try std.testing.expectEqualStrings("bearer", shared);
    const header = try b64urlEncode(gpa, "{\"alg\":\"none\"}");
    defer gpa.free(header);
    const payload = try b64urlEncode(gpa, "{\"sub\":\"alice\",\"exp\":2000000000}");
    defer gpa.free(payload);
    const none = try std.fmt.allocPrint(gpa, "{s}.{s}.", .{ header, payload });
    defer gpa.free(none);
    try std.testing.expectError(error.NotAuthorized, identify(gpa, null, &set, none, 1_700_000_000));
    var flipped = try gpa.dupe(u8, token);
    defer gpa.free(flipped);
    flipped[flipped.len - 1] = if (flipped[flipped.len - 1] == 'a') 'b' else 'a';
    try std.testing.expectError(error.NotAuthorized, identify(gpa, null, &set, flipped, 1_700_000_000));
}
