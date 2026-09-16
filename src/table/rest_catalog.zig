//! Iceberg REST Catalog, read-only.
//!
//! `GET /v1/config` then `GET /v1/{prefix}/namespaces/{ns}/tables/{table}`.
//! After `loadTable`, the existing Iceberg reader scans metadata + manifests.
//! Auth: none, bearer, OAuth2 client_credentials, catalog SigV4.

const std = @import("std");
const aws = @import("../kernel/aws.zig");
const iceberg = @import("iceberg.zig");
const vfs = @import("../vfs/source.zig");
const Transport = vfs.Transport;
const FileSource = vfs.FileSource;

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Auth = enum { auto, none, bearer, oauth2, sigv4 };

pub const Options = struct {
    endpoint: []const u8,
    warehouse: []const u8 = "",
    token: ?[]const u8 = null,
    extra_headers: []const Header = &.{},
    oauth_client_id: ?[]const u8 = null,
    oauth_client_secret: ?[]const u8 = null,
    oauth_server: ?[]const u8 = null,
    oauth_scope: ?[]const u8 = null,
    auth: Auth = .auto,
    sigv4_service: []const u8 = "glue",
    default_namespace: []const u8 = "default",
    default_table: ?[]const u8 = null,
};

pub const Config = struct {
    prefix: []const u8 = "",
    token: ?[]const u8 = null,
    s3_access_key_id: ?[]const u8 = null,
    s3_secret_access_key: ?[]const u8 = null,
    s3_session_token: ?[]const u8 = null,
    s3_region: ?[]const u8 = null,
    s3_endpoint: ?[]const u8 = null,
    gcs_token: ?[]const u8 = null,
    gcs_project_id: ?[]const u8 = null,
};

pub const LoadedTable = struct {
    metadata_location: []const u8,
    metadata: iceberg.TableMetadata,
    config: Config,
};

pub const Client = struct {
    endpoint: []const u8,
    warehouse: []const u8,
    prefix: []const u8,
    token: ?[]const u8,
    extra_headers: []const Header,
    auth: Auth,
    sigv4_service: []const u8,
    sigv4_creds: ?aws.Credentials,
    config: Config,
    default_namespace: []const u8,

    pub fn connect(allocator: std.mem.Allocator, t: Transport, opts: Options) !Client {
        const o = resolveOptions(opts);
        const endpoint = try allocator.dupe(u8, std.mem.trimEnd(u8, o.endpoint, "/"));
        const warehouse = try allocator.dupe(u8, o.warehouse);
        const extra = try dupeHeaders(allocator, o.extra_headers);
        var auth = o.auth;
        const token: ?[]const u8 = if (o.token) |tok| try allocator.dupe(u8, tok) else null;
        const oauth_id = o.oauth_client_id;
        const oauth_secret = o.oauth_client_secret;
        const oauth_server = o.oauth_server;
        const oauth_scope = o.oauth_scope;

        if (auth == .auto) {
            if (token != null) {
                auth = .bearer;
            } else if (oauth_id != null) {
                auth = .oauth2;
            } else {
                auth = .none;
            }
        }

        var sigv4_creds: ?aws.Credentials = null;
        if (auth == .sigv4) {
            const raw = try aws.loadCredentials(t.allocator, t.io);
            defer {
                t.allocator.free(raw.access_key);
                t.allocator.free(raw.secret_key);
                if (raw.session_token) |tok| t.allocator.free(tok);
                t.allocator.free(raw.region);
            }
            sigv4_creds = try copyCreds(allocator, raw);
        }

        var client = Client{
            .endpoint = endpoint,
            .warehouse = warehouse,
            .prefix = "",
            .token = token,
            .extra_headers = extra,
            .auth = auth,
            .sigv4_service = try allocator.dupe(u8, o.sigv4_service),
            .sigv4_creds = sigv4_creds,
            .config = .{},
            .default_namespace = try allocator.dupe(u8, o.default_namespace),
        };

        if (auth == .oauth2) {
            const access = try client.fetchOauthToken(allocator, t, oauth_id, oauth_secret, oauth_server, oauth_scope);
            client.token = access;
            client.auth = .bearer;
        }

        const cfg_body = try client.getJson(allocator, t, try client.configUrl(allocator));
        const cfg = try parseConfigObject(allocator, cfg_body);
        client.config = cfg;
        client.prefix = cfg.prefix;
        if (cfg.token) |tok| client.token = tok;
        return client;
    }

    pub fn loadTable(
        self: *Client,
        allocator: std.mem.Allocator,
        t: Transport,
        namespace: []const u8,
        name: []const u8,
    ) !LoadedTable {
        const url = try self.tableUrl(allocator, namespace, name);
        const body = try self.getJson(allocator, t, url);
        return try parseLoadTable(allocator, t, body);
    }

    fn configUrl(self: *Client, allocator: std.mem.Allocator) ![]u8 {
        if (self.warehouse.len == 0)
            return std.fmt.allocPrint(allocator, "{s}/v1/config", .{self.endpoint});
        const enc = try aws.uriEncode(allocator, self.warehouse, true);
        defer allocator.free(enc);
        return std.fmt.allocPrint(allocator, "{s}/v1/config?warehouse={s}", .{ self.endpoint, enc });
    }

    fn tableUrl(self: *Client, allocator: std.mem.Allocator, namespace: []const u8, name: []const u8) ![]u8 {
        const ns = try encodeNamespace(allocator, namespace);
        defer allocator.free(ns);
        const tbl = try aws.uriEncode(allocator, name, true);
        defer allocator.free(tbl);
        if (self.prefix.len == 0) {
            return std.fmt.allocPrint(allocator, "{s}/v1/namespaces/{s}/tables/{s}", .{
                self.endpoint, ns, tbl,
            });
        }
        const prefix = try aws.uriEncode(allocator, self.prefix, false);
        defer allocator.free(prefix);
        return std.fmt.allocPrint(allocator, "{s}/v1/{s}/namespaces/{s}/tables/{s}", .{
            self.endpoint, prefix, ns, tbl,
        });
    }

    fn fetchOauthToken(
        self: *Client,
        allocator: std.mem.Allocator,
        t: Transport,
        client_id: ?[]const u8,
        client_secret: ?[]const u8,
        server: ?[]const u8,
        scope: ?[]const u8,
    ) ![]const u8 {
        const id = client_id orelse return error.OauthFailed;
        const secret = client_secret orelse return error.OauthFailed;
        const url = if (server) |s|
            try allocator.dupe(u8, std.mem.trimEnd(u8, s, "/"))
        else
            try std.fmt.allocPrint(allocator, "{s}/v1/oauth/tokens", .{self.endpoint});

        const id_e = try aws.uriEncode(allocator, id, true);
        defer allocator.free(id_e);
        const secret_e = try aws.uriEncode(allocator, secret, true);
        defer allocator.free(secret_e);
        var form: std.ArrayList(u8) = .empty;
        try form.appendSlice(allocator, "grant_type=client_credentials&client_id=");
        try form.appendSlice(allocator, id_e);
        try form.appendSlice(allocator, "&client_secret=");
        try form.appendSlice(allocator, secret_e);
        if (scope) |sc| {
            if (sc.len > 0) {
                const sc_e = try aws.uriEncode(allocator, sc, true);
                defer allocator.free(sc_e);
                try form.appendSlice(allocator, "&scope=");
                try form.appendSlice(allocator, sc_e);
            }
        }

        const headers = [_]std.http.Header{
            .{ .name = "content-type", .value = "application/x-www-form-urlencoded" },
            .{ .name = "accept", .value = "application/json" },
        };
        const body = try fetch(allocator, t, .POST, url, &headers, form.items);
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .allocate = .alloc_always });
        const root = switch (parsed.value) {
            .object => |o| o,
            else => return error.OauthFailed,
        };
        const access = jsonString(root, "access_token") orelse return error.OauthFailed;
        return try allocator.dupe(u8, access);
    }

    fn getJson(self: *Client, allocator: std.mem.Allocator, t: Transport, url: []const u8) ![]u8 {
        var hdrs: [16]std.http.Header = undefined;
        var n: usize = 0;
        var auth_owned: ?[]u8 = null;
        defer if (auth_owned) |p| allocator.free(p);
        var date_buf: [16]u8 = undefined;
        var sig_owned: ?[]u8 = null;
        defer if (sig_owned) |p| allocator.free(p);

        hdrs[n] = .{ .name = "accept", .value = "application/json" };
        n += 1;
        hdrs[n] = .{ .name = "X-Iceberg-Access-Delegation", .value = "vended-credentials" };
        n += 1;
        for (self.extra_headers) |h| {
            hdrs[n] = .{ .name = h.name, .value = h.value };
            n += 1;
        }

        if (self.auth == .bearer) {
            if (self.token) |tok| {
                auth_owned = try std.fmt.allocPrint(allocator, "Bearer {s}", .{tok});
                hdrs[n] = .{ .name = "authorization", .value = auth_owned.? };
                n += 1;
            }
        } else if (self.auth == .sigv4) {
            const creds = self.sigv4_creds orelse return error.AwsCredentialsMissing;
            const amz_date = aws.nowAmzDate(t.io, &date_buf);
            const unsigned_extras = hdrs[0..n];
            const auth = try aws.authorization(allocator, .{
                .method = "GET",
                .host = urlHost(url),
                .path = urlPath(url),
                .query = urlQuery(url),
                .extra_headers = unsigned_extras,
                .payload_hash = aws.empty_payload_hash,
                .amz_date = amz_date,
                .region = creds.region,
                .service = self.sigv4_service,
                .creds = creds,
            });
            sig_owned = auth;
            hdrs[n] = .{ .name = "x-amz-date", .value = amz_date };
            n += 1;
            hdrs[n] = .{ .name = "x-amz-content-sha256", .value = aws.empty_payload_hash };
            n += 1;
            hdrs[n] = .{ .name = "authorization", .value = auth };
            n += 1;
            if (creds.session_token) |tok| {
                hdrs[n] = .{ .name = "x-amz-security-token", .value = tok };
                n += 1;
            }
        }

        return fetch(allocator, t, .GET, url, hdrs[0..n], null);
    }
};

pub fn resolveOptions(opts: Options) Options {
    var o = opts;
    if (o.endpoint.len == 0) o.endpoint = envSlice("ICEBERG_REST_URI") orelse o.endpoint;
    if (o.warehouse.len == 0) o.warehouse = envSlice("ICEBERG_WAREHOUSE") orelse o.warehouse;
    if (o.token == null) o.token = envSlice("ICEBERG_TOKEN");
    if (o.oauth_client_id == null) o.oauth_client_id = envSlice("ICEBERG_CLIENT_ID");
    if (o.oauth_client_secret == null) o.oauth_client_secret = envSlice("ICEBERG_CLIENT_SECRET");
    if (o.oauth_server == null) o.oauth_server = envSlice("ICEBERG_OAUTH2_SERVER");
    if (o.oauth_scope == null) o.oauth_scope = envSlice("ICEBERG_OAUTH2_SCOPE");
    return o;
}

fn envSlice(key: [:0]const u8) ?[]const u8 {
    if (comptime @import("builtin").cpu.arch == .wasm32) return null;
    const p = std.c.getenv(key) orelse return null;
    const s = std.mem.span(p);
    return if (s.len == 0) null else s;
}

pub fn parseHeaderLine(line: []const u8) !Header {
    const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.InvalidHeader;
    const name = std.mem.trim(u8, line[0..colon], " \t");
    const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
    if (name.len == 0) return error.InvalidHeader;
    return .{ .name = name, .value = value };
}

pub fn splitIdent(from: []const u8, default_ns: []const u8) struct { ns: []const u8, name: []const u8 } {
    if (std.mem.lastIndexOfScalar(u8, from, '.')) |i| {
        return .{ .ns = from[0..i], .name = from[i + 1 ..] };
    }
    return .{ .ns = default_ns, .name = from };
}

pub fn encodeNamespace(allocator: std.mem.Allocator, ns: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var it = std.mem.splitScalar(u8, ns, '.');
    var first = true;
    while (it.next()) |part| {
        if (!first) try out.appendSlice(allocator, "%1F");
        first = false;
        const enc = try aws.uriEncode(allocator, part, true);
        defer allocator.free(enc);
        try out.appendSlice(allocator, enc);
    }
    return out.toOwnedSlice(allocator);
}

pub fn fileBase(loaded: LoadedTable) []const u8 {
    const loc = iceberg.stripFileUrl(loaded.metadata.location);
    if (aws.isRemote(loc) or std.fs.path.isAbsolute(loc)) return loc;
    if (loaded.metadata_location.len == 0) return loc;
    return iceberg.tableDirFromMetaPath(loaded.metadata_location);
}

fn parseLoadTable(allocator: std.mem.Allocator, t: Transport, body: []const u8) !LoadedTable {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .allocate = .alloc_always });
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return error.InvalidMetadata,
    };

    const meta_loc = if (jsonString(root, "metadata-location")) |s|
        try allocator.dupe(u8, s)
    else
        try allocator.dupe(u8, "");

    const metadata = if (root.get("metadata")) |mv| blk: {
        const json = std.json.Stringify.valueAlloc(allocator, mv, .{}) catch return error.InvalidMetadata;
        break :blk try iceberg.parseMetadata(allocator, json);
    } else if (meta_loc.len > 0) blk: {
        var src = try FileSource.openLocation(t, iceberg.stripFileUrl(meta_loc));
        defer src.close();
        const json = try src.readAll(allocator);
        break :blk try iceberg.parseMetadata(allocator, json);
    } else return error.InvalidMetadata;

    var cfg: Config = .{};
    if (root.get("config")) |cv| {
        cfg = try parseConfigValue(allocator, cv);
    }

    return .{
        .metadata_location = meta_loc,
        .metadata = metadata,
        .config = cfg,
    };
}

fn parseConfigObject(allocator: std.mem.Allocator, body: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .allocate = .alloc_always });
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return .{},
    };
    var cfg: Config = .{};
    if (root.get("defaults")) |d| cfg = try mergeConfig(allocator, cfg, d);
    if (root.get("overrides")) |o| cfg = try mergeConfig(allocator, cfg, o);
    return cfg;
}

fn parseConfigValue(allocator: std.mem.Allocator, value: std.json.Value) !Config {
    return mergeConfig(allocator, .{}, value);
}

fn mergeConfig(allocator: std.mem.Allocator, base: Config, value: std.json.Value) !Config {
    var cfg = base;
    const obj = switch (value) {
        .object => |o| o,
        else => return cfg,
    };
    var it = obj.iterator();
    while (it.next()) |kv| {
        const s = switch (kv.value_ptr.*) {
            .string => |v| v,
            else => continue,
        };
        const owned = try allocator.dupe(u8, s);
        if (std.mem.eql(u8, kv.key_ptr.*, "prefix")) {
            cfg.prefix = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "token")) {
            cfg.token = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "s3.access-key-id")) {
            cfg.s3_access_key_id = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "s3.secret-access-key")) {
            cfg.s3_secret_access_key = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "s3.session-token")) {
            cfg.s3_session_token = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "s3.region") or std.mem.eql(u8, kv.key_ptr.*, "client.region")) {
            cfg.s3_region = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "s3.endpoint")) {
            cfg.s3_endpoint = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "gcs.oauth2.token")) {
            cfg.gcs_token = owned;
        } else if (std.mem.eql(u8, kv.key_ptr.*, "gcs.project-id")) {
            cfg.gcs_project_id = owned;
        }
    }
    return cfg;
}

fn jsonString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const v = obj.get(key) orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn dupeHeaders(allocator: std.mem.Allocator, src: []const Header) ![]Header {
    const out = try allocator.alloc(Header, src.len);
    for (src, 0..) |h, i| {
        out[i] = .{
            .name = try allocator.dupe(u8, h.name),
            .value = try allocator.dupe(u8, h.value),
        };
    }
    return out;
}

fn copyCreds(allocator: std.mem.Allocator, creds: aws.Credentials) !aws.Credentials {
    return .{
        .access_key = try allocator.dupe(u8, creds.access_key),
        .secret_key = try allocator.dupe(u8, creds.secret_key),
        .session_token = if (creds.session_token) |tok| try allocator.dupe(u8, tok) else null,
        .region = try allocator.dupe(u8, creds.region),
    };
}

fn fetch(
    allocator: std.mem.Allocator,
    t: Transport,
    method: std.http.Method,
    url: []const u8,
    extra_headers: []const std.http.Header,
    payload: ?[]const u8,
) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const result = t.http.fetch(.{
        .location = .{ .url = url },
        .method = method,
        .payload = payload,
        .extra_headers = extra_headers,
        .response_writer = &aw.writer,
        .keep_alive = false,
        .headers = .{ .accept_encoding = .omit },
    }) catch return error.RestCatalogFailed;
    const body = try aw.toOwnedSlice();
    errdefer allocator.free(body);
    return switch (result.status) {
        .ok => body,
        .unauthorized, .forbidden => error.AccessDenied,
        .not_found => error.TableNotFound,
        else => error.RestCatalogFailed,
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

test "encodeNamespace uses unit separator" {
    const ns = try encodeNamespace(std.testing.allocator, "public.nyc");
    defer std.testing.allocator.free(ns);
    try std.testing.expectEqualStrings("public%1Fnyc", ns);
}

test "splitIdent last dot is the table" {
    const a = splitIdent("public.nyc.trips", "default");
    try std.testing.expectEqualStrings("public.nyc", a.ns);
    try std.testing.expectEqualStrings("trips", a.name);
    const b = splitIdent("nation", "default");
    try std.testing.expectEqualStrings("default", b.ns);
    try std.testing.expectEqualStrings("nation", b.name);
}

test "parseHeaderLine" {
    const h = try parseHeaderLine("X-Goog-User-Project: my-billing");
    try std.testing.expectEqualStrings("X-Goog-User-Project", h.name);
    try std.testing.expectEqualStrings("my-billing", h.value);
    try std.testing.expectError(error.InvalidHeader, parseHeaderLine("nope"));
}

test "parseConfig reads vended s3 and gcs keys" {
    const json =
        \\{"s3.access-key-id":"AKIA","s3.secret-access-key":"s","gcs.oauth2.token":"ya29","prefix":"cat"}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const cfg = try parseConfigValue(arena.allocator(), parsed.value);
    try std.testing.expectEqualStrings("AKIA", cfg.s3_access_key_id.?);
    try std.testing.expectEqualStrings("ya29", cfg.gcs_token.?);
    try std.testing.expectEqualStrings("cat", cfg.prefix);
}
