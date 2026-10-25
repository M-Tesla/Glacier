//! Native Iceberg REST Catalog HTTP server over the Glacier warehouse registry.

const std = @import("std");
const iceberg = @import("iceberg.zig");
const glacier_catalog = @import("glacier_catalog.zig");
const grants = @import("grants.zig");
const vfs = @import("../vfs/source.zig");

const net = std.Io.net;
const http = std.http;
const json = std.json;

const json_headers = [_]http.Header{
    .{ .name = "Content-Type", .value = "application/json" },
};

pub const Handle = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []u8,
    server: net.Server,
    tls: ?*anyopaque = null,
    /// When set, every REST call needs `Authorization: Bearer <token>`.
    token: ?[]u8 = null,
    closed: bool = false,

    pub fn port(self: Handle) u16 {
        return self.server.socket.address.getPort();
    }

    pub fn stop(self: *Handle) void {
        if (self.closed) return;
        self.server.deinit(self.io);
        self.closed = true;
    }

    pub fn deinit(self: *Handle) void {
        self.stop();
        if (self.tls) |ctx| glacier_tls_free(ctx);
        if (self.token) |tok| self.gpa.free(tok);
        self.gpa.free(self.warehouse);
    }

    pub fn setBearer(self: *Handle, token: []const u8) !void {
        if (self.token) |old| self.gpa.free(old);
        self.token = try self.gpa.dupe(u8, token);
    }
};

extern fn glacier_tls_server(cert: [*:0]const u8, key: [*:0]const u8) ?*anyopaque;
extern fn glacier_tls_free(box: ?*anyopaque) void;
extern fn glacier_tls_accept(box: *anyopaque, fd: c_int) ?*anyopaque;
extern fn glacier_tls_read(ssl: *anyopaque, buf: [*]u8, n: c_int) c_int;
extern fn glacier_tls_write(ssl: *anyopaque, buf: [*]const u8, n: c_int) c_int;
extern fn glacier_tls_close(ssl: ?*anyopaque) void;

pub fn parseListen(text: []const u8) !net.IpAddress {
    return parseListenPort(text, 8181);
}

pub fn parseListenPort(text: []const u8, default_port: u16) !net.IpAddress {
    return parseListenPortTls(text, default_port, false);
}

pub fn parseListenPortTls(text: []const u8, default_port: u16, allow_public: bool) !net.IpAddress {
    var buf: [128]u8 = undefined;
    const spec = spec: {
        if (std.ascii.startsWithIgnoreCase(text, "localhost")) {
            if (text.len == 9) {
                break :spec std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{default_port}) catch
                    return error.InvalidAddress;
            }
            if (text[9] == ':') {
                break :spec std.fmt.bufPrint(&buf, "127.0.0.1{s}", .{text[9..]}) catch
                    return error.InvalidAddress;
            }
            return error.InvalidAddress;
        }
        if (std.mem.indexOfScalar(u8, text, ':') == null) {
            break :spec std.fmt.bufPrint(&buf, "{s}:{d}", .{ text, default_port }) catch
                return error.InvalidAddress;
        }
        break :spec text;
    };
    const addr = net.IpAddress.parseLiteral(spec) catch return error.InvalidAddress;
    if (!isLoopback(addr) and !allow_public) return error.TlsRequired;
    return addr;
}

fn isLoopback(addr: net.IpAddress) bool {
    return switch (addr) {
        .ip4 => |a| a.bytes[0] == 127,
        .ip6 => |a| std.mem.eql(u8, &a.bytes, &loopback6),
    };
}

const loopback6 = [_]u8{0} ** 15 ++ [_]u8{1};

pub fn bind(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
) !Handle {
    return bindTls(gpa, io, warehouse, listen, null, null);
}

pub fn bindTls(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
    cert: ?[]const u8,
    key: ?[]const u8,
) !Handle {
    if ((cert == null) != (key == null)) return error.TlsRequired;
    var addr = try parseListenPortTls(listen, 8181, cert != null);
    try glacier_catalog.ensure(gpa, io, warehouse);
    var tls: ?*anyopaque = null;
    if (cert) |c| {
        const c_cert = try gpa.dupeZ(u8, c);
        defer gpa.free(c_cert);
        const c_key = try gpa.dupeZ(u8, key.?);
        defer gpa.free(c_key);
        tls = glacier_tls_server(c_cert.ptr, c_key.ptr) orelse return error.TlsRequired;
    }
    var server = try addr.listen(io, .{ .reuse_address = true });
    errdefer server.deinit(io);
    errdefer if (tls) |ctx| glacier_tls_free(ctx);
    return .{
        .gpa = gpa,
        .io = io,
        .warehouse = try gpa.dupe(u8, warehouse),
        .server = server,
        .tls = tls,
    };
}

pub fn run(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
    out: *std.Io.Writer,
) !void {
    var h = try bind(gpa, io, warehouse, listen);
    defer h.deinit();
    try out.print("Iceberg REST  http://{f}\nwarehouse     {s}\n", .{
        h.server.socket.address,
        h.warehouse,
    });
    try out.flush();
    try serveLoop(&h);
}

pub fn serveLoop(self: *Handle) std.Io.Cancelable!void {
    while (true) {
        const stream = self.server.accept(self.io) catch |err| switch (err) {
            error.Canceled, error.SocketNotListening => return,
            else => continue,
        };
        serveConn(self, stream);
    }
}

fn serveConn(self: *Handle, stream: net.Stream) void {
    if (self.tls) |ctx| {
        serveConnTls(self, stream, ctx);
        return;
    }
    const io = self.io;
    defer {
        var copy = stream;
        copy.close(io);
    }
    var send_buffer: [4096]u8 = undefined;
    var recv_buffer: [4096]u8 = undefined;
    var connection_reader = stream.reader(io, &recv_buffer);
    var connection_writer = stream.writer(io, &send_buffer);
    var server: http.Server = .init(&connection_reader.interface, &connection_writer.interface);
    while (true) {
        var request = server.receiveHead() catch return;
        handleRequest(self, &request) catch {
            request.respond(
                "{\"error\":{\"message\":\"internal\",\"type\":\"InternalServerError\"}}",
                .{
                    .status = .internal_server_error,
                    .keep_alive = false,
                    .extra_headers = &json_headers,
                },
            ) catch {};
            return;
        };
        return;
    }
}

fn httpReady(buf: []const u8) bool {
    const sep = std.mem.indexOf(u8, buf, "\r\n\r\n") orelse return false;
    const head = buf[0..sep];
    var len: usize = 0;
    var it = std.mem.splitScalar(u8, head, '\n');
    while (it.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, "\r");
        if (std.ascii.startsWithIgnoreCase(line, "content-length:")) {
            const n = std.mem.trim(u8, line["content-length:".len..], " \t");
            len = std.fmt.parseInt(usize, n, 10) catch return false;
        }
    }
    return buf.len >= sep + 4 + len;
}

fn serveConnTls(self: *Handle, stream: net.Stream, ctx: *anyopaque) void {
    const io = self.io;
    defer {
        var copy = stream;
        copy.close(io);
    }
    const ssl = glacier_tls_accept(ctx, stream.socket.handle) orelse return;
    defer glacier_tls_close(ssl);
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(self.gpa);
    var tmp: [4096]u8 = undefined;
    while (req.items.len < 1024 * 1024) {
        const n = glacier_tls_read(ssl, &tmp, tmp.len);
        if (n <= 0) break;
        req.appendSlice(self.gpa, tmp[0..@intCast(n)]) catch return;
        if (httpReady(req.items)) break;
    }
    if (!httpReady(req.items)) return;
    var reader = std.Io.Reader.fixed(req.items);
    var aw: std.Io.Writer.Allocating = .init(self.gpa);
    defer aw.deinit();
    var server: http.Server = .init(&reader, &aw.writer);
    var request = server.receiveHead() catch return;
    handleRequest(self, &request) catch {
        request.respond(
            "{\"error\":{\"message\":\"internal\",\"type\":\"InternalServerError\"}}",
            .{
                .status = .internal_server_error,
                .keep_alive = false,
                .extra_headers = &json_headers,
            },
        ) catch {};
    };
    const out = aw.written();
    var off: usize = 0;
    while (off < out.len) {
        const n = glacier_tls_write(ssl, out[off..].ptr, @intCast(out.len - off));
        if (n <= 0) return;
        off += @intCast(n);
    }
}

fn handleRequest(self: *Handle, request: *http.Server.Request) !void {
    var arena = std.heap.ArenaAllocator.init(self.gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const target = request.head.target;
    const q = std.mem.indexOfScalar(u8, target, '?') orelse target.len;
    const path = std.mem.trimEnd(u8, target[0..q], "/");
    const method = request.head.method;
    if (!bearerOk(self, request)) {
        return sendErrA(a, request, .unauthorized, "NotAuthorizedException", "missing or invalid bearer token");
    }
    const body = try readBody(request, a);

    var parts: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, path, '/');
    while (it.next()) |p| try parts.append(a, p);
    const segs = parts.items;

    if (method == .GET and segs.len == 3 and eql(segs[1], "v1") and eql(segs[2], "config")) {
        return sendJson(request, .ok, try configJson(a, self.warehouse));
    }
    if (method == .GET and segs.len == 3 and eql(segs[1], "v1") and eql(segs[2], "namespaces")) {
        return sendJson(request, .ok, try listNamespacesJson(self, a));
    }
    if (method == .POST and segs.len == 3 and eql(segs[1], "v1") and eql(segs[2], "namespaces")) {
        return postNamespace(self, a, request, body);
    }
    if (segs.len == 5 and eql(segs[1], "v1") and eql(segs[2], "namespaces") and eql(segs[4], "tables")) {
        const ns = try decodeNs(a, segs[3]);
        if (method == .GET) return sendJson(request, .ok, try listTablesJson(self, a, ns));
        if (method == .POST) return postTable(self, a, request, ns, body);
    }
    if (segs.len == 6 and eql(segs[1], "v1") and eql(segs[2], "namespaces") and eql(segs[4], "tables")) {
        const ns = try decodeNs(a, segs[3]);
        const name = try decodeNs(a, segs[5]);
        if (method == .GET) return getTable(self, a, request, ns, name);
        if (method == .POST) return commitTable(self, a, request, ns, name, body);
        if (method == .DELETE) return deleteTable(self, a, request, ns, name);
    }
    return sendErrA(a, request, .not_found, "NoSuchTableException", "not found");
}

fn bearerOk(self: *const Handle, request: *http.Server.Request) bool {
    const expect = self.token orelse return true;
    var it = request.iterateHeaders();
    while (it.next()) |h| {
        if (!std.ascii.eqlIgnoreCase(h.name, "authorization")) continue;
        const prefix = "Bearer ";
        if (h.value.len <= prefix.len) return false;
        if (!std.ascii.eqlIgnoreCase(h.value[0..prefix.len], prefix)) return false;
        return tokenMatch(h.value[prefix.len..], expect);
    }
    return false;
}

fn tokenMatch(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var diff: u8 = 0;
    for (a, b) |x, y| diff |= x ^ y;
    return diff == 0;
}

fn readBody(request: *http.Server.Request, allocator: std.mem.Allocator) ![]const u8 {
    if (!request.head.method.requestHasBody()) return "{}";
    var buf: [4096]u8 = undefined;
    const reader = try request.readerExpectContinue(&buf);
    const raw = reader.allocRemaining(allocator, .limited(8 * 1024 * 1024)) catch return "{}";
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    return if (trimmed.len == 0) "{}" else trimmed;
}

fn sendJson(request: *http.Server.Request, status: http.Status, body: []const u8) !void {
    try request.respond(body, .{
        .status = status,
        .keep_alive = false,
        .extra_headers = &json_headers,
    });
}

fn sendErrA(
    allocator: std.mem.Allocator,
    request: *http.Server.Request,
    status: http.Status,
    typ: []const u8,
    msg: []const u8,
) !void {
    const text = try std.fmt.allocPrint(allocator, "{{\"error\":{{\"message\":{f},\"type\":{f}}}}}", .{
        json.fmt(msg, .{}),
        json.fmt(typ, .{}),
    });
    try sendJson(request, status, text);
}

fn configJson(allocator: std.mem.Allocator, warehouse: []const u8) ![]u8 {
    const Cfg = struct {
        defaults: struct { warehouse: []const u8 },
        overrides: struct {},
    };
    return json.Stringify.valueAlloc(allocator, Cfg{
        .defaults = .{ .warehouse = warehouse },
        .overrides = .{},
    }, .{});
}

fn bearerGrants(self: *Handle, a: std.mem.Allocator) ![]const grants.Grant {
    if (self.token == null) return &.{};
    var http_client: std.http.Client = .{ .allocator = self.gpa, .io = self.io };
    defer http_client.deinit();
    const t = vfs.Transport{ .allocator = self.gpa, .io = self.io, .http = &http_client };
    return grants.load(a, t, self.warehouse);
}

fn forbid(a: std.mem.Allocator, request: *http.Server.Request) !void {
    return sendErrA(a, request, .forbidden, "ForbiddenException", "not granted");
}

fn listNamespacesJson(self: *Handle, a: std.mem.Allocator) ![]u8 {
    var http_client: std.http.Client = .{ .allocator = self.gpa, .io = self.io };
    defer http_client.deinit();
    const t = vfs.Transport{ .allocator = self.gpa, .io = self.io, .http = &http_client };
    const nss = try glacier_catalog.listNamespaces(a, t, self.warehouse);
    const gs = try bearerGrants(self, a);
    var buf: std.ArrayList(u8) = .empty;
    try buf.appendSlice(a, "{\"namespaces\":[");
    var n: usize = 0;
    for (nss) |ns| {
        if (self.token != null and !grants.namespaceVisible(gs, ns)) continue;
        if (n > 0) try buf.append(a, ',');
        n += 1;
        try appendNsArray(&buf, a, ns);
    }
    try buf.appendSlice(a, "]}");
    return buf.items;
}

fn listTablesJson(self: *Handle, a: std.mem.Allocator, ns: []const u8) ![]u8 {
    var http_client: std.http.Client = .{ .allocator = self.gpa, .io = self.io };
    defer http_client.deinit();
    const t = vfs.Transport{ .allocator = self.gpa, .io = self.io, .http = &http_client };
    const tables = try glacier_catalog.listTables(a, t, self.warehouse, ns);
    const gs = try bearerGrants(self, a);
    var buf: std.ArrayList(u8) = .empty;
    try buf.appendSlice(a, "{\"identifiers\":[");
    var n: usize = 0;
    for (tables) |tbl| {
        if (self.token != null and !grants.allows(gs, tbl.namespace, tbl.name, false)) continue;
        if (n > 0) try buf.append(a, ',');
        n += 1;
        try buf.appendSlice(a, "{\"namespace\":");
        try appendNsArray(&buf, a, tbl.namespace);
        try buf.appendSlice(a, ",\"name\":");
        try appendJsonStr(&buf, a, tbl.name);
        try buf.append(a, '}');
    }
    try buf.appendSlice(a, "]}");
    return buf.items;
}

fn postNamespace(
    self: *Handle,
    a: std.mem.Allocator,
    request: *http.Server.Request,
    body: []const u8,
) !void {
    const parsed = json.parseFromSliceLeaky(json.Value, a, body, .{ .allocate = .alloc_always }) catch
        return sendErrA(a, request, .bad_request, "BadRequestException", "invalid json");
    const ns = switch (parsed) {
        .object => |obj| try nsFromValue(a, obj.get("namespace") orelse .null),
        else => "",
    };
    if (ns.len == 0) return sendErrA(a, request, .bad_request, "BadRequestException", "namespace");
    if (self.token != null) return forbid(a, request);
    try glacier_catalog.addNamespace(a, self.io, self.warehouse, ns);
    var buf: std.ArrayList(u8) = .empty;
    try buf.appendSlice(a, "{\"namespace\":");
    try appendNsArray(&buf, a, ns);
    try buf.append(a, '}');
    try sendJson(request, .ok, buf.items);
}

fn postTable(
    self: *Handle,
    a: std.mem.Allocator,
    request: *http.Server.Request,
    ns: []const u8,
    body: []const u8,
) !void {
    const parsed = json.parseFromSliceLeaky(json.Value, a, body, .{ .allocate = .alloc_always }) catch
        return sendErrA(a, request, .bad_request, "BadRequestException", "invalid json");
    const obj = switch (parsed) {
        .object => |o| o,
        else => return sendErrA(a, request, .bad_request, "BadRequestException", "invalid json"),
    };
    const name = switch (obj.get("name") orelse .null) {
        .string => |s| s,
        else => "",
    };
    if (name.len == 0) return sendErrA(a, request, .bad_request, "BadRequestException", "name");
    if (self.token != null) return forbid(a, request);

    var http_client: std.http.Client = .{ .allocator = self.gpa, .io = self.io };
    defer http_client.deinit();
    const t = vfs.Transport{ .allocator = self.gpa, .io = self.io, .http = &http_client };
    const existing = try glacier_catalog.listTables(a, t, self.warehouse, ns);
    for (existing) |tbl| {
        if (std.ascii.eqlIgnoreCase(tbl.name, name))
            return sendErrA(a, request, .conflict, "AlreadyExistsException", "already exists");
    }

    const fields = try parseSchemaFields(a, obj.get("schema") orelse .null);
    const part = try parsePartitionFields(a, obj.get("partition-spec") orelse .null);
    const loc = try glacier_catalog.defaultTablePath(a, self.warehouse, ns, name);
    iceberg.createTable(a, self.io, loc, fields, part) catch |err| switch (err) {
        error.InvalidSyntax => return sendErrA(a, request, .bad_request, "BadRequestException", "schema"),
        else => return err,
    };
    glacier_catalog.addTable(a, self.io, self.warehouse, ns, name, loc) catch |err| switch (err) {
        error.InvalidSyntax => return sendErrA(a, request, .conflict, "AlreadyExistsException", "already exists"),
        else => return err,
    };
    const load = try loadTableJson(self, a, ns, name) orelse
        return sendErrA(a, request, .internal_server_error, "InternalServerError", "create");
    try sendJson(request, .ok, load);
}

fn getTable(
    self: *Handle,
    a: std.mem.Allocator,
    request: *http.Server.Request,
    ns: []const u8,
    name: []const u8,
) !void {
    if (self.token != null) {
        const gs = try bearerGrants(self, a);
        if (!grants.allows(gs, ns, name, false)) return forbid(a, request);
    }
    const load = try loadTableJson(self, a, ns, name) orelse
        return sendErrA(a, request, .not_found, "NoSuchTableException", "no such table");
    try sendJson(request, .ok, load);
}

fn deleteTable(
    self: *Handle,
    a: std.mem.Allocator,
    request: *http.Server.Request,
    ns: []const u8,
    name: []const u8,
) !void {
    if (self.token != null) {
        const gs = try bearerGrants(self, a);
        if (!grants.allows(gs, ns, name, true)) return forbid(a, request);
    }
    _ = glacier_catalog.dropTable(a, self.io, self.warehouse, ns, name) catch |err| switch (err) {
        error.TableNotFound => return sendErrA(a, request, .not_found, "NoSuchTableException", "no such table"),
        else => return err,
    };
    try request.respond("", .{
        .status = .no_content,
        .keep_alive = false,
    });
}

fn commitTable(
    self: *Handle,
    a: std.mem.Allocator,
    request: *http.Server.Request,
    ns: []const u8,
    name: []const u8,
    body: []const u8,
) !void {
    if (self.token != null) {
        const gs = try bearerGrants(self, a);
        if (!grants.allows(gs, ns, name, true)) return forbid(a, request);
    }
    var http_client: std.http.Client = .{ .allocator = self.gpa, .io = self.io };
    defer http_client.deinit();
    const t = vfs.Transport{ .allocator = self.gpa, .io = self.io, .http = &http_client };
    const loc = glacier_catalog.tablePath(a, t, self.warehouse, ns, name) catch
        return sendErrA(a, request, .not_found, "NoSuchTableException", "no such table");
    const found = blk: {
        const tables = try glacier_catalog.listTables(a, t, self.warehouse, ns);
        for (tables) |tbl| {
            if (std.ascii.eqlIgnoreCase(tbl.name, name)) break :blk true;
        }
        break :blk false;
    };
    if (!found) return sendErrA(a, request, .not_found, "NoSuchTableException", "no such table");

    const meta_path = try metadataPath(a, t, loc);
    var src = vfs.FileSource.openLocation(t, meta_path) catch
        return sendErrA(a, request, .not_found, "NoSuchTableException", "no such table");
    const meta_text = try src.readAll(a);
    src.close();

    var meta = json.parseFromSliceLeaky(json.Value, a, meta_text, .{ .allocate = .alloc_always }) catch
        return sendErrA(a, request, .bad_request, "BadRequestException", "metadata");
    const meta_obj = switch (meta) {
        .object => |*o| o,
        else => return sendErrA(a, request, .bad_request, "BadRequestException", "metadata"),
    };

    const req = json.parseFromSliceLeaky(json.Value, a, body, .{ .allocate = .alloc_always }) catch
        return sendErrA(a, request, .bad_request, "BadRequestException", "invalid json");
    const req_obj = switch (req) {
        .object => |o| o,
        else => return sendErrA(a, request, .bad_request, "BadRequestException", "invalid json"),
    };

    if (req_obj.get("requirements")) |reqs| {
        switch (reqs) {
            .array => |arr| {
                for (arr.items) |item| {
                    if (!try checkRequirement(meta_obj.*, item))
                        return sendErrA(a, request, .conflict, "CommitConflictException", "conflict");
                }
            },
            else => {},
        }
    }
    if (req_obj.get("updates")) |upds| {
        switch (upds) {
            .array => |arr| {
                for (arr.items) |item| {
                    try applyUpdate(a, meta_obj, item);
                }
            },
            else => {},
        }
    }

    const next = try nextHint(a, t, loc);
    const out_json = try json.Stringify.valueAlloc(a, meta, .{});
    try writeMetaVersion(a, self.io, loc, next, out_json);
    const load = try loadTableJson(self, a, ns, name) orelse
        return sendErrA(a, request, .internal_server_error, "InternalServerError", "commit");
    try sendJson(request, .ok, load);
}

fn checkRequirement(meta: json.ObjectMap, item: json.Value) !bool {
    const obj = switch (item) {
        .object => |o| o,
        else => return true,
    };
    const typ = switch (obj.get("type") orelse .null) {
        .string => |s| s,
        else => return true,
    };
    if (std.mem.eql(u8, typ, "assert-table-uuid")) {
        const want = switch (obj.get("uuid") orelse .null) {
            .string => |s| s,
            else => return false,
        };
        const have = switch (meta.get("table-uuid") orelse .null) {
            .string => |s| s,
            else => "",
        };
        return std.mem.eql(u8, want, have);
    }
    if (std.mem.eql(u8, typ, "assert-ref-snapshot-id")) {
        const expected = snapshotIdValue(obj.get("snapshot-id") orelse .null);
        const current = snapshotIdValue(meta.get("current-snapshot-id") orelse .null);
        return expected == current;
    }
    return true;
}

fn snapshotIdValue(v: json.Value) ?i64 {
    return switch (v) {
        .null => null,
        .integer => |n| if (n == -1) null else n,
        .float => |n| blk: {
            const i: i64 = @intFromFloat(n);
            break :blk if (i == -1) null else i;
        },
        else => null,
    };
}

fn applyUpdate(a: std.mem.Allocator, meta: *json.ObjectMap, item: json.Value) !void {
    const obj = switch (item) {
        .object => |o| o,
        else => return,
    };
    const act = switch (obj.get("action") orelse .null) {
        .string => |s| s,
        else => return,
    };
    if (std.mem.eql(u8, act, "add-snapshot")) {
        const snap = obj.get("snapshot") orelse .null;
        try appendToArray(a, meta, "snapshots", snap);
        if (snap == .object) {
            if (snap.object.get("sequence-number")) |seq| {
                try meta.put(a, "last-sequence-number", seq);
            }
            if (snap.object.get("timestamp-ms")) |ts| {
                try meta.put(a, "last-updated-ms", ts);
            }
        }
    } else if (std.mem.eql(u8, act, "set-snapshot-ref")) {
        if (obj.get("snapshot-id")) |sid| {
            try meta.put(a, "current-snapshot-id", sid);
        }
    } else if (std.mem.eql(u8, act, "add-schema")) {
        const sch = obj.get("schema") orelse .null;
        try appendToArray(a, meta, "schemas", sch);
        if (sch == .object) {
            if (sch.object.get("fields")) |fv| {
                const last = maxFieldId(fv);
                const cur = switch (meta.get("last-column-id") orelse .null) {
                    .integer => |n| n,
                    else => 0,
                };
                if (last > cur) try meta.put(a, "last-column-id", .{ .integer = last });
            }
        }
    } else if (std.mem.eql(u8, act, "set-current-schema")) {
        if (obj.get("schema-id")) |sid| {
            try meta.put(a, "current-schema-id", sid);
        }
    }
}

fn appendToArray(a: std.mem.Allocator, meta: *json.ObjectMap, key: []const u8, item: json.Value) !void {
    if (meta.getPtr(key)) |ptr| {
        switch (ptr.*) {
            .array => |*arr| {
                try arr.append(item);
                return;
            },
            else => {},
        }
    }
    var arr = json.Array.init(a);
    try arr.append(item);
    try meta.put(a, key, .{ .array = arr });
}

fn maxFieldId(v: json.Value) i64 {
    const arr = switch (v) {
        .array => |a| a,
        else => return 0,
    };
    var last: i64 = 0;
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |o| o,
            else => continue,
        };
        const id = snapshotIdValue(obj.get("id") orelse .null) orelse 0;
        if (id > last) last = id;
    }
    return last;
}

fn loadTableJson(self: *Handle, a: std.mem.Allocator, ns: []const u8, name: []const u8) !?[]u8 {
    var http_client: std.http.Client = .{ .allocator = self.gpa, .io = self.io };
    defer http_client.deinit();
    const t = vfs.Transport{ .allocator = self.gpa, .io = self.io, .http = &http_client };
    const tables = try glacier_catalog.listTables(a, t, self.warehouse, ns);
    var loc: ?[]const u8 = null;
    for (tables) |tbl| {
        if (std.ascii.eqlIgnoreCase(tbl.name, name)) {
            loc = if (tbl.location.len > 0) tbl.location else try glacier_catalog.defaultTablePath(a, self.warehouse, ns, name);
            break;
        }
    }
    const table_dir = loc orelse return null;
    const meta_path = try metadataPath(a, t, table_dir);
    var src = vfs.FileSource.openLocation(t, meta_path) catch return null;
    const meta_text = try src.readAll(a);
    src.close();
    const loc_json = try json.Stringify.valueAlloc(a, meta_path, .{});
    return try std.fmt.allocPrint(a, "{{\"metadata-location\":{s},\"metadata\":{s},\"config\":{{}}}}", .{
        loc_json,
        meta_text,
    });
}

fn metadataPath(allocator: std.mem.Allocator, t: vfs.Transport, table_dir: []const u8) ![]u8 {
    const hint_path = try vfs.joinLocation(allocator, table_dir, "metadata/version-hint.text");
    if (readText(allocator, t, hint_path)) |hint| {
        const trimmed = std.mem.trim(u8, hint, " \t\r\n");
        const with_v = try vfs.joinLocation(
            allocator,
            table_dir,
            try std.fmt.allocPrint(allocator, "metadata/v{s}.metadata.json", .{trimmed}),
        );
        if (readable(t, with_v)) return with_v;
        const without_v = try vfs.joinLocation(
            allocator,
            table_dir,
            try std.fmt.allocPrint(allocator, "metadata/{s}.metadata.json", .{trimmed}),
        );
        if (readable(t, without_v)) return without_v;
        return with_v;
    } else |_| {
        return vfs.joinLocation(allocator, table_dir, "metadata/v1.metadata.json");
    }
}

fn nextHint(allocator: std.mem.Allocator, t: vfs.Transport, table_dir: []const u8) !i32 {
    const hint_path = vfs.joinLocation(allocator, table_dir, "metadata/version-hint.text") catch return 1;
    const text = readText(allocator, t, hint_path) catch return 1;
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    const n = std.fmt.parseInt(i32, trimmed, 10) catch return 1;
    return n + 1;
}

fn writeMetaVersion(
    allocator: std.mem.Allocator,
    io: std.Io,
    table_dir: []const u8,
    ver: i32,
    json_text: []const u8,
) !void {
    try std.Io.Dir.cwd().createDirPath(io, try std.fs.path.join(allocator, &.{ table_dir, "metadata" }));
    const meta_path = try std.fmt.allocPrint(allocator, "{s}/metadata/v{d}.metadata.json", .{ table_dir, ver });
    try writeText(io, meta_path, json_text);
    const hint = try std.fmt.allocPrint(allocator, "{d}\n", .{ver});
    try writeText(io, try std.fmt.allocPrint(allocator, "{s}/metadata/version-hint.text", .{table_dir}), hint);
}

fn writeText(io: std.Io, path: []const u8, text: []const u8) !void {
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    var buf: [1024]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(text);
    try writer.interface.flush();
}

fn readText(allocator: std.mem.Allocator, t: vfs.Transport, path: []const u8) ![]u8 {
    var source = try vfs.FileSource.openLocation(t, path);
    defer source.close();
    return source.readAll(allocator);
}

fn readable(t: vfs.Transport, path: []const u8) bool {
    var source = vfs.FileSource.openLocation(t, path) catch return false;
    source.close();
    return true;
}

fn parseSchemaFields(a: std.mem.Allocator, schema: json.Value) ![]iceberg.SchemaField {
    const obj = switch (schema) {
        .object => |o| o,
        else => return error.InvalidSyntax,
    };
    const fields_v = obj.get("fields") orelse return error.InvalidSyntax;
    const arr = switch (fields_v) {
        .array => |x| x,
        else => return error.InvalidSyntax,
    };
    if (arr.items.len == 0) return error.InvalidSyntax;
    const out = try a.alloc(iceberg.SchemaField, arr.items.len);
    for (arr.items, 0..) |item, i| {
        const f = switch (item) {
            .object => |o| o,
            else => return error.InvalidSyntax,
        };
        const type_name = switch (f.get("type") orelse .null) {
            .string => |s| s,
            else => return error.UnsupportedNested,
        };
        const required = switch (f.get("required") orelse .null) {
            .bool => |b| b,
            else => true,
        };
        out[i] = .{
            .id = @intCast(snapshotIdValue(f.get("id") orelse .null) orelse return error.InvalidSyntax),
            .name = switch (f.get("name") orelse .null) {
                .string => |s| s,
                else => return error.InvalidSyntax,
            },
            .required = required,
            .type_name = type_name,
        };
    }
    return out;
}

fn parsePartitionFields(a: std.mem.Allocator, spec: json.Value) ![]iceberg.PartitionField {
    const fields_v: json.Value = switch (spec) {
        .object => |o| o.get("fields") orelse .null,
        .array => spec,
        .null => return &.{},
        else => return &.{},
    };
    const arr = switch (fields_v) {
        .array => |x| x,
        .null => return &.{},
        else => return &.{},
    };
    if (arr.items.len == 0) return &.{};
    const out = try a.alloc(iceberg.PartitionField, arr.items.len);
    for (arr.items, 0..) |item, i| {
        const f = switch (item) {
            .object => |o| o,
            else => return error.InvalidSyntax,
        };
        out[i] = .{
            .source_id = @intCast(snapshotIdValue(f.get("source-id") orelse .null) orelse 0),
            .field_id = @intCast(snapshotIdValue(f.get("field-id") orelse .null) orelse @as(i64, @intCast(1000 + i))),
            .name = switch (f.get("name") orelse .null) {
                .string => |s| s,
                else => "",
            },
            .transform = switch (f.get("transform") orelse .null) {
                .string => |s| s,
                else => "identity",
            },
        };
    }
    return out;
}

fn nsFromValue(a: std.mem.Allocator, v: json.Value) ![]const u8 {
    return switch (v) {
        .string => |s| s,
        .array => |arr| blk: {
            var buf: std.ArrayList(u8) = .empty;
            for (arr.items) |item| {
                const part = switch (item) {
                    .string => |s| s,
                    else => continue,
                };
                if (buf.items.len > 0) try buf.append(a, '.');
                try buf.appendSlice(a, part);
            }
            break :blk buf.items;
        },
        else => "",
    };
}

fn decodeNs(allocator: std.mem.Allocator, enc: []const u8) ![]u8 {
    const buf = try allocator.dupe(u8, enc);
    defer allocator.free(buf);
    const decoded = std.Uri.percentDecodeInPlace(buf);
    for (decoded) |*c| {
        if (c.* == 0x1f) c.* = '.';
    }
    return allocator.dupe(u8, decoded);
}

fn appendNsArray(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, namespace: []const u8) !void {
    try buf.append(allocator, '[');
    var it = std.mem.splitScalar(u8, namespace, '.');
    var first = true;
    while (it.next()) |part| {
        if (part.len == 0) continue;
        if (!first) try buf.append(allocator, ',');
        first = false;
        try appendJsonStr(buf, allocator, part);
    }
    try buf.append(allocator, ']');
}

fn appendJsonStr(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    const escaped = try json.Stringify.valueAlloc(allocator, s, .{});
    try buf.appendSlice(allocator, escaped);
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

test "parseListen loopback only" {
    const pub_ok = try parseListenPortTls("0.0.0.0:8181", 8181, true);
    try std.testing.expect(!isLoopback(pub_ok));
    const a = try parseListen("127.0.0.1:8181");
    try std.testing.expectEqual(@as(u16, 8181), a.getPort());
    const b = try parseListen("localhost");
    try std.testing.expectEqual(@as(u16, 8181), b.getPort());
    try std.testing.expectError(error.TlsRequired, parseListen("0.0.0.0:8181"));
    try std.testing.expectError(error.TlsRequired, parseListenPortTls("0.0.0.0:9", 8181, false));
    try std.testing.expectError(error.TlsRequired, parseListen("1.2.3.4:90"));
}

test "tls rest answers https config" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const cert = "/tmp/glacier-tls-test.crt";
    const key = "/tmp/glacier-tls-test.key";
    var gen = std.process.spawn(io, .{
        .argv = &.{
            "openssl", "req",    "-x509", "-newkey",       "rsa:2048",
            "-keyout", key,      "-out",  cert,            "-days",
            "1",       "-nodes", "-subj", "/CN=localhost",
        },
        .stdout = .ignore,
        .stderr = .ignore,
        .stdin = .ignore,
    }) catch return error.SkipZigTest;
    switch (gen.wait(io) catch return error.SkipZigTest) {
        .exited => |code| if (code != 0) return error.SkipZigTest,
        else => return error.SkipZigTest,
    }

    const root = "/tmp/glacier_tls_wh";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var h = try bindTls(gpa, io, root, "127.0.0.1:0", cert, key);
    var fut = try io.concurrent(serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }
    const url = try std.fmt.allocPrint(gpa, "https://127.0.0.1:{d}/v1/config", .{h.port()});
    defer gpa.free(url);
    var py = try std.process.spawn(io, .{
        .argv = &.{
            "python3", "-c",
            \\import ssl, sys, urllib.request
            \\ctx = ssl._create_unverified_context()
            \\print(urllib.request.urlopen(sys.argv[1], context=ctx, timeout=5).read().decode())
            ,
            url,
        },
        .stdout = .pipe,
        .stderr = .ignore,
        .stdin = .ignore,
    });
    defer py.kill(io);
    var line_buf: [2048]u8 = undefined;
    var stdout_reader = py.stdout.?.readerStreaming(io, &line_buf);
    const line = try stdout_reader.interface.takeDelimiterExclusive('\n');
    try std.testing.expect(std.mem.indexOf(u8, line, "glacier_tls_wh") != null);
}

fn httpStatus(gpa: std.mem.Allocator, io: std.Io, url: []const u8, headers: []const http.Header) !http.Status {
    var client: std.http.Client = .{ .allocator = gpa, .io = io };
    defer client.deinit();
    var aw: std.Io.Writer.Allocating = .init(gpa);
    defer aw.deinit();
    const result = try client.fetch(.{
        .location = .{ .url = url },
        .method = .GET,
        .extra_headers = headers,
        .response_writer = &aw.writer,
        .keep_alive = false,
        .headers = .{ .accept_encoding = .omit },
    });
    return result.status;
}

test "bearer token gates REST" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_bearer_wh";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var h = try bind(gpa, io, root, "127.0.0.1:0");
    try h.setBearer("s3cret");
    var fut = try io.concurrent(serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }
    const url = try std.fmt.allocPrint(gpa, "http://127.0.0.1:{d}/v1/config", .{h.port()});
    defer gpa.free(url);
    try std.testing.expectEqual(http.Status.unauthorized, try httpStatus(gpa, io, url, &.{}));
    try std.testing.expectEqual(http.Status.unauthorized, try httpStatus(gpa, io, url, &.{
        .{ .name = "Authorization", .value = "Bearer nope" },
    }));
    try std.testing.expectEqual(http.Status.ok, try httpStatus(gpa, io, url, &.{
        .{ .name = "Authorization", .value = "Bearer s3cret" },
    }));
}

test "decodeNs unit separator" {
    const ns = try decodeNs(std.testing.allocator, "public%1Fnyc");
    defer std.testing.allocator.free(ns);
    try std.testing.expectEqualStrings("public.nyc", ns);
}
