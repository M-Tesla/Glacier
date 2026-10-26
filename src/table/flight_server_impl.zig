//! Native Arrow Flight SQL server. Session per connection; not in WASM.

const std = @import("std");
const glacier = @import("glacier");
const h2 = @import("h2.zig");

const net = std.Io.net;
const Session = glacier.Session;
const Batch = glacier.Batch;
const Column = glacier.batch.Column;
const arrow_ipc = glacier.arrow_ipc;
const rest_server = glacier.rest_server;
const glacier_catalog = glacier.glacier_catalog;

extern fn glacier_tls_server(cert: [*:0]const u8, key: [*:0]const u8) ?*anyopaque;
extern fn glacier_tls_free(box: ?*anyopaque) void;
extern fn glacier_tls_accept(box: *anyopaque, fd: c_int) ?*anyopaque;
extern fn glacier_tls_read(ssl: *anyopaque, buf: [*]u8, n: c_int) c_int;
extern fn glacier_tls_write(ssl: *anyopaque, buf: [*]const u8, n: c_int) c_int;
extern fn glacier_tls_close(ssl: ?*anyopaque) void;

const svc = "/arrow.flight.protocol.FlightService/";
const sql_query_url = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery";

pub const Handle = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []u8,
    server: net.Server,
    tls: ?*anyopaque = null,
    token: ?[]u8 = null,
    /// Not owned. One RS256 provider. Null means the shared token, or no auth.
    keys: ?*const glacier.jwt.Set = null,
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
        self.gpa.free(self.warehouse);
        if (self.token) |tok| self.gpa.free(tok);
        if (self.tls) |ctx| glacier_tls_free(ctx);
    }
};

pub fn bind(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
) !Handle {
    return bindDoor(gpa, io, warehouse, listen, null, null, null, null);
}

fn bindDoor(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
    cert: ?[]const u8,
    key: ?[]const u8,
    bearer: ?[]const u8,
    keys: ?*const glacier.jwt.Set,
) !Handle {
    if ((cert == null) != (key == null)) return error.TlsRequired;
    var addr = try rest_server.parseListenPortTls(listen, 8815, cert != null);
    const has_secret = bearer != null and bearer.?.len > 0;
    if (!addrIsLoopback(addr) and !has_secret and keys == null) return error.TokenRequired;
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
    const token = if (bearer) |tok| try gpa.dupe(u8, tok) else null;
    errdefer if (token) |tok| gpa.free(tok);
    return .{
        .gpa = gpa,
        .io = io,
        .warehouse = try gpa.dupe(u8, warehouse),
        .server = server,
        .tls = tls,
        .token = token,
        .keys = keys,
    };
}

fn addrIsLoopback(addr: net.IpAddress) bool {
    return switch (addr) {
        .ip4 => |a| a.bytes[0] == 127,
        .ip6 => |a| a.bytes[0] == 0 and a.bytes[15] == 1 and std.mem.allEqual(u8, a.bytes[1..15], 0),
    };
}

pub fn run(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
    flight: []const u8,
    out: *std.Io.Writer,
    tls_cert: ?[]const u8,
    tls_key: ?[]const u8,
    bearer: ?[]const u8,
    jwks_path: ?[]const u8,
    oidc_issuer: ?[]const u8,
    oidc_audience: ?[]const u8,
) !void {
    var keys_store: ?glacier.jwt.Set = null;
    defer if (keys_store) |*k| k.deinit();
    if (jwks_path) |p| {
        keys_store = try glacier.jwt.loadPath(gpa, io, p, oidc_issuer orelse "", oidc_audience orelse "");
    } else if (oidc_issuer) |iss| {
        keys_store = try glacier.jwt.loadIssuer(gpa, io, iss, oidc_audience orelse "");
    }
    const keys: ?*const glacier.jwt.Set = if (keys_store) |*k| k else null;
    var rest = try rest_server.bindTls(gpa, io, warehouse, listen, tls_cert, tls_key);
    if (bearer) |tok| try rest.setBearer(tok);
    if (keys) |k| rest.setKeys(k);
    defer rest.deinit();
    var fl = try bindDoor(gpa, io, warehouse, flight, tls_cert, tls_key, bearer, keys);
    defer fl.deinit();
    const rest_scheme: []const u8 = if (rest.tls != null) "https" else "http";
    const flight_scheme: []const u8 = if (fl.tls != null) "grpcs" else "grpc";
    try out.print("Iceberg REST  {s}://{f}\n", .{ rest_scheme, rest.server.socket.address });
    try out.print("Arrow Flight  {s}://{f}\n", .{ flight_scheme, fl.server.socket.address });
    try out.print("warehouse     {s}\n", .{warehouse});
    if (bearer != null) try out.print("auth          bearer\n", .{});
    if (keys != null) try out.print("auth          jwt\n", .{});
    try out.flush();
    var rest_fut = try io.concurrent(rest_server.serveLoop, .{&rest});
    defer _ = rest_fut.cancel(io) catch {};
    try serveLoop(&fl);
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
    var send_buffer: [16 * 1024]u8 = undefined;
    var recv_buffer: [16 * 1024]u8 = undefined;
    var connection_reader = stream.reader(io, &recv_buffer);
    var connection_writer = stream.writer(io, &send_buffer);
    serveH2(self, &connection_reader.interface, &connection_writer.interface);
}

fn serveConnTls(self: *Handle, stream: net.Stream, ctx: *anyopaque) void {
    const io = self.io;
    defer {
        var copy = stream;
        copy.close(io);
    }
    const ssl = glacier_tls_accept(ctx, stream.socket.handle) orelse return;
    defer glacier_tls_close(ssl);
    var pipe: TlsPipe = .{ .ssl = ssl };
    pipe.reader = .{
        .vtable = &tls_reader_vt,
        .buffer = &pipe.rbuf,
        .seek = 0,
        .end = 0,
    };
    pipe.writer = .{
        .vtable = &tls_writer_vt,
        .buffer = &.{},
        .end = 0,
    };
    serveH2(self, &pipe.reader, &pipe.writer);
}

const TlsPipe = struct {
    ssl: *anyopaque,
    rbuf: [16 * 1024]u8 = undefined,
    reader: std.Io.Reader = undefined,
    writer: std.Io.Writer = undefined,
};

const tls_reader_vt = std.Io.Reader.VTable{ .stream = tlsStream };
const tls_writer_vt = std.Io.Writer.VTable{ .drain = tlsDrain };

fn tlsStream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
    const pipe: *TlsPipe = @alignCast(@fieldParentPtr("reader", r));
    const cap = limit.minInt(4096);
    if (cap == 0) return 0;
    var tmp: [4096]u8 = undefined;
    const n = glacier_tls_read(pipe.ssl, &tmp, @intCast(cap));
    if (n == 0) return error.EndOfStream;
    if (n < 0) return error.ReadFailed;
    w.writeAll(tmp[0..@intCast(n)]) catch return error.WriteFailed;
    return @intCast(n);
}

fn tlsDrain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
    const pipe: *TlsPipe = @alignCast(@fieldParentPtr("writer", w));
    if (data.len == 0) return 0;
    var total: usize = 0;
    for (data[0 .. data.len - 1]) |chunk| {
        sslWriteAll(pipe.ssl, chunk) catch return error.WriteFailed;
        total += chunk.len;
    }
    const last = data[data.len - 1];
    for (0..splat) |_| {
        sslWriteAll(pipe.ssl, last) catch return error.WriteFailed;
        total += last.len;
    }
    return total;
}

fn sslWriteAll(ssl: *anyopaque, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = glacier_tls_write(ssl, bytes[off..].ptr, @intCast(bytes.len - off));
        if (n <= 0) return error.WriteFailed;
        off += @intCast(n);
    }
}

fn serveH2(self: *Handle, reader: *std.Io.Reader, writer: *std.Io.Writer) void {
    var conn = h2.Conn.init(self.gpa, reader, writer);
    defer conn.deinit();
    conn.expectPreface() catch return;
    conn.writeSettings() catch return;

    var sess = Session.open(self.gpa, self.io, self.warehouse) catch return;
    defer sess.close();

    var streams = std.AutoHashMap(u32, StreamBuf).init(self.gpa);
    defer {
        var it = streams.iterator();
        while (it.next()) |e| e.value_ptr.deinit(self.gpa);
        streams.deinit();
    }

    while (true) {
        const frame = conn.readFrame() catch return;
        defer self.gpa.free(frame.payload);
        switch (frame.typ) {
            .settings => {
                if (frame.flags & h2.Flag.ack == 0) conn.writeSettingsAck() catch return;
            },
            .ping => {
                if (frame.flags & h2.Flag.ack == 0)
                    conn.writeFrame(.ping, h2.Flag.ack, 0, frame.payload) catch return;
            },
            .window_update, .rst_stream => {},
            .goaway => return,
            .headers, .data => {
                handleStream(self, &conn, &sess, &streams, frame) catch |err| {
                    const msg = glacier.err.staticMessage(err);
                    conn.writeGrpcError(frame.stream, "2", msg) catch {};
                };
            },
            else => {},
        }
    }
}

const StreamBuf = struct {
    path: []u8 = &.{},
    data: std.ArrayList(u8) = .empty,
    ended: bool = false,
    cred: []u8 = &.{},

    fn deinit(self: *StreamBuf, gpa: std.mem.Allocator) void {
        if (self.path.len > 0) gpa.free(self.path);
        if (self.cred.len > 0) gpa.free(self.cred);
        self.data.deinit(gpa);
    }
};

fn handleStream(
    self: *Handle,
    conn: *h2.Conn,
    sess: *Session,
    streams: *std.AutoHashMap(u32, StreamBuf),
    frame: h2.Frame,
) !void {
    var arena = std.heap.ArenaAllocator.init(self.gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const gop = try streams.getOrPut(frame.stream);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{};
    }

    var payload = frame.payload;
    if (frame.flags & h2.Flag.padded != 0) {
        if (payload.len == 0) return;
        const pad = payload[0];
        if (1 + pad > payload.len) return error.Http2Preface;
        payload = payload[1 .. payload.len - pad];
    }
    if (frame.typ == .headers and frame.flags & h2.Flag.priority != 0) {
        if (payload.len < 5) return;
        payload = payload[5..];
    }

    if (frame.typ == .headers) {
        var headers: std.ArrayList(h2.Header) = .empty;
        try conn.decodeHeaders(payload, &headers, a);
        const path = h2.pathOf(headers.items);
        if (gop.value_ptr.path.len > 0) self.gpa.free(gop.value_ptr.path);
        gop.value_ptr.path = try self.gpa.dupe(u8, path);
        const raw = bearerRaw(headers.items);
        if (gop.value_ptr.cred.len > 0) self.gpa.free(gop.value_ptr.cred);
        gop.value_ptr.cred = try self.gpa.dupe(u8, raw);
    } else {
        try gop.value_ptr.data.appendSlice(self.gpa, payload);
    }

    if (frame.flags & h2.Flag.end_stream == 0) return;

    const path = gop.value_ptr.path;
    const body = gop.value_ptr.data.items;
    if (needsAuth(self)) {
        applyPrincipal(self, sess, gop.value_ptr.cred) catch {
            try conn.writeGrpcError(frame.stream, "16", "missing or invalid bearer token");
            gop.value_ptr.deinit(self.gpa);
            _ = streams.remove(frame.stream);
            return;
        };
    }
    const msgs = try grpcSplit(a, body);
    if (std.mem.eql(u8, path, svc ++ "Handshake")) {
        try conn.writeGrpcMessages(frame.stream, &.{try pbHandshake()}, true);
    } else if (std.mem.eql(u8, path, svc ++ "GetFlightInfo")) {
        const sql = try sqlFromDescriptor(a, if (msgs.len > 0) msgs[0] else &.{});
        const info = try encodeFlightInfo(a, sql);
        try conn.writeGrpcMessages(frame.stream, &.{info}, true);
    } else if (std.mem.eql(u8, path, svc ++ "GetSchema")) {
        const sql = try sqlFromDescriptor(a, if (msgs.len > 0) msgs[0] else &.{});
        const schema = try schemaForSql(a, sess, sql);
        const result = try encodeSchemaResult(a, schema);
        try conn.writeGrpcMessages(frame.stream, &.{result}, true);
    } else if (std.mem.eql(u8, path, svc ++ "DoGet")) {
        const sql = try sqlFromTicket(a, if (msgs.len > 0) msgs[0] else &.{});
        try doGet(self.gpa, conn, sess, frame.stream, sql);
    } else {
        try conn.writeGrpcError(frame.stream, "12", "unimplemented");
    }

    if (gop.value_ptr.path.len > 0) self.gpa.free(gop.value_ptr.path);
    if (gop.value_ptr.cred.len > 0) self.gpa.free(gop.value_ptr.cred);
    gop.value_ptr.data.deinit(self.gpa);
    _ = streams.remove(frame.stream);
}

fn needsAuth(self: *const Handle) bool {
    return self.token != null or self.keys != null;
}

fn applyPrincipal(self: *Handle, sess: *Session, raw: []const u8) !void {
    const now = std.Io.Timestamp.now(self.io, .real).toSeconds();
    const who = glacier.jwt.identify(self.gpa, self.token, self.keys, raw, now) catch return error.NotAuthorized;
    defer self.gpa.free(who);
    try sess.setPrincipal(who);
    sess.as_bearer = true;
}

fn bearerRaw(headers: []const h2.Header) []const u8 {
    const v = h2.headerValue(headers, "authorization");
    const prefix = "Bearer ";
    if (v.len <= prefix.len) return "";
    if (!std.ascii.eqlIgnoreCase(v[0..prefix.len], prefix)) return "";
    return v[prefix.len..];
}

fn doGet(gpa: std.mem.Allocator, conn: *h2.Conn, sess: *Session, stream: u32, sql: []const u8) !void {
    var result = sess.execute(sql) catch |err| {
        try conn.writeGrpcError(stream, "2", glacier.err.staticMessage(err));
        return;
    };
    defer result.deinit();
    const schema_hdr = try arrow_ipc.encodeSchemaHeader(gpa, result.batch);
    defer gpa.free(schema_hdr);
    const schema_msg = try encodeFlightData(gpa, schema_hdr, &.{});
    defer gpa.free(schema_msg);
    try conn.writeHeaders(stream, &.{
        .{ .name = ":status", .value = "200" },
        .{ .name = "content-type", .value = "application/grpc" },
    }, false);
    {
        const framed = try h2.grpcFrame(gpa, schema_msg);
        defer gpa.free(framed);
        try conn.writeData(stream, framed, false);
    }
    while (result.nextBatch()) |batch| {
        const enc = try arrow_ipc.encodeBatch(gpa, batch);
        defer enc.deinit(gpa);
        const batch_msg = try encodeFlightData(gpa, enc.header, enc.body);
        defer gpa.free(batch_msg);
        const framed = try h2.grpcFrame(gpa, batch_msg);
        defer gpa.free(framed);
        try conn.writeData(stream, framed, false);
    }
    try conn.writeHeaders(stream, &.{
        .{ .name = "grpc-status", .value = "0" },
    }, true);
}

fn schemaForSql(a: std.mem.Allocator, sess: *Session, sql: []const u8) ![]u8 {
    if (looksLikeWrite(sql)) {
        var cols = [_]Column{.{ .name = "affected", .data_type = .int64, .len = 0, .i64s = &.{} }};
        const batch: Batch = .{ .columns = &cols, .len = 0 };
        return arrow_ipc.encodeSchema(a, batch);
    }
    var result = try sess.execute(sql);
    defer result.deinit();
    return arrow_ipc.encodeSchema(a, result.batch);
}

fn looksLikeWrite(sql: []const u8) bool {
    const t = std.mem.trim(u8, sql, " \t\r\n");
    const keys = [_][]const u8{ "INSERT", "CREATE", "DROP", "DELETE", "UPDATE", "MERGE", "ALTER", "COPY", "ATTACH", "DETACH", "USE" };
    for (keys) |k| {
        if (t.len >= k.len and std.ascii.eqlIgnoreCase(t[0..k.len], k)) return true;
    }
    return false;
}

fn sqlFromDescriptor(a: std.mem.Allocator, desc: []const u8) ![]u8 {
    const cmd = pbBytes(desc, 3) orelse return error.InvalidFlight;
    if (unwrapAny(a, cmd)) |sql| return sql;
    if (cmd.len > 0) return a.dupe(u8, cmd);
    return error.InvalidFlight;
}

fn sqlFromTicket(a: std.mem.Allocator, ticket_msg: []const u8) ![]u8 {
    const inner = pbBytes(ticket_msg, 1) orelse ticket_msg;
    return a.dupe(u8, inner);
}

fn unwrapAny(a: std.mem.Allocator, any_bytes: []const u8) ?[]u8 {
    const url = pbString(any_bytes, 1) orelse return null;
    const value = pbBytes(any_bytes, 2) orelse &.{};
    if (std.mem.endsWith(u8, url, "CommandStatementQuery")) {
        const q = pbString(value, 1) orelse return null;
        return a.dupe(u8, q) catch null;
    }
    if (std.mem.endsWith(u8, url, "CommandGetCatalogs")) {
        return a.dupe(u8, "SHOW CATALOGS") catch null;
    }
    if (std.mem.endsWith(u8, url, "CommandGetTables")) {
        return a.dupe(u8, "SHOW TABLES") catch null;
    }
    return null;
}

fn encodeFlightInfo(a: std.mem.Allocator, sql: []const u8) ![]u8 {
    var cols = [_]Column{.{ .name = "col", .data_type = .int64, .len = 0, .i64s = &.{} }};
    const batch: Batch = .{ .columns = &cols, .len = 0 };
    const schema = try arrow_ipc.encodeSchema(a, batch);

    var ticket_inner: std.ArrayList(u8) = .empty;
    try pbPutBytes(&ticket_inner, a, 1, sql);

    var endpoint: std.ArrayList(u8) = .empty;
    try pbPutMsg(&endpoint, a, 1, ticket_inner.items);

    var desc: std.ArrayList(u8) = .empty;
    try pbPutVarint(&desc, a, 1, 2);
    try pbPutBytes(&desc, a, 3, sql);

    var info: std.ArrayList(u8) = .empty;
    try pbPutBytes(&info, a, 1, schema);
    try pbPutMsg(&info, a, 2, desc.items);
    try pbPutMsg(&info, a, 3, endpoint.items);
    return info.toOwnedSlice(a);
}

fn encodeSchemaResult(a: std.mem.Allocator, schema: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try pbPutBytes(&out, a, 1, schema);
    return out.toOwnedSlice(a);
}

fn encodeFlightData(gpa: std.mem.Allocator, header: []const u8, body: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    try pbPutBytes(&out, gpa, 2, header);
    if (body.len > 0) try pbPutBytes(&out, gpa, 1000, body);
    return out.toOwnedSlice(gpa);
}

fn pbHandshake() ![]u8 {
    return &.{};
}

fn grpcSplit(a: std.mem.Allocator, buf: []const u8) ![][]const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var i: usize = 0;
    while (i + 5 <= buf.len) {
        const n = std.mem.readInt(u32, buf[i + 1 ..][0..4], .big);
        i += 5;
        if (i + n > buf.len) break;
        try out.append(a, buf[i .. i + n]);
        i += n;
    }
    return out.items;
}

fn pbPutVarint(out: *std.ArrayList(u8), a: std.mem.Allocator, field: u32, v: u64) !void {
    try putVarint(out, a, (@as(u64, field) << 3) | 0);
    try putVarint(out, a, v);
}

fn pbPutBytes(out: *std.ArrayList(u8), a: std.mem.Allocator, field: u32, bytes: []const u8) !void {
    try putVarint(out, a, (@as(u64, field) << 3) | 2);
    try putVarint(out, a, bytes.len);
    try out.appendSlice(a, bytes);
}

fn pbPutMsg(out: *std.ArrayList(u8), a: std.mem.Allocator, field: u32, bytes: []const u8) !void {
    try pbPutBytes(out, a, field, bytes);
}

fn putVarint(out: *std.ArrayList(u8), a: std.mem.Allocator, v: u64) !void {
    var x = v;
    while (x >= 0x80) {
        try out.append(a, @as(u8, @intCast((x & 0x7f) | 0x80)));
        x >>= 7;
    }
    try out.append(a, @intCast(x));
}

fn pbBytes(src: []const u8, field: u32) ?[]const u8 {
    var i: usize = 0;
    while (i < src.len) {
        const key, const kn = readVarint(src[i..]) orelse return null;
        i += kn;
        const num: u32 = @intCast(key >> 3);
        const wire: u3 = @intCast(key & 7);
        switch (wire) {
            0 => {
                _, const n = readVarint(src[i..]) orelse return null;
                i += n;
            },
            1 => {
                if (i + 8 > src.len) return null;
                i += 8;
            },
            2 => {
                const len, const n = readVarint(src[i..]) orelse return null;
                i += n;
                if (i + len > src.len) return null;
                const slice = src[i .. i + len];
                i += len;
                if (num == field) return slice;
            },
            5 => {
                if (i + 4 > src.len) return null;
                i += 4;
            },
            else => return null,
        }
    }
    return null;
}

fn pbString(src: []const u8, field: u32) ?[]const u8 {
    return pbBytes(src, field);
}

fn readVarint(src: []const u8) ?struct { u64, usize } {
    var v: u64 = 0;
    var s: u6 = 0;
    var n: usize = 0;
    while (n < src.len and n < 10) {
        const b = src[n];
        n += 1;
        v |= @as(u64, b & 0x7f) << s;
        if (b & 0x80 == 0) return .{ v, n };
        s += 7;
        if (s > 63) return null;
    }
    return null;
}

fn encodeAnyQuery(a: std.mem.Allocator, sql: []const u8) ![]u8 {
    var cmd: std.ArrayList(u8) = .empty;
    try pbPutBytes(&cmd, a, 1, sql);
    var any: std.ArrayList(u8) = .empty;
    try pbPutBytes(&any, a, 1, sql_query_url);
    try pbPutBytes(&any, a, 2, cmd.items);
    return any.toOwnedSlice(a);
}

fn encodeDescriptor(a: std.mem.Allocator, sql: []const u8) ![]u8 {
    const any = try encodeAnyQuery(a, sql);
    var desc: std.ArrayList(u8) = .empty;
    try pbPutVarint(&desc, a, 1, 2);
    try pbPutBytes(&desc, a, 3, any);
    return desc.toOwnedSlice(a);
}

fn encodeTicket(a: std.mem.Allocator, sql: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try pbPutBytes(&out, a, 1, sql);
    return out.toOwnedSlice(a);
}

pub fn runSql(gpa: std.mem.Allocator, io: std.Io, port: u16, sql: []const u8) !void {
    const client = try Client.connect(gpa, io, port);
    defer client.deinit();
    try client.exec(sql);
}

pub fn queryI64(gpa: std.mem.Allocator, io: std.Io, port: u16, sql: []const u8) ![]i64 {
    return queryI64Token(gpa, io, port, sql, null);
}

pub fn queryI64Token(gpa: std.mem.Allocator, io: std.Io, port: u16, sql: []const u8, token: ?[]const u8) ![]i64 {
    const client = try Client.connect(gpa, io, port);
    client.token = token;
    defer client.deinit();
    return client.queryI64(sql);
}

pub const I64Stream = struct {
    values: []i64,
    n_batches: usize,
};

pub fn queryI64Stream(gpa: std.mem.Allocator, io: std.Io, port: u16, sql: []const u8) !I64Stream {
    const client = try Client.connect(gpa, io, port);
    defer client.deinit();
    return client.collectI64(sql);
}

const Client = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    stream: net.Stream,
    token: ?[]const u8 = null,
    reader_buf: [16 * 1024]u8 = undefined,
    writer_buf: [16 * 1024]u8 = undefined,
    reader: net.Stream.Reader = undefined,
    writer: net.Stream.Writer = undefined,
    conn: h2.Conn = undefined,
    opened: bool = false,

    fn connect(gpa: std.mem.Allocator, io: std.Io, port: u16) !*Client {
        var addr: net.IpAddress = .{ .ip4 = .loopback(port) };
        const stream = try addr.connect(io, .{ .mode = .stream });
        const self = try gpa.create(Client);
        self.* = .{ .gpa = gpa, .io = io, .stream = stream };
        self.reader = self.stream.reader(io, &self.reader_buf);
        self.writer = self.stream.writer(io, &self.writer_buf);
        self.conn = h2.Conn.init(gpa, &self.reader.interface, &self.writer.interface);
        self.opened = true;
        errdefer self.deinit();
        try self.conn.writePreface();
        try self.conn.writeSettings();
        try self.waitSettingsAck();
        return self;
    }

    fn deinit(self: *Client) void {
        if (self.opened) {
            self.conn.deinit();
            self.stream.close(self.io);
            self.opened = false;
        }
        self.gpa.destroy(self);
    }

    fn waitSettingsAck(self: *Client) !void {
        var saw_ack = false;
        var saw_settings = false;
        while (!saw_ack or !saw_settings) {
            const frame = try self.conn.readFrame();
            defer self.gpa.free(frame.payload);
            switch (frame.typ) {
                .settings => {
                    if (frame.flags & h2.Flag.ack != 0) {
                        saw_ack = true;
                    } else {
                        saw_settings = true;
                        try self.conn.writeSettingsAck();
                    }
                },
                .window_update, .ping => {},
                else => {},
            }
        }
    }

    fn exec(self: *Client, sql: []const u8) !void {
        const got = try self.doQuery(sql);
        self.gpa.free(got.header);
        self.gpa.free(got.body);
    }

    fn queryI64(self: *Client, sql: []const u8) ![]i64 {
        const got = try self.collectI64(sql);
        return got.values;
    }

    fn collectI64(self: *Client, sql: []const u8) !I64Stream {
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        const desc = try encodeDescriptor(a, sql);
        const info_stream = self.conn.allocStream();
        const info_path = svc ++ "GetFlightInfo";
        try self.unary(info_stream, info_path, desc);
        const info_msgs = try self.readUnary(a, info_stream);
        if (info_msgs.len == 0) return error.InvalidFlight;
        const ticket_sql = ticketFromInfo(a, info_msgs[0]) orelse sql;

        const ticket = try encodeTicket(a, ticket_sql);
        const get_stream = self.conn.allocStream();
        try self.unary(get_stream, svc ++ "DoGet", ticket);
        const data_msgs = try self.readUnary(a, get_stream);

        var acc: std.ArrayList(i64) = .empty;
        errdefer acc.deinit(self.gpa);
        var n_batches: usize = 0;
        for (data_msgs) |m| {
            const h = pbBytes(m, 2) orelse continue;
            const b = pbBytes(m, 1000) orelse continue;
            const part = try arrow_ipc.firstI64s(self.gpa, h, b);
            defer self.gpa.free(part);
            try acc.appendSlice(self.gpa, part);
            n_batches += 1;
        }
        return .{ .values = try acc.toOwnedSlice(self.gpa), .n_batches = n_batches };
    }

    const Got = struct { header: []u8, body: []u8 };

    fn doQuery(self: *Client, sql: []const u8) !Got {
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        const desc = try encodeDescriptor(a, sql);
        const info_stream = self.conn.allocStream();
        const info_path = svc ++ "GetFlightInfo";
        try self.unary(info_stream, info_path, desc);
        const info_msgs = try self.readUnary(a, info_stream);
        if (info_msgs.len == 0) return error.InvalidFlight;
        const ticket_sql = ticketFromInfo(a, info_msgs[0]) orelse sql;

        const ticket = try encodeTicket(a, ticket_sql);
        const get_stream = self.conn.allocStream();
        try self.unary(get_stream, svc ++ "DoGet", ticket);
        const data_msgs = try self.readUnary(a, get_stream);
        var header: []u8 = &.{};
        var body: []u8 = &.{};
        errdefer {
            if (header.len > 0) self.gpa.free(header);
            if (body.len > 0) self.gpa.free(body);
        }
        for (data_msgs) |m| {
            if (pbBytes(m, 2)) |h| {
                if (pbBytes(m, 1000)) |b| {
                    if (header.len > 0) self.gpa.free(header);
                    if (body.len > 0) self.gpa.free(body);
                    header = try self.gpa.dupe(u8, h);
                    body = try self.gpa.dupe(u8, b);
                } else if (header.len == 0) {
                    header = try self.gpa.dupe(u8, h);
                }
            }
        }
        if (header.len == 0) return error.InvalidIpc;
        return .{ .header = header, .body = body };
    }

    fn unary(self: *Client, stream: u32, path: []const u8, proto: []const u8) !void {
        const auth = if (self.token) |tok|
            try std.fmt.allocPrint(self.gpa, "Bearer {s}", .{tok})
        else
            null;
        defer if (auth) |v| self.gpa.free(v);
        if (auth) |value| {
            try self.conn.writeHeaders(stream, &.{
                .{ .name = ":method", .value = "POST" },
                .{ .name = ":scheme", .value = "http" },
                .{ .name = ":path", .value = path },
                .{ .name = ":authority", .value = "localhost" },
                .{ .name = "content-type", .value = "application/grpc" },
                .{ .name = "te", .value = "trailers" },
                .{ .name = "authorization", .value = value },
            }, false);
        } else {
            try self.conn.writeHeaders(stream, &.{
                .{ .name = ":method", .value = "POST" },
                .{ .name = ":scheme", .value = "http" },
                .{ .name = ":path", .value = path },
                .{ .name = ":authority", .value = "localhost" },
                .{ .name = "content-type", .value = "application/grpc" },
                .{ .name = "te", .value = "trailers" },
            }, false);
        }
        const framed = try h2.grpcFrame(self.gpa, proto);
        defer self.gpa.free(framed);
        try self.conn.writeData(stream, framed, true);
    }

    fn readUnary(self: *Client, a: std.mem.Allocator, stream: u32) ![][]const u8 {
        var buf: std.ArrayList(u8) = .empty;
        var status: []const u8 = "0";
        var ended = false;
        while (!ended) {
            const frame = try self.conn.readFrame();
            defer self.gpa.free(frame.payload);
            if (frame.stream != stream and frame.stream != 0) continue;
            switch (frame.typ) {
                .settings => {
                    if (frame.flags & h2.Flag.ack == 0) try self.conn.writeSettingsAck();
                },
                .ping => {
                    if (frame.flags & h2.Flag.ack == 0)
                        try self.conn.writeFrame(.ping, h2.Flag.ack, 0, frame.payload);
                },
                .window_update => {},
                .data => {
                    if (frame.stream == stream) try buf.appendSlice(a, frame.payload);
                    if (frame.flags & h2.Flag.end_stream != 0) ended = true;
                },
                .headers => {
                    if (frame.stream != stream) continue;
                    var headers: std.ArrayList(h2.Header) = .empty;
                    try self.conn.decodeHeaders(frame.payload, &headers, a);
                    const gs = h2.headerValue(headers.items, "grpc-status");
                    if (gs.len > 0) status = gs;
                    if (frame.flags & h2.Flag.end_stream != 0) ended = true;
                },
                .rst_stream, .goaway => return error.InvalidFlight,
                else => {},
            }
        }
        if (!std.mem.eql(u8, status, "0")) return error.InvalidFlight;
        return grpcSplit(a, buf.items);
    }
};

fn ticketFromInfo(a: std.mem.Allocator, info: []const u8) ?[]u8 {
    const ep = pbBytes(info, 3) orelse return null;
    const ticket = pbBytes(ep, 1) orelse return null;
    const sql = pbBytes(ticket, 1) orelse ticket;
    return a.dupe(u8, sql) catch null;
}

test "Flight SQL SELECT 1 and INSERT visible on second connection" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_flight_serve";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);

    var h = try bind(gpa, io, root, "127.0.0.1:0");
    var fut = try io.concurrent(serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }

    const port = h.port();
    {
        const got = try queryI64(gpa, io, port, "SELECT 1");
        defer gpa.free(got);
        try std.testing.expectEqual(@as(usize, 1), got.len);
        try std.testing.expectEqual(@as(i64, 1), got[0]);
    }
    try runSql(gpa, io, port, "CREATE NAMESPACE sales");
    try runSql(gpa, io, port, "CREATE TABLE sales.orders (id BIGINT, category STRING)");
    try runSql(gpa, io, port, "INSERT INTO sales.orders VALUES (1, 'fruit'), (2, 'veg')");
    {
        const got = try queryI64(gpa, io, port, "SELECT COUNT(*) FROM sales.orders");
        defer gpa.free(got);
        try std.testing.expectEqual(@as(usize, 1), got.len);
        try std.testing.expectEqual(@as(i64, 2), got[0]);
    }
}

test "Flight SQL DoGet streams multiple record batches" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const posix_env = struct {
        extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern "c" fn unsetenv(name: [*:0]const u8) c_int;
    };
    _ = posix_env.setenv("GLACIER_BATCH_ROWS", "2", 1);
    defer _ = posix_env.unsetenv("GLACIER_BATCH_ROWS");

    const root = "/tmp/glacier_flight_stream";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);

    var h = try bind(gpa, io, root, "127.0.0.1:0");
    var fut = try io.concurrent(serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }

    const port = h.port();
    try runSql(gpa, io, port, "CREATE NAMESPACE sales");
    try runSql(gpa, io, port, "CREATE TABLE sales.orders (id BIGINT, category STRING)");
    try runSql(gpa, io, port, "INSERT INTO sales.orders VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
    const got = try queryI64Stream(gpa, io, port, "SELECT id FROM sales.orders ORDER BY id");
    defer gpa.free(got.values);
    try std.testing.expect(got.n_batches >= 2);
    try std.testing.expectEqual(@as(usize, 5), got.values.len);
    try std.testing.expectEqual(@as(i64, 1), got.values[0]);
    try std.testing.expectEqual(@as(i64, 2), got.values[1]);
    try std.testing.expectEqual(@as(i64, 3), got.values[2]);
    try std.testing.expectEqual(@as(i64, 4), got.values[3]);
    try std.testing.expectEqual(@as(i64, 5), got.values[4]);
}

test "public Flight needs TLS and a token" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const root = "/tmp/glacier_flight_door";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);
    try std.testing.expectError(error.TlsRequired, bindDoor(gpa, io, root, "0.0.0.0:9", null, null, "tok", null));
    try std.testing.expectError(error.TokenRequired, bindDoor(gpa, io, root, "0.0.0.0:9", "missing.crt", "missing.key", null, null));
    var dummy = glacier.jwt.probeSet(gpa);
    try std.testing.expectError(error.TlsRequired, bindDoor(gpa, io, root, "0.0.0.0:9", "missing.crt", "missing.key", null, &dummy));
}

test "Flight bearer uses the native GRANT" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_flight_grant";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);
    var owner = try Session.open(gpa, io, root);
    defer owner.close();
    var made = try owner.execute("CREATE NAMESPACE sales");
    made.deinit();
    made = try owner.execute("CREATE TABLE sales.orders (id BIGINT)");
    made.deinit();
    made = try owner.execute("CREATE TABLE sales.hidden (id BIGINT)");
    made.deinit();
    made = try owner.execute("INSERT INTO sales.orders VALUES (7)");
    made.deinit();
    made = try owner.execute("INSERT INTO sales.hidden VALUES (9)");
    made.deinit();
    made = try owner.execute("GRANT SELECT ON TABLE sales.orders TO bearer");
    made.deinit();

    var h = try bindDoor(gpa, io, root, "127.0.0.1:0", null, null, "sekret", null);
    var fut = try io.concurrent(serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }
    const port = h.port();
    try std.testing.expectError(error.InvalidFlight, queryI64Token(gpa, io, port, "SELECT COUNT(*) FROM sales.orders", null));
    try std.testing.expectError(error.InvalidFlight, queryI64Token(gpa, io, port, "SELECT COUNT(*) FROM sales.orders", "nope"));
    try std.testing.expectError(error.InvalidFlight, queryI64Token(gpa, io, port, "SELECT COUNT(*) FROM sales.hidden", "sekret"));
    const got = try queryI64Token(gpa, io, port, "SELECT COUNT(*) FROM sales.orders", "sekret");
    defer gpa.free(got);
    try std.testing.expectEqual(@as(usize, 1), got.len);
    try std.testing.expectEqual(@as(i64, 1), got[0]);
}

test "Flight TLS accepts the HTTP/2 preface" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const cert = "/tmp/glacier-flight-tls.crt";
    const key = "/tmp/glacier-flight-tls.key";
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

    const root = "/tmp/glacier_flight_tls";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);
    var h = try bindDoor(gpa, io, root, "127.0.0.1:0", cert, key, null, null);
    var fut = try io.concurrent(serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }
    const port_text = try std.fmt.allocPrint(gpa, "{d}", .{h.port()});
    defer gpa.free(port_text);
    var py = try std.process.spawn(io, .{
        .argv = &.{
            "python3", "-c",
            \\import socket, ssl, sys
            \\ctx = ssl._create_unverified_context()
            \\raw = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5)
            \\s = ctx.wrap_socket(raw, server_hostname="localhost")
            \\s.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            \\data = s.recv(9)
            \\print(len(data))
            ,
            port_text,
        },
        .stdout = .pipe,
        .stderr = .ignore,
        .stdin = .ignore,
    });
    defer py.kill(io);
    var line_buf: [64]u8 = undefined;
    var stdout_reader = py.stdout.?.readerStreaming(io, &line_buf);
    const line = try stdout_reader.interface.takeDelimiterExclusive('\n');
    try std.testing.expectEqualStrings("9", line);
}

test "Flight JWT sub uses the native GRANT filter" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    if (@import("builtin").cpu.arch == .wasm32) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const ran = std.process.run(gpa, io, .{
        .argv = &.{ "python3", "-c",
            \\import base64, json, pathlib, subprocess
            \\key = "/tmp/glacier-flight-jwt.pem"
            \\jwks_path = "/tmp/glacier-flight-jwt.jwks"
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
            \\msg = "/tmp/glacier-flight-jwt.msg"
            \\sigp = "/tmp/glacier-flight-jwt.sig"
            \\pathlib.Path(msg).write_bytes(signing.encode())
            \\subprocess.check_call(["openssl", "dgst", "-sha256", "-sign", key, "-out", sigp, msg])
            \\print(signing + "." + b64(pathlib.Path(sigp).read_bytes()))
        },
    }) catch return error.SkipZigTest;
    defer gpa.free(ran.stdout);
    defer gpa.free(ran.stderr);
    switch (ran.term) {
        .exited => |code| if (code != 0) return error.SkipZigTest,
        else => return error.SkipZigTest,
    }
    const token = std.mem.trim(u8, ran.stdout, " \r\n");
    var set = glacier.jwt.loadPath(gpa, io, "/tmp/glacier-flight-jwt.jwks", "https://idp.example", "glacier") catch return error.SkipZigTest;
    defer set.deinit();

    const root = "/tmp/glacier_flight_jwt";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);
    var owner = try Session.open(gpa, io, root);
    defer owner.close();
    var made = try owner.execute("CREATE NAMESPACE sales");
    made.deinit();
    made = try owner.execute("CREATE TABLE sales.orders (id BIGINT)");
    made.deinit();
    made = try owner.execute("INSERT INTO sales.orders VALUES (7), (9)");
    made.deinit();
    made = try owner.execute("GRANT SELECT ON TABLE sales.orders TO alice FILTER (id = 7)");
    made.deinit();

    var h = try bindDoor(gpa, io, root, "127.0.0.1:0", null, null, null, &set);
    var fut = try io.concurrent(serveLoop, .{&h});
    defer {
        h.stop();
        _ = fut.cancel(io) catch {};
        h.deinit();
    }
    const port = h.port();
    try std.testing.expectError(error.InvalidFlight, queryI64Token(gpa, io, port, "SELECT COUNT(*) FROM sales.orders", "nope"));
    const got = try queryI64Token(gpa, io, port, "SELECT id FROM sales.orders WHERE 1 = 1", token);
    defer gpa.free(got);
    try std.testing.expectEqual(@as(usize, 1), got.len);
    try std.testing.expectEqual(@as(i64, 7), got[0]);
}
