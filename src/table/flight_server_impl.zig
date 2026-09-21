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

const svc = "/arrow.flight.protocol.FlightService/";
const sql_query_url = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery";

pub const Handle = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []u8,
    server: net.Server,
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
    }
};

pub fn bind(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
) !Handle {
    var addr = try rest_server.parseListenPort(listen, 8815);
    try glacier_catalog.ensure(gpa, io, warehouse);
    var server = try addr.listen(io, .{ .reuse_address = true });
    errdefer server.deinit(io);
    return .{
        .gpa = gpa,
        .io = io,
        .warehouse = try gpa.dupe(u8, warehouse),
        .server = server,
    };
}

pub fn run(
    gpa: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    listen: []const u8,
    flight: []const u8,
    out: *std.Io.Writer,
) !void {
    var rest = try rest_server.bind(gpa, io, warehouse, listen);
    defer rest.deinit();
    var fl = try bind(gpa, io, warehouse, flight);
    defer fl.deinit();
    try out.print("Iceberg REST  http://{f}\n", .{rest.server.socket.address});
    try out.print("Arrow Flight  grpc://{f}\n", .{fl.server.socket.address});
    try out.print("warehouse     {s}\n", .{warehouse});
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
    const io = self.io;
    defer {
        var copy = stream;
        copy.close(io);
    }
    var send_buffer: [16 * 1024]u8 = undefined;
    var recv_buffer: [16 * 1024]u8 = undefined;
    var connection_reader = stream.reader(io, &recv_buffer);
    var connection_writer = stream.writer(io, &send_buffer);
    var conn = h2.Conn.init(self.gpa, &connection_reader.interface, &connection_writer.interface);
    defer conn.deinit();
    conn.expectPreface() catch return;
    conn.writeSettings() catch return;

    var sess = Session.open(self.gpa, io, self.warehouse) catch return;
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

    fn deinit(self: *StreamBuf, gpa: std.mem.Allocator) void {
        if (self.path.len > 0) gpa.free(self.path);
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
    if (!gop.found_existing) gop.value_ptr.* = .{};

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
    } else {
        try gop.value_ptr.data.appendSlice(self.gpa, payload);
    }

    if (frame.flags & h2.Flag.end_stream == 0) return;

    const path = gop.value_ptr.path;
    const body = gop.value_ptr.data.items;
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
    gop.value_ptr.data.deinit(self.gpa);
    _ = streams.remove(frame.stream);
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
    const client = try Client.connect(gpa, io, port);
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
        try self.conn.writeHeaders(stream, &.{
            .{ .name = ":method", .value = "POST" },
            .{ .name = ":scheme", .value = "http" },
            .{ .name = ":path", .value = path },
            .{ .name = ":authority", .value = "localhost" },
            .{ .name = "content-type", .value = "application/grpc" },
            .{ .name = "te", .value = "trailers" },
        }, false);
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
