//! HTTP/2 + HPACK + gRPC framing for Arrow Flight on localhost.
//! Encoder uses literal headers (no Huffman). Decoder accepts Huffman.

const std = @import("std");

pub const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub const FrameType = enum(u8) {
    data = 0,
    headers = 1,
    rst_stream = 3,
    settings = 4,
    ping = 6,
    goaway = 7,
    window_update = 8,
    continuation = 9,
    _,
};

pub const Flag = struct {
    pub const end_stream: u8 = 0x1;
    pub const ack: u8 = 0x1;
    pub const end_headers: u8 = 0x4;
    pub const padded: u8 = 0x8;
    pub const priority: u8 = 0x20;
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Frame = struct {
    typ: FrameType,
    flags: u8,
    stream: u32,
    payload: []u8,
};

pub const Conn = struct {
    gpa: std.mem.Allocator,
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    dyn: std.ArrayList(Header),
    dyn_size: usize = 0,
    dyn_cap: usize = 4096,
    next_stream: u32 = 1,
    scratch: std.ArrayList(u8),

    pub fn init(gpa: std.mem.Allocator, reader: *std.Io.Reader, writer: *std.Io.Writer) Conn {
        return .{
            .gpa = gpa,
            .reader = reader,
            .writer = writer,
            .dyn = .empty,
            .scratch = .empty,
        };
    }

    pub fn deinit(self: *Conn) void {
        for (self.dyn.items) |h| {
            self.gpa.free(h.name);
            self.gpa.free(h.value);
        }
        self.dyn.deinit(self.gpa);
        self.scratch.deinit(self.gpa);
    }

    pub fn writeFrame(self: *Conn, typ: FrameType, flags: u8, stream: u32, payload: []const u8) !void {
        var hdr: [9]u8 = undefined;
        const n: u32 = @intCast(payload.len);
        hdr[0] = @intCast((n >> 16) & 0xff);
        hdr[1] = @intCast((n >> 8) & 0xff);
        hdr[2] = @intCast(n & 0xff);
        hdr[3] = @intFromEnum(typ);
        hdr[4] = flags;
        std.mem.writeInt(u32, hdr[5..9], stream & 0x7fff_ffff, .big);
        try self.writer.writeAll(&hdr);
        if (payload.len > 0) try self.writer.writeAll(payload);
        try self.writer.flush();
    }

    pub fn readFrame(self: *Conn) !Frame {
        const hdr = try self.reader.takeArray(9);
        const n: usize = (@as(usize, hdr[0]) << 16) | (@as(usize, hdr[1]) << 8) | hdr[2];
        const typ: FrameType = @enumFromInt(hdr[3]);
        const flags = hdr[4];
        const stream = std.mem.readInt(u32, hdr[5..9], .big) & 0x7fff_ffff;
        const payload = try self.gpa.alloc(u8, n);
        errdefer self.gpa.free(payload);
        if (n > 0) try self.reader.readSliceAll(payload);
        return .{ .typ = typ, .flags = flags, .stream = stream, .payload = payload };
    }

    pub fn writeSettings(self: *Conn) !void {
        var p: [12]u8 = undefined;
        std.mem.writeInt(u16, p[0..2], 0x4, .big); // INITIAL_WINDOW_SIZE
        std.mem.writeInt(u32, p[2..6], 16 * 1024 * 1024, .big);
        std.mem.writeInt(u16, p[6..8], 0x5, .big); // MAX_FRAME_SIZE
        std.mem.writeInt(u32, p[8..12], 16 * 1024 * 1024, .big);
        try self.writeFrame(.settings, 0, 0, &p);
        try self.writeFrame(.window_update, 0, 0, &windowDelta(16 * 1024 * 1024));
    }

    pub fn writeSettingsAck(self: *Conn) !void {
        try self.writeFrame(.settings, Flag.ack, 0, &.{});
    }

    pub fn writeHeaders(self: *Conn, stream: u32, headers: []const Header, end_stream: bool) !void {
        self.scratch.clearRetainingCapacity();
        try encodeHeaders(self.gpa, &self.scratch, headers);
        var flags: u8 = Flag.end_headers;
        if (end_stream) flags |= Flag.end_stream;
        try self.writeFrame(.headers, flags, stream, self.scratch.items);
    }

    pub fn writeData(self: *Conn, stream: u32, data: []const u8, end_stream: bool) !void {
        try self.writeFrame(.data, if (end_stream) Flag.end_stream else 0, stream, data);
    }

    pub fn writeGrpcMessages(self: *Conn, stream: u32, messages: []const []const u8, trailers_ok: bool) !void {
        try self.writeHeaders(stream, &.{
            .{ .name = ":status", .value = "200" },
            .{ .name = "content-type", .value = "application/grpc" },
        }, false);
        for (messages) |msg| {
            const framed = try grpcFrame(self.gpa, msg);
            defer self.gpa.free(framed);
            try self.writeData(stream, framed, false);
        }
        if (trailers_ok) {
            try self.writeHeaders(stream, &.{
                .{ .name = "grpc-status", .value = "0" },
            }, true);
        }
    }

    pub fn writeGrpcError(self: *Conn, stream: u32, status: []const u8, message: []const u8) !void {
        try self.writeHeaders(stream, &.{
            .{ .name = ":status", .value = "200" },
            .{ .name = "content-type", .value = "application/grpc" },
            .{ .name = "grpc-status", .value = status },
            .{ .name = "grpc-message", .value = message },
        }, true);
    }

    pub fn writePreface(self: *Conn) !void {
        try self.writer.writeAll(preface);
        try self.writer.flush();
    }

    pub fn expectPreface(self: *Conn) !void {
        const got = try self.reader.take(preface.len);
        if (!std.mem.eql(u8, got, preface)) return error.Http2Preface;
    }

    pub fn allocStream(self: *Conn) u32 {
        const id = self.next_stream;
        self.next_stream += 2;
        return id;
    }

    pub fn decodeHeaders(self: *Conn, payload: []const u8, out: *std.ArrayList(Header), arena: std.mem.Allocator) !void {
        var i: usize = 0;
        while (i < payload.len) {
            const b = payload[i];
            if (b & 0x80 != 0) {
                const idx, const n = try decodeInt(payload[i..], 7);
                i += n;
                const h = try self.tableGet(idx, arena);
                try out.append(arena, h);
            } else if (b & 0x40 != 0) {
                const idx, const n = try decodeInt(payload[i..], 6);
                i += n;
                const h = try self.decodeLiteral(payload, &i, idx, arena);
                try out.append(arena, h);
                try self.dynAdd(h, arena);
            } else if (b & 0x20 != 0) {
                const cap, const n = try decodeInt(payload[i..], 5);
                i += n;
                self.dyn_cap = cap;
                self.dynEvict();
            } else {
                const idx, const n = try decodeInt(payload[i..], 4);
                i += n;
                const h = try self.decodeLiteral(payload, &i, idx, arena);
                try out.append(arena, h);
            }
        }
    }

    fn decodeLiteral(self: *Conn, payload: []const u8, i: *usize, idx: usize, arena: std.mem.Allocator) !Header {
        const name = if (idx == 0)
            try decodeString(payload, i, arena)
        else
            (try self.tableGet(idx, arena)).name;
        const value = try decodeString(payload, i, arena);
        return .{ .name = name, .value = value };
    }

    fn tableGet(self: Conn, idx: usize, arena: std.mem.Allocator) !Header {
        if (idx == 0) return error.Hpack;
        if (idx <= static_table.len) {
            const e = static_table[idx - 1];
            return .{ .name = e.name, .value = e.value };
        }
        const di = idx - static_table.len - 1;
        if (di >= self.dyn.items.len) return error.Hpack;
        const e = self.dyn.items[self.dyn.items.len - 1 - di];
        return .{
            .name = try arena.dupe(u8, e.name),
            .value = try arena.dupe(u8, e.value),
        };
    }

    fn dynAdd(self: *Conn, h: Header, _: std.mem.Allocator) !void {
        const name = try self.gpa.dupe(u8, h.name);
        const value = try self.gpa.dupe(u8, h.value);
        try self.dyn.append(self.gpa, .{ .name = name, .value = value });
        self.dyn_size += h.name.len + h.value.len + 32;
        self.dynEvict();
    }

    fn dynEvict(self: *Conn) void {
        while (self.dyn_size > self.dyn_cap and self.dyn.items.len > 0) {
            const old = self.dyn.orderedRemove(0);
            self.dyn_size -= old.name.len + old.value.len + 32;
            self.gpa.free(old.name);
            self.gpa.free(old.value);
        }
    }
};

fn windowDelta(n: u32) [4]u8 {
    var p: [4]u8 = undefined;
    std.mem.writeInt(u32, &p, n, .big);
    return p;
}

pub fn grpcFrame(gpa: std.mem.Allocator, proto: []const u8) ![]u8 {
    const out = try gpa.alloc(u8, 5 + proto.len);
    out[0] = 0;
    std.mem.writeInt(u32, out[1..5], @intCast(proto.len), .big);
    @memcpy(out[5..], proto);
    return out;
}

pub fn pathOf(headers: []const Header) []const u8 {
    for (headers) |h| {
        if (std.mem.eql(u8, h.name, ":path")) return h.value;
    }
    return "";
}

pub fn headerValue(headers: []const Header, name: []const u8) []const u8 {
    for (headers) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
    }
    return "";
}

fn encodeHeaders(gpa: std.mem.Allocator, out: *std.ArrayList(u8), headers: []const Header) !void {
    for (headers) |h| {
        if (std.mem.eql(u8, h.name, ":status") and std.mem.eql(u8, h.value, "200")) {
            try out.append(gpa, 0x80 | 8);
            continue;
        }
        if (std.mem.eql(u8, h.name, ":method") and std.mem.eql(u8, h.value, "POST")) {
            try out.append(gpa, 0x80 | 3);
            continue;
        }
        if (std.mem.eql(u8, h.name, ":scheme") and std.mem.eql(u8, h.value, "http")) {
            try out.append(gpa, 0x80 | 6);
            continue;
        }
        try out.append(gpa, 0x00);
        try encodeString(gpa, out, h.name);
        try encodeString(gpa, out, h.value);
    }
}

fn encodeString(gpa: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    try encodeInt(gpa, out, s.len, 7, 0);
    try out.appendSlice(gpa, s);
}

fn encodeInt(gpa: std.mem.Allocator, out: *std.ArrayList(u8), value: usize, prefix_bits: u3, hi: u8) !void {
    const max: usize = (@as(usize, 1) << prefix_bits) - 1;
    if (value < max) {
        try out.append(gpa, hi | @as(u8, @intCast(value)));
        return;
    }
    try out.append(gpa, hi | @as(u8, @intCast(max)));
    var v = value - max;
    while (v >= 128) {
        try out.append(gpa, @as(u8, @intCast((v & 0x7f) | 0x80)));
        v >>= 7;
    }
    try out.append(gpa, @intCast(v));
}

fn decodeInt(src: []const u8, prefix_bits: u3) !struct { usize, usize } {
    if (src.len == 0) return error.Hpack;
    const max: usize = (@as(usize, 1) << prefix_bits) - 1;
    var i: usize = src[0] & @as(u8, @intCast(max));
    var n: usize = 1;
    if (i < max) return .{ i, n };
    var m: u6 = 0;
    while (n < src.len) {
        const b = src[n];
        n += 1;
        i += @as(usize, b & 0x7f) << m;
        if (b & 0x80 == 0) return .{ i, n };
        m += 7;
        if (m > 28) return error.Hpack;
    }
    return error.Hpack;
}

fn decodeString(src: []const u8, i: *usize, arena: std.mem.Allocator) ![]u8 {
    if (i.* >= src.len) return error.Hpack;
    const huff = src[i.*] & 0x80 != 0;
    const len, const n = try decodeInt(src[i.*..], 7);
    i.* += n;
    if (i.* + len > src.len) return error.Hpack;
    const slice = src[i.* .. i.* + len];
    i.* += len;
    if (huff) return huffmanDecode(arena, slice);
    return arena.dupe(u8, slice);
}

fn huffmanDecode(arena: std.mem.Allocator, src: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(arena);
    var cur: u64 = 0;
    var nbits: u32 = 0;
    for (src) |byte| {
        cur = (cur << 8) | byte;
        nbits += 8;
        decode: while (nbits >= 5) {
            var found = false;
            var sym: usize = 0;
            while (sym < 256) : (sym += 1) {
                const len = huff_len[sym];
                if (len > nbits) continue;
                const shift: u6 = @intCast(nbits - len);
                const mask: u64 = if (len == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(len)) - 1;
                if ((cur >> shift) & mask == huff_code[sym]) {
                    try out.append(arena, @intCast(sym));
                    nbits -= len;
                    if (nbits == 0) cur = 0 else cur &= (@as(u64, 1) << @intCast(nbits)) - 1;
                    found = true;
                    continue :decode;
                }
            }
            if (!found) break;
        }
    }
    return out.toOwnedSlice(arena);
}

const static_table = [_]Header{
    .{ .name = ":authority", .value = "" },
    .{ .name = ":method", .value = "GET" },
    .{ .name = ":method", .value = "POST" },
    .{ .name = ":path", .value = "/" },
    .{ .name = ":path", .value = "/index.html" },
    .{ .name = ":scheme", .value = "http" },
    .{ .name = ":scheme", .value = "https" },
    .{ .name = ":status", .value = "200" },
    .{ .name = ":status", .value = "204" },
    .{ .name = ":status", .value = "206" },
    .{ .name = ":status", .value = "304" },
    .{ .name = ":status", .value = "400" },
    .{ .name = ":status", .value = "404" },
    .{ .name = ":status", .value = "500" },
    .{ .name = "accept-charset", .value = "" },
    .{ .name = "accept-encoding", .value = "gzip, deflate" },
    .{ .name = "accept-language", .value = "" },
    .{ .name = "accept-ranges", .value = "" },
    .{ .name = "accept", .value = "" },
    .{ .name = "access-control-allow-origin", .value = "" },
    .{ .name = "age", .value = "" },
    .{ .name = "allow", .value = "" },
    .{ .name = "authorization", .value = "" },
    .{ .name = "cache-control", .value = "" },
    .{ .name = "content-disposition", .value = "" },
    .{ .name = "content-encoding", .value = "" },
    .{ .name = "content-language", .value = "" },
    .{ .name = "content-length", .value = "" },
    .{ .name = "content-location", .value = "" },
    .{ .name = "content-range", .value = "" },
    .{ .name = "content-type", .value = "" },
    .{ .name = "cookie", .value = "" },
    .{ .name = "date", .value = "" },
    .{ .name = "etag", .value = "" },
    .{ .name = "expect", .value = "" },
    .{ .name = "expires", .value = "" },
    .{ .name = "from", .value = "" },
    .{ .name = "host", .value = "" },
    .{ .name = "if-match", .value = "" },
    .{ .name = "if-modified-since", .value = "" },
    .{ .name = "if-none-match", .value = "" },
    .{ .name = "if-range", .value = "" },
    .{ .name = "if-unmodified-since", .value = "" },
    .{ .name = "last-modified", .value = "" },
    .{ .name = "link", .value = "" },
    .{ .name = "location", .value = "" },
    .{ .name = "max-forwards", .value = "" },
    .{ .name = "proxy-authenticate", .value = "" },
    .{ .name = "proxy-authorization", .value = "" },
    .{ .name = "range", .value = "" },
    .{ .name = "referer", .value = "" },
    .{ .name = "refresh", .value = "" },
    .{ .name = "retry-after", .value = "" },
    .{ .name = "server", .value = "" },
    .{ .name = "set-cookie", .value = "" },
    .{ .name = "strict-transport-security", .value = "" },
    .{ .name = "transfer-encoding", .value = "" },
    .{ .name = "user-agent", .value = "" },
    .{ .name = "vary", .value = "" },
    .{ .name = "via", .value = "" },
    .{ .name = "www-authenticate", .value = "" },
};

const huff_code = [_]u32{
    0x1ff8,      0x7fffd8,    0xfffffe2,   0xfffffe3,   0xfffffe4,   0xfffffe5,   0xfffffe6,   0xfffffe7,
    0xfffffe8,   0xffffea,    0x3ffffffc,  0xfffffe9,   0xfffffea,   0x3ffffffd,  0xfffffeb,   0xfffffec,
    0xfffffed,   0xfffffee,   0xfffffef,   0xffffff0,   0xffffff1,   0xffffff2,   0x3ffffffe,  0xffffff3,
    0xffffff4,   0xffffff5,   0xffffff6,   0xffffff7,   0xffffff8,   0xffffff9,   0xffffffa,   0xffffffb,
    0x14,        0x3f8,       0x3f9,       0xffa,       0x1ff9,      0x15,        0xf8,        0x7fa,
    0x3fa,       0x3fb,       0xf9,        0x7fb,       0xfa,        0x16,        0x17,        0x18,
    0x0,         0x1,         0x2,         0x19,        0x1a,        0x1b,        0x1c,        0x1d,
    0x1e,        0x1f,        0x5c,        0xfb,        0x7ffc,      0x20,        0xffb,       0x3fc,
    0x1ffa,      0x21,        0x5d,        0x5e,        0x5f,        0x60,        0x61,        0x62,
    0x63,        0x64,        0x65,        0x66,        0x67,        0x68,        0x69,        0x6a,
    0x6b,        0x6c,        0x6d,        0x6e,        0x6f,        0x70,        0x71,        0x72,
    0xfc,        0x73,        0xfd,        0x1ffb,      0x7fff0,     0x1ffc,      0x3ffc,      0x22,
    0x7ffd,      0x3,         0x23,        0x4,         0x24,        0x5,         0x25,        0x26,
    0x27,        0x6,         0x74,        0x75,        0x28,        0x29,        0x2a,        0x7,
    0x2b,        0x76,        0x2c,        0x8,         0x9,         0x2d,        0x77,        0x78,
    0x79,        0x7a,        0x7b,        0x7ffe,      0x7fc,       0x3ffd,      0x1ffd,      0xffffffc,
    0xfffe6,     0x3fffd2,    0xfffe7,     0xfffe8,     0x3fffd3,    0x3fffd4,    0x3fffd5,    0x7fffd9,
    0x3fffd6,    0x7fffda,    0x7fffdb,    0x7fffdc,    0x7fffdd,    0x7fffde,    0xffffeb,    0x7fffdf,
    0xffffec,    0xffffed,    0x3fffd7,    0x7fffe0,    0xffffee,    0x7fffe1,    0x7fffe2,    0x7fffe3,
    0x7fffe4,    0x1fffdc,    0x3fffd8,    0x7fffe5,    0x3fffd9,    0x7fffe6,    0x7fffe7,    0xffffef,
    0x3fffda,    0x1fffdd,    0xfffe9,     0x3fffdb,    0x3fffdc,    0x7fffe8,    0x7fffe9,    0x1fffde,
    0x7fffea,    0x3fffdd,    0x3fffde,    0xfffff0,    0x1fffdf,    0x3fffdf,    0x7fffeb,    0x7fffec,
    0x1fffe0,    0x1fffe1,    0x3fffe0,    0x1fffe2,    0x7fffed,    0x3fffe1,    0x7fffee,    0x7fffef,
    0xfffea,     0x3fffe2,    0x3fffe3,    0x3fffe4,    0x7ffff0,    0x3fffe5,    0x3fffe6,    0x7ffff1,
    0x3ffffe0,   0x3ffffe1,   0xfffeb,     0x7fff1,     0x3fffe7,    0x7ffff2,    0x3fffe8,    0x1ffffec,
    0x3ffffe2,   0x3ffffe3,   0x3ffffe4,   0x7ffffde,   0x7ffffdf,   0x3ffffe5,   0xfffff1,    0x1ffffed,
    0x7fff2,     0x1fffe3,    0x3ffffe6,   0x7ffffe0,   0x7ffffe1,   0x3ffffe7,   0x7ffffe2,   0xfffff2,
    0x1fffe4,    0x1fffe5,    0x3ffffe8,   0x3ffffe9,   0xffffffd,   0x7ffffe3,   0x7ffffe4,   0x7ffffe5,
    0xfffec,     0xfffff3,    0xfffed,     0x1fffe6,    0x3fffe9,    0x1fffe7,    0x1fffe8,    0x7ffff3,
    0x3fffea,    0x3fffeb,    0x1ffffee,   0x1ffffef,   0xfffff4,    0xfffff5,    0x3ffffea,   0x7ffff4,
    0x3ffffeb,   0x7ffffe6,   0x3ffffec,   0x3ffffed,   0x7ffffe7,   0x7ffffe8,   0x7ffffe9,   0x7ffffea,
    0x7ffffeb,   0xffffffe,   0x7ffffec,   0x7ffffed,   0x7ffffee,   0x7ffffef,   0x7fffff0,   0x3ffffee,
};

const huff_len = [_]u8{
    13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28,
    28, 28, 28, 28, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 28,
    6,  10, 10, 12, 13, 6,  8,  11, 10, 10, 8,  11, 8,  6,  6,  6,
    5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  7,  8,  15, 6,  12, 10,
    13, 6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  8,  7,  8,  13, 19, 13, 14, 6,
    15, 5,  6,  5,  6,  5,  6,  6,  6,  5,  7,  7,  6,  6,  6,  5,
    6,  7,  6,  5,  5,  6,  7,  7,  7,  7,  7,  15, 11, 14, 13, 28,
    20, 22, 20, 20, 22, 22, 22, 23, 22, 23, 23, 23, 23, 23, 24, 23,
    24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24,
    22, 21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23,
    21, 21, 22, 21, 23, 22, 23, 23, 20, 22, 22, 22, 23, 22, 22, 23,
    26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25,
    19, 21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27,
    20, 24, 20, 21, 22, 21, 21, 23, 22, 22, 25, 25, 24, 24, 26, 23,
    26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26,
};

test "hpack literal roundtrip" {
    const gpa = std.testing.allocator;
    var encoded: std.ArrayList(u8) = .empty;
    defer encoded.deinit(gpa);
    try encodeHeaders(gpa, &encoded, &.{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/arrow.flight.protocol.FlightService/DoGet" },
        .{ .name = "content-type", .value = "application/grpc" },
    });
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var dummy_r = std.Io.Reader.fixed(&.{});
    var dummy_w_buf: [1]u8 = undefined;
    var dummy_w = std.Io.Writer.fixed(&dummy_w_buf);
    var conn = Conn.init(gpa, &dummy_r, &dummy_w);
    defer conn.deinit();
    var headers: std.ArrayList(Header) = .empty;
    try conn.decodeHeaders(encoded.items, &headers, arena.allocator());
    try std.testing.expectEqualStrings("POST", headerValue(headers.items, ":method"));
    try std.testing.expectEqualStrings("/arrow.flight.protocol.FlightService/DoGet", headerValue(headers.items, ":path"));
    try std.testing.expectEqualStrings("application/grpc", headerValue(headers.items, "content-type"));
}
