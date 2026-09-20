//! wasm32 stub: no Flight listen.

const std = @import("std");

pub const Handle = struct {
    pub fn port(_: Handle) u16 {
        return 0;
    }
    pub fn stop(_: *Handle) void {}
    pub fn deinit(_: *Handle) void {}
};

pub fn run(
    _: std.mem.Allocator,
    _: std.Io,
    _: []const u8,
    _: []const u8,
    _: []const u8,
    _: *std.Io.Writer,
) !void {
    return error.WriteUnsupported;
}

pub fn bind(
    _: std.mem.Allocator,
    _: std.Io,
    _: []const u8,
    _: []const u8,
) !Handle {
    return error.WriteUnsupported;
}

pub fn serveLoop(_: *Handle) !void {
    return error.WriteUnsupported;
}

pub fn runSql(
    _: std.mem.Allocator,
    _: std.Io,
    _: u16,
    _: []const u8,
) !void {
    return error.WriteUnsupported;
}

pub fn queryI64(
    _: std.mem.Allocator,
    _: std.Io,
    _: u16,
    _: []const u8,
) ![]i64 {
    return error.WriteUnsupported;
}
