//! Privileges on the native Glacier catalog. One principal, `bearer`,
//! which is the identity of `glacier serve --token`. The file is
//! `_glacier_grants.json` next to the registry. A local session does
//! not consult it.

const std = @import("std");
const vfs = @import("../vfs/source.zig");
const Transport = vfs.Transport;

pub const principal = "bearer";

pub const Privilege = enum { select, all };

pub const Grant = struct {
    namespace: []const u8,
    name: []const u8,
    privilege: Privilege,
    principal: []const u8,
};

pub fn filePath(allocator: std.mem.Allocator, warehouse: []const u8) ![]u8 {
    return vfs.joinLocation(allocator, warehouse, "_glacier_grants.json");
}

pub fn load(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8) ![]Grant {
    const path = filePath(allocator, warehouse) catch return &.{};
    defer allocator.free(path);
    var source = vfs.FileSource.openLocation(t, path) catch return &.{};
    defer source.close();
    const json_text = source.readAll(allocator) catch return &.{};
    return parse(allocator, json_text);
}

pub fn grant(
    allocator: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    namespace: []const u8,
    name: []const u8,
    privilege: Privilege,
    who: []const u8,
) !void {
    var http: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = allocator, .io = io, .http = &http };
    const cur = try load(allocator, t, warehouse);
    for (cur) |g| {
        if (same(g, namespace, name, privilege, who)) return;
    }
    const next = try allocator.alloc(Grant, cur.len + 1);
    @memcpy(next[0..cur.len], cur);
    next[cur.len] = .{
        .namespace = try allocator.dupe(u8, namespace),
        .name = try allocator.dupe(u8, name),
        .privilege = privilege,
        .principal = try allocator.dupe(u8, who),
    };
    try save(allocator, io, warehouse, next);
}

pub fn revoke(
    allocator: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    namespace: []const u8,
    name: []const u8,
    privilege: Privilege,
    who: []const u8,
) !void {
    var http: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = allocator, .io = io, .http = &http };
    const cur = try load(allocator, t, warehouse);
    var keep: std.ArrayList(Grant) = .empty;
    for (cur) |g| {
        if (same(g, namespace, name, privilege, who)) continue;
        try keep.append(allocator, g);
    }
    try save(allocator, io, warehouse, keep.items);
}

pub fn allows(items: []const Grant, namespace: []const u8, name: []const u8, write: bool) bool {
    for (items) |g| {
        if (!std.ascii.eqlIgnoreCase(g.principal, principal)) continue;
        if (!std.ascii.eqlIgnoreCase(g.namespace, namespace)) continue;
        if (!std.ascii.eqlIgnoreCase(g.name, name)) continue;
        if (g.privilege == .all) return true;
        if (!write and g.privilege == .select) return true;
    }
    return false;
}

pub fn namespaceVisible(items: []const Grant, namespace: []const u8) bool {
    for (items) |g| {
        if (!std.ascii.eqlIgnoreCase(g.principal, principal)) continue;
        if (std.ascii.eqlIgnoreCase(g.namespace, namespace)) return true;
    }
    return false;
}

fn same(g: Grant, namespace: []const u8, name: []const u8, privilege: Privilege, who: []const u8) bool {
    return std.ascii.eqlIgnoreCase(g.namespace, namespace) and
        std.ascii.eqlIgnoreCase(g.name, name) and
        g.privilege == privilege and
        std.ascii.eqlIgnoreCase(g.principal, who);
}

fn parse(allocator: std.mem.Allocator, json_text: []const u8) ![]Grant {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_text, .{ .allocate = .alloc_always }) catch return &.{};
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return &.{},
    };
    const arr = switch (root.get("grants") orelse return &.{}) {
        .array => |a| a,
        else => return &.{},
    };
    var out: std.ArrayList(Grant) = .empty;
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |o| o,
            else => continue,
        };
        const ns = switch (obj.get("namespace") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const name = switch (obj.get("table") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const priv_s = switch (obj.get("privilege") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const who = switch (obj.get("principal") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const privilege: Privilege = if (std.ascii.eqlIgnoreCase(priv_s, "all"))
            .all
        else if (std.ascii.eqlIgnoreCase(priv_s, "select"))
            .select
        else
            continue;
        try out.append(allocator, .{
            .namespace = try allocator.dupe(u8, ns),
            .name = try allocator.dupe(u8, name),
            .privilege = privilege,
            .principal = try allocator.dupe(u8, who),
        });
    }
    return out.items;
}

fn save(allocator: std.mem.Allocator, io: std.Io, warehouse: []const u8, items: []const Grant) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "{\"grants\":[");
    for (items, 0..) |g, i| {
        if (i > 0) try buf.append(allocator, ',');
        try buf.appendSlice(allocator, "{\"namespace\":");
        try appendJsonStr(&buf, allocator, g.namespace);
        try buf.appendSlice(allocator, ",\"table\":");
        try appendJsonStr(&buf, allocator, g.name);
        try buf.appendSlice(allocator, ",\"privilege\":");
        try appendJsonStr(&buf, allocator, if (g.privilege == .all) "all" else "select");
        try buf.appendSlice(allocator, ",\"principal\":");
        try appendJsonStr(&buf, allocator, g.principal);
        try buf.append(allocator, '}');
    }
    try buf.appendSlice(allocator, "]}");
    const path = try filePath(allocator, warehouse);
    defer allocator.free(path);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    var wbuf: [1024]u8 = undefined;
    var writer = file.writer(io, &wbuf);
    try writer.interface.writeAll(buf.items);
    try writer.interface.flush();
}

fn appendJsonStr(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            else => try buf.append(allocator, c),
        }
    }
    try buf.append(allocator, '"');
}

test "grant file roundtrip" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const root = "/tmp/glacier_grants_file";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(io, root);
    var http: std.http.Client = .{ .allocator = gpa, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = gpa, .io = io, .http = &http };
    try grant(gpa, io, root, "sales", "orders", .select, principal);
    try grant(gpa, io, root, "sales", "orders", .select, principal);
    try grant(gpa, io, root, "sales", "orders", .all, principal);
    const loaded = try load(gpa, t, root);
    try std.testing.expectEqual(@as(usize, 2), loaded.len);
    try std.testing.expect(allows(loaded, "sales", "orders", false));
    try std.testing.expect(allows(loaded, "sales", "orders", true));
    try std.testing.expect(!allows(loaded, "sales", "other", false));
    try revoke(gpa, io, root, "sales", "orders", .all, principal);
    const left = try load(gpa, t, root);
    try std.testing.expect(allows(left, "sales", "orders", false));
    try std.testing.expect(!allows(left, "sales", "orders", true));
}
