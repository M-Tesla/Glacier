//! Native Glacier catalog: a warehouse directory plus `_glacier_catalog.json`.
//! Tables are Iceberg. The JSON is the name registry (namespace / table / location).

const std = @import("std");
const vfs = @import("../vfs/source.zig");
const Transport = vfs.Transport;

pub const Ident = struct {
    namespace: []const u8,
    name: []const u8,
    location: []const u8 = "",
};

pub const Registry = struct {
    namespaces: [][]const u8 = &.{},
    tables: []Ident = &.{},
};

fn isRemote(path: []const u8) bool {
    return std.mem.indexOf(u8, path, "://") != null;
}

pub fn catalogFile(allocator: std.mem.Allocator, warehouse: []const u8) ![]u8 {
    return vfs.joinLocation(allocator, warehouse, "_glacier_catalog.json");
}

pub fn isCatalog(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8) bool {
    const path = catalogFile(allocator, warehouse) catch return false;
    defer allocator.free(path);
    var source = vfs.FileSource.openLocation(t, path) catch return false;
    source.close();
    return true;
}

pub fn isEmptyDir(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8) bool {
    if (isRemote(warehouse)) return false;
    _ = allocator;
    var dir = std.Io.Dir.cwd().openDir(t.io, warehouse, .{ .iterate = true }) catch return false;
    defer dir.close(t.io);
    var it = dir.iterate();
    while (it.next(t.io) catch return false) |entry| {
        if (entry.name.len == 0 or entry.name[0] == '.') continue;
        return false;
    }
    return true;
}

pub fn ensure(allocator: std.mem.Allocator, io: std.Io, warehouse: []const u8) !void {
    if (isRemote(warehouse)) return error.UnsupportedSql;
    try std.Io.Dir.cwd().createDirPath(io, warehouse);
    var http: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = allocator, .io = io, .http = &http };
    if (isCatalog(allocator, t, warehouse)) return;
    try save(allocator, io, warehouse, .{});
}

pub fn load(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8) !Registry {
    const path = try catalogFile(allocator, warehouse);
    defer allocator.free(path);
    var source = vfs.FileSource.openLocation(t, path) catch return .{};
    defer source.close();
    const json_text = try source.readAll(allocator);
    return parseRegistry(allocator, json_text);
}

fn parseRegistry(allocator: std.mem.Allocator, json_text: []const u8) !Registry {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_text, .{ .allocate = .alloc_always }) catch return .{};
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |o| o,
        else => return .{},
    };
    var namespaces: std.ArrayList([]const u8) = .empty;
    if (root.get("namespaces")) |nv| {
        switch (nv) {
            .array => |arr| {
                for (arr.items) |item| {
                    switch (item) {
                        .string => |s| try namespaces.append(allocator, try allocator.dupe(u8, s)),
                        else => {},
                    }
                }
            },
            else => {},
        }
    }
    var tables: std.ArrayList(Ident) = .empty;
    if (root.get("tables")) |tv| {
        switch (tv) {
            .array => |arr| {
                for (arr.items) |item| {
                    const obj = switch (item) {
                        .object => |o| o,
                        else => continue,
                    };
                    const ns = switch (obj.get("namespace") orelse continue) {
                        .string => |s| s,
                        else => continue,
                    };
                    const name = switch (obj.get("name") orelse continue) {
                        .string => |s| s,
                        else => continue,
                    };
                    const location: []const u8 = if (obj.get("location")) |lv| switch (lv) {
                        .string => |s| s,
                        else => "",
                    } else "";
                    try tables.append(allocator, .{
                        .namespace = try allocator.dupe(u8, ns),
                        .name = try allocator.dupe(u8, name),
                        .location = try allocator.dupe(u8, location),
                    });
                }
            },
            else => {},
        }
    }
    return .{ .namespaces = namespaces.items, .tables = tables.items };
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

pub fn save(allocator: std.mem.Allocator, io: std.Io, warehouse: []const u8, reg: Registry) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "{\n  \"namespaces\": [");
    for (reg.namespaces, 0..) |ns, i| {
        if (i > 0) try buf.appendSlice(allocator, ", ");
        try appendJsonStr(&buf, allocator, ns);
    }
    try buf.appendSlice(allocator, "],\n  \"tables\": [");
    for (reg.tables, 0..) |tbl, i| {
        if (i > 0) try buf.appendSlice(allocator, ", ");
        try buf.appendSlice(allocator, "\n    {\"namespace\": ");
        try appendJsonStr(&buf, allocator, tbl.namespace);
        try buf.appendSlice(allocator, ", \"name\": ");
        try appendJsonStr(&buf, allocator, tbl.name);
        try buf.appendSlice(allocator, ", \"location\": ");
        try appendJsonStr(&buf, allocator, tbl.location);
        try buf.append(allocator, '}');
    }
    if (reg.tables.len > 0) try buf.appendSlice(allocator, "\n  ");
    try buf.appendSlice(allocator, "]\n}\n");
    const path = try catalogFile(allocator, warehouse);
    defer allocator.free(path);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    var wbuf: [1024]u8 = undefined;
    var writer = file.writer(io, &wbuf);
    try writer.interface.writeAll(buf.items);
    try writer.interface.flush();
}

pub fn listNamespaces(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8) ![][]const u8 {
    const reg = try load(allocator, t, warehouse);
    return reg.namespaces;
}

pub fn listTables(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8, namespace: []const u8) ![]Ident {
    const reg = try load(allocator, t, warehouse);
    var out: std.ArrayList(Ident) = .empty;
    for (reg.tables) |tbl| {
        if (std.ascii.eqlIgnoreCase(tbl.namespace, namespace)) try out.append(allocator, tbl);
    }
    return out.items;
}

pub fn tablePath(
    allocator: std.mem.Allocator,
    t: Transport,
    warehouse: []const u8,
    namespace: []const u8,
    name: []const u8,
) ![]u8 {
    const reg = load(allocator, t, warehouse) catch return defaultTablePath(allocator, warehouse, namespace, name);
    for (reg.tables) |tbl| {
        if (std.ascii.eqlIgnoreCase(tbl.namespace, namespace) and std.ascii.eqlIgnoreCase(tbl.name, name)) {
            if (tbl.location.len > 0) return allocator.dupe(u8, tbl.location);
        }
    }
    return defaultTablePath(allocator, warehouse, namespace, name);
}

pub fn defaultTablePath(allocator: std.mem.Allocator, warehouse: []const u8, namespace: []const u8, name: []const u8) ![]u8 {
    return vfs.joinLocation(allocator, warehouse, try std.fmt.allocPrint(allocator, "{s}/{s}", .{ namespace, name }));
}

pub fn addNamespace(allocator: std.mem.Allocator, io: std.Io, warehouse: []const u8, namespace: []const u8) !void {
    var http: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = allocator, .io = io, .http = &http };
    var reg = try load(allocator, t, warehouse);
    for (reg.namespaces) |ns| {
        if (std.ascii.eqlIgnoreCase(ns, namespace)) return;
    }
    const nss = try allocator.alloc([]const u8, reg.namespaces.len + 1);
    @memcpy(nss[0..reg.namespaces.len], reg.namespaces);
    nss[reg.namespaces.len] = try allocator.dupe(u8, namespace);
    reg.namespaces = nss;
    try save(allocator, io, warehouse, reg);
    const dir = try vfs.joinLocation(allocator, warehouse, namespace);
    std.Io.Dir.cwd().createDirPath(io, dir) catch {};
}

pub fn addTable(
    allocator: std.mem.Allocator,
    io: std.Io,
    warehouse: []const u8,
    namespace: []const u8,
    name: []const u8,
    location: []const u8,
) !void {
    try addNamespace(allocator, io, warehouse, namespace);
    var http: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = allocator, .io = io, .http = &http };
    var reg = try load(allocator, t, warehouse);
    for (reg.tables) |tbl| {
        if (std.ascii.eqlIgnoreCase(tbl.namespace, namespace) and std.ascii.eqlIgnoreCase(tbl.name, name))
            return error.InvalidSyntax;
    }
    const tables = try allocator.alloc(Ident, reg.tables.len + 1);
    @memcpy(tables[0..reg.tables.len], reg.tables);
    tables[reg.tables.len] = .{
        .namespace = try allocator.dupe(u8, namespace),
        .name = try allocator.dupe(u8, name),
        .location = try allocator.dupe(u8, location),
    };
    reg.tables = tables;
    try save(allocator, io, warehouse, reg);
}

pub fn dropTable(allocator: std.mem.Allocator, io: std.Io, warehouse: []const u8, namespace: []const u8, name: []const u8) !Ident {
    var http: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = allocator, .io = io, .http = &http };
    const reg = try load(allocator, t, warehouse);
    var keep: std.ArrayList(Ident) = .empty;
    var found: ?Ident = null;
    for (reg.tables) |tbl| {
        if (std.ascii.eqlIgnoreCase(tbl.namespace, namespace) and std.ascii.eqlIgnoreCase(tbl.name, name)) {
            found = tbl;
            continue;
        }
        try keep.append(allocator, tbl);
    }
    const hit = found orelse return error.TableNotFound;
    var next = reg;
    next.tables = keep.items;
    try save(allocator, io, warehouse, next);
    return hit;
}

test "native catalog registry roundtrip" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_native_registry";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var http: std.http.Client = .{ .allocator = gpa, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = gpa, .io = io, .http = &http };
    try ensure(a, io, root);

    try addNamespace(a, io, root, "sales");
    try addTable(a, io, root, "sales", "orders", "/tmp/glacier_native_registry/sales/orders");
    const nss = try listNamespaces(a, t, root);
    try std.testing.expectEqual(@as(usize, 1), nss.len);
    try std.testing.expectEqualStrings("sales", nss[0]);
    const tables = try listTables(a, t, root, "sales");
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings("orders", tables[0].name);
    const dropped = try dropTable(a, io, root, "sales", "orders");
    try std.testing.expectEqualStrings("orders", dropped.name);
    try std.testing.expectEqual(@as(usize, 0), (try listTables(a, t, root, "sales")).len);
}
