//! Iceberg Hadoop warehouse: `root/namespace/table/metadata/…`.
//! A local root is a directory walk. `s3://`, `gs://`, and `az://` list one level of prefixes.

const std = @import("std");
const iceberg = @import("iceberg.zig");
const vfs = @import("../vfs/source.zig");
const Transport = vfs.Transport;

pub const Ident = struct {
    namespace: []const u8,
    name: []const u8,
};

fn isRemote(path: []const u8) bool {
    return std.mem.indexOf(u8, path, "://") != null;
}

fn keepChild(name: []const u8) bool {
    if (name.len == 0 or name[0] == '.') return false;
    if (std.ascii.eqlIgnoreCase(name, "metadata") or std.ascii.eqlIgnoreCase(name, "data")) return false;
    return true;
}

fn listChildDirs(allocator: std.mem.Allocator, t: Transport, path: []const u8) ![][]const u8 {
    if (isRemote(path)) {
        const raw = try vfs.listPrefixNames(t, allocator, path);
        var names: std.ArrayList([]const u8) = .empty;
        for (raw) |name| {
            if (!keepChild(name)) {
                allocator.free(name);
                continue;
            }
            try names.append(allocator, name);
        }
        return names.items;
    }
    var dir = std.Io.Dir.cwd().openDir(t.io, path, .{ .iterate = true }) catch return &.{};
    defer dir.close(t.io);
    var names: std.ArrayList([]const u8) = .empty;
    var it = dir.iterate();
    while (try it.next(t.io)) |entry| {
        if (!keepChild(entry.name)) continue;
        switch (entry.kind) {
            .directory, .sym_link => try names.append(allocator, try allocator.dupe(u8, entry.name)),
            else => {},
        }
    }
    return names.items;
}

pub fn tablePath(allocator: std.mem.Allocator, warehouse: []const u8, namespace: []const u8, name: []const u8) ![]u8 {
    return vfs.joinLocation(allocator, warehouse, try std.fmt.allocPrint(allocator, "{s}/{s}", .{ namespace, name }));
}

pub fn isWarehouse(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8) bool {
    if (iceberg.isTableDir(allocator, t, warehouse)) return false;
    const nss = listNamespaces(allocator, t, warehouse) catch return false;
    return nss.len > 0;
}

pub fn listNamespaces(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8) ![][]const u8 {
    const children = try listChildDirs(allocator, t, warehouse);
    var names: std.ArrayList([]const u8) = .empty;
    for (children) |ns| {
        const tables = listTables(allocator, t, warehouse, ns) catch continue;
        if (tables.len > 0) try names.append(allocator, ns);
    }
    return names.items;
}

pub fn listTables(allocator: std.mem.Allocator, t: Transport, warehouse: []const u8, namespace: []const u8) ![]Ident {
    const ns_path = try vfs.joinLocation(allocator, warehouse, namespace);
    const children = try listChildDirs(allocator, t, ns_path);
    var out: std.ArrayList(Ident) = .empty;
    for (children) |name| {
        const dir = try vfs.joinLocation(allocator, ns_path, name);
        if (!iceberg.isTableDir(allocator, t, dir)) continue;
        try out.append(allocator, .{ .namespace = namespace, .name = name });
    }
    return out.items;
}

test "Hadoop warehouse lists two namespaces" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var da: std.heap.DebugAllocator(.{}) = .init;
    defer _ = da.deinit();
    const gpa = da.allocator();
    var threaded = std.Io.Threaded.init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const root = "/tmp/glacier_hadoop_list";
    try iceberg.writeWarehouseFixture(gpa, io, root);

    var http: std.http.Client = .{ .allocator = gpa, .io = io };
    defer http.deinit();
    const t = Transport{ .allocator = gpa, .io = io, .http = &http };
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    try std.testing.expect(isWarehouse(a, t, root));
    try std.testing.expect(!iceberg.isTableDir(a, t, root));
    const nss = try listNamespaces(a, t, root);
    try std.testing.expectEqual(@as(usize, 2), nss.len);
    const tables = try listTables(a, t, root, "sales");
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqualStrings("prune", tables[0].name);
}
