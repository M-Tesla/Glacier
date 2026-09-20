//! Glacier CLI. Session client. No Parquet/Iceberg decode here.

const std = @import("std");
const builtin = @import("builtin");
const glacier = @import("glacier");
const flight_server = @import("flight_server");

const usage_text =
    \\usage: glacier [options] [path]
    \\
    \\  path              parquet, avro, .glacier, iceberg dir, Hadoop warehouse,
    \\                    s3://, gs://, http(s):// file, or Iceberg REST URI
    \\                    with --catalog: default table (namespace.table)
    \\                    omitted: TUI source picker (local / S3 / HTTP / empty)
    \\  --catalog URL     Iceberg REST Catalog
    \\  --warehouse NAME  warehouse query for GET /v1/config
    \\  --token TOKEN     bearer token (or ICEBERG_TOKEN)
    \\  --header 'N: v'   extra catalog header (repeatable)
    \\  --auth MODE       auto, none, bearer, oauth2, sigv4 (default auto)
    \\  --oauth-client-id / --oauth-client-secret / --oauth-server / --oauth-scope
    \\  --sigv4-service   glue (default) or s3tables
    \\  --namespace NS    default namespace (default)
    \\  -c, --command SQL run SQL and exit (repeatable)
    \\                    SHOW / ATTACH / DESCRIBE / USE / DETACH work as catalog SQL
    \\                    CREATE / INSERT / COPY FROM / DELETE / UPDATE / MERGE / ALTER ADD COLUMN / DROP / PARTITIONED BY identity on glacier, Hadoop, REST
    \\                    FROM glacier.catalogs / glacier.tables / glacier.snapshots / glacier.files
    \\  --timer           print wall time + peak RSS after each query (default)
    \\  --no-timer        hide stats
    \\  --no-tui          line REPL instead of full-screen TUI
    \\  serve [warehouse] Iceberg REST Catalog on 127.0.0.1:8181 and Arrow Flight SQL
    \\                    on 127.0.0.1:8815 (native catalog; not in WASM or the wheel)
    \\  --listen HOST:PORT  REST bind (localhost only without TLS)
    \\  --flight HOST:PORT  Flight SQL bind, grpc:// (localhost only without TLS)

    \\  -h, --help        this help
    \\  -v, --version     print version
    \\
    \\  Interactive (no -c): pick a source, then type SQL. glacier PATH skips
    \\  the picker. Ctrl+O opens the picker again. --no-tui for a line prompt.
    \\  Tab expands/folds the selected cell; arrows move and scroll wrapped text.
    \\
;

const Args = struct {
    path: ?[]const u8 = null,
    catalog: ?[]const u8 = null,
    warehouse: []const u8 = "",
    token: ?[]const u8 = null,
    headers: []const []const u8 = &.{},
    auth: glacier.rest_catalog.Auth = .auto,
    oauth_client_id: ?[]const u8 = null,
    oauth_client_secret: ?[]const u8 = null,
    oauth_server: ?[]const u8 = null,
    oauth_scope: ?[]const u8 = null,
    sigv4_service: []const u8 = "glue",
    namespace: []const u8 = "default",
    commands: []const []const u8 = &.{},
    timer: bool = true,
    tui: bool = true,
    help: bool = false,
    version: bool = false,
    serve: bool = false,
    listen: []const u8 = "127.0.0.1:8181",
    flight: []const u8 = "127.0.0.1:8815",
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var stdout_buf: [16384]u8 = undefined;
    var stdout_file = std.Io.File.stdout().writerStreaming(io, &stdout_buf);
    const out = &stdout_file.interface;

    var raw_args: std.ArrayList([]const u8) = .empty;
    defer raw_args.deinit(gpa);
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.next();
    while (it.next()) |a| try raw_args.append(gpa, a);

    var cmd_store: std.ArrayList([]const u8) = .empty;
    defer cmd_store.deinit(gpa);
    var header_store: std.ArrayList([]const u8) = .empty;
    defer header_store.deinit(gpa);
    const args = parseArgs(raw_args.items, &cmd_store, &header_store, gpa) catch |err| {
        switch (err) {
            error.MissingCommand => try out.print("glacier: -c needs a SQL string\n{s}", .{usage_text}),
            error.MissingOptionValue => try out.print("glacier: option needs a value\n{s}", .{usage_text}),
            error.UnknownOption => try out.print("glacier: unknown option\n{s}", .{usage_text}),
            error.ExtraPath => try out.print("glacier: extra path\n{s}", .{usage_text}),
            else => return err,
        }
        die(out);
    };

    if (args.help) {
        try out.writeAll(usage_text);
        try out.flush();
        return;
    }
    if (args.version) {
        try out.print("glacier {s}\n", .{glacier.version});
        try out.flush();
        return;
    }
    if (args.serve) {
        const warehouse = args.path orelse ".";
        flight_server.run(gpa, io, warehouse, args.listen, args.flight, out) catch |err| {
            switch (err) {
                error.TlsRequired => try out.print("glacier serve: bind outside localhost needs TLS\n", .{}),
                error.InvalidAddress => try out.print("glacier serve: invalid --listen / --flight\n", .{}),
                else => try out.print("glacier serve failed: {s}\n", .{glacier.err.staticMessage(err)}),
            }
            die(out);
        };
        return;
    }

    var session = if (args.catalog) |endpoint| blk: {
        var parsed_headers: std.ArrayList(glacier.rest_catalog.Header) = .empty;
        defer parsed_headers.deinit(gpa);
        for (args.headers) |line| {
            try parsed_headers.append(gpa, glacier.rest_catalog.parseHeaderLine(line) catch {
                try out.print("glacier: invalid --header (want Name: value)\n", .{});
                die(out);
            });
        }
        break :blk glacier.Session.openRest(gpa, io, .{
            .endpoint = endpoint,
            .warehouse = args.warehouse,
            .token = args.token,
            .extra_headers = parsed_headers.items,
            .oauth_client_id = args.oauth_client_id,
            .oauth_client_secret = args.oauth_client_secret,
            .oauth_server = args.oauth_server,
            .oauth_scope = args.oauth_scope,
            .auth = args.auth,
            .sigv4_service = args.sigv4_service,
            .default_namespace = args.namespace,
            .default_table = args.path,
        }) catch {
            const ge = glacier.session.lastOpenError().?;
            try out.print("open failed: {s}\n", .{ge.message});
            die(out);
        };
    } else if (args.path) |path|
        glacier.Session.open(gpa, io, path) catch {
            const ge = glacier.session.lastOpenError().?;
            try out.print("open failed: {s}\n", .{ge.message});
            die(out);
        }
    else
        try glacier.Session.openEmpty(gpa, io);
    defer session.close();

    var timer = args.timer;
    var ctx = RunCtx{
        .gpa = gpa,
        .io = io,
        .out = out,
        .timer = &timer,
    };

    if (args.commands.len > 0) {
        for (args.commands) |sql| {
            switch (try handleLine(&session, &ctx, sql)) {
                .ok => {},
                .quit => return,
                .fail => die(out),
            }
        }
        try out.flush();
        return;
    }

    const stdin_is_tty = std.Io.File.stdin().isTty(io) catch false;
    if (!stdin_is_tty) {
        const script = try readAllStdin(gpa, io);
        defer gpa.free(script);
        const stmts = try splitStatements(gpa, script);
        defer gpa.free(stmts);
        for (stmts) |sql| {
            switch (try handleLine(&session, &ctx, sql)) {
                .ok => {},
                .quit => return,
                .fail => die(out),
            }
        }
        try out.flush();
        return;
    }

    const stdout_tty = std.Io.File.stdout().isTty(io) catch false;
    if (args.tui and stdout_tty) {
        try runTui(&session, &ctx);
        return;
    }

    try printBanner(out, session);
    try out.flush();
    try runLineRepl(&session, &ctx);
}

fn runLineRepl(session: *glacier.Session, ctx: *RunCtx) !void {
    var stdin_buf: [4096]u8 = undefined;
    var stdin_file = std.Io.File.stdin().readerStreaming(ctx.io, &stdin_buf);
    const stdin = &stdin_file.interface;

    while (true) {
        const prompt = if (session.table_name.len == 0) "glacier" else session.table_name;
        try ctx.out.print("{s}> ", .{prompt});
        try ctx.out.flush();

        const line = stdin.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => {
                try ctx.out.print("line too long\n", .{});
                try ctx.out.flush();
                continue;
            },
            else => return err,
        } orelse break;
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;

        switch (try handleLine(session, ctx, trimmed)) {
            .ok, .fail => {},
            .quit => break,
        }
        try ctx.out.flush();
    }
}

const welcome_text =
    \\
    \\  Type SQL and press Enter.
    \\
    \\  SHOW / ATTACH / glacier.catalogs / glacier.tables
    \\  CREATE / INSERT / COPY FROM / DELETE / DROP / PARTITIONED BY on glacier, Hadoop, or REST
    \\  Iceberg: glacier.snapshots / glacier.files / t.snapshots
    \\
    \\  Tab            expand / fold the selected cell
    \\  ↑↓ ←→          pick row / column (scroll when expanded)
    \\  .open PATH     parquet / avro / iceberg / s3 / http
    \\  .schema        columns
    \\  .timer on|off
    \\  .help          this panel
    \\  .quit          leave  (Ctrl+D too)
    \\
;

const TermSize = struct { cols: u16, rows: u16 };

const raw_tui_ok = switch (builtin.os.tag) {
    .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly => true,
    else => false,
};

const Screen = enum { source, browse, url, query };
const UrlKind = enum { s3, http };
const BrowseKind = enum { parent, dir, file };
const BrowseItem = struct {
    name: []u8,
    kind: BrowseKind,
    iceberg: bool = false,
};

const Key = union(enum) {
    char: u8,
    up,
    down,
    left,
    right,
    home,
    end,
    delete,
    tab,
    enter,
    esc,
    backspace,
    ctrl_c,
    ctrl_d,
    ctrl_o,
    ctrl_e,
    none,
};

const ResultPane = struct {
    result: ?glacier.Result = null,
    elapsed: ?std.Io.Clock.Duration = null,
    show_timer: bool = true,
    sel_row: usize = 0,
    sel_col: usize = 0,
    table_scroll: usize = 0,
    expanded: bool = false,
    wrap_scroll: usize = 0,

    fn deinit(self: *ResultPane) void {
        if (self.result) |*r| r.deinit();
        self.* = .{};
    }

    fn take(self: *ResultPane, result: glacier.Result, elapsed: std.Io.Clock.Duration, show_timer: bool) void {
        self.deinit();
        self.result = result;
        self.elapsed = elapsed;
        self.show_timer = show_timer;
        self.sel_row = 0;
        self.sel_col = 0;
        self.table_scroll = 0;
        self.expanded = false;
        self.wrap_scroll = 0;
    }

    fn batch(self: *const ResultPane) ?glacier.Batch {
        const r = self.result orelse return null;
        return r.batch;
    }

    fn clampSel(self: *ResultPane) void {
        const b = self.batch() orelse {
            self.sel_row = 0;
            self.sel_col = 0;
            return;
        };
        if (b.len == 0 or b.columns.len == 0) {
            self.sel_row = 0;
            self.sel_col = 0;
            return;
        }
        self.sel_row = @min(self.sel_row, b.len - 1);
        self.sel_col = @min(self.sel_col, b.columns.len - 1);
    }
};

fn isDataFile(name: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(name, ".parquet") or
        std.ascii.endsWithIgnoreCase(name, ".avro");
}

fn utf8Pop(buf: []u8, len: *usize) void {
    if (len.* == 0) return;
    var i = len.* - 1;
    while (i > 0 and (buf[i] & 0xC0) == 0x80) i -= 1;
    len.* = i;
}

fn insertByte(buf: []u8, len: *usize, cur: *usize, c: u8) void {
    if (len.* >= buf.len or cur.* > len.*) return;
    var i = len.*;
    while (i > cur.*) : (i -= 1) buf[i] = buf[i - 1];
    buf[cur.*] = c;
    len.* += 1;
    cur.* += 1;
}

fn backspaceAt(buf: []u8, len: *usize, cur: *usize) void {
    if (cur.* == 0 or cur.* > len.*) return;
    const at = cur.* - 1;
    var i = at;
    while (i + 1 < len.*) : (i += 1) buf[i] = buf[i + 1];
    len.* -= 1;
    cur.* -= 1;
}

fn deleteAt(buf: []u8, len: *usize, cur: *usize) void {
    if (cur.* >= len.*) return;
    var i = cur.*;
    while (i + 1 < len.*) : (i += 1) buf[i] = buf[i + 1];
    len.* -= 1;
}

fn browseLessThan(_: void, a: BrowseItem, b: BrowseItem) bool {
    const rank = struct {
        fn of(k: BrowseKind) u8 {
            return switch (k) {
                .parent => 0,
                .dir => 1,
                .file => 2,
            };
        }
    }.of;
    const ra = rank(a.kind);
    const rb = rank(b.kind);
    if (ra != rb) return ra < rb;
    return std.ascii.lessThanIgnoreCase(a.name, b.name);
}

fn freeBrowse(gpa: std.mem.Allocator, items: *std.ArrayList(BrowseItem)) void {
    for (items.items) |it| gpa.free(it.name);
    items.clearRetainingCapacity();
}

fn dirLooksLikeIceberg(gpa: std.mem.Allocator, io: std.Io, parent: []const u8, name: []const u8) bool {
    const meta = std.fs.path.join(gpa, &.{ parent, name, "metadata" }) catch return false;
    defer gpa.free(meta);
    std.Io.Dir.cwd().access(io, meta, .{}) catch return false;
    return true;
}

fn loadBrowse(
    gpa: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    items: *std.ArrayList(BrowseItem),
) !void {
    freeBrowse(gpa, items);
    try items.append(gpa, .{
        .name = try gpa.dupe(u8, ".."),
        .kind = .parent,
    });

    var dir = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    defer dir.close(io);

    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.name.len == 0 or entry.name[0] == '.') continue;
        switch (entry.kind) {
            .directory => try items.append(gpa, .{
                .name = try gpa.dupe(u8, entry.name),
                .kind = .dir,
                .iceberg = dirLooksLikeIceberg(gpa, io, path, entry.name),
            }),
            .file => {
                if (!isDataFile(entry.name)) continue;
                try items.append(gpa, .{
                    .name = try gpa.dupe(u8, entry.name),
                    .kind = .file,
                });
            },
            .sym_link => {
                if (isDataFile(entry.name)) {
                    try items.append(gpa, .{
                        .name = try gpa.dupe(u8, entry.name),
                        .kind = .file,
                    });
                } else {
                    try items.append(gpa, .{
                        .name = try gpa.dupe(u8, entry.name),
                        .kind = .dir,
                        .iceberg = dirLooksLikeIceberg(gpa, io, path, entry.name),
                    });
                }
            },
            else => {},
        }
    }
    std.mem.sort(BrowseItem, items.items, {}, browseLessThan);
}

fn setErr(gpa: std.mem.Allocator, err: *std.ArrayList(u8), msg: []const u8) void {
    err.clearRetainingCapacity();
    err.appendSlice(gpa, msg) catch {};
}

fn adoptOpen(
    session: *glacier.Session,
    ctx: *RunCtx,
    path: []const u8,
    view: *std.Io.Writer.Allocating,
    err: *std.ArrayList(u8),
    pane: *ResultPane,
) bool {
    const next = glacier.Session.open(ctx.gpa, ctx.io, path) catch {
        const ge = glacier.session.lastOpenError().?;
        setErr(ctx.gpa, err, ge.message);
        return false;
    };
    session.close();
    session.* = next;
    err.clearRetainingCapacity();
    pane.deinit();
    resetAlloc(view, ctx.gpa);
    printSchema(&view.writer, session.*) catch {};
    return true;
}

fn adoptEmpty(
    session: *glacier.Session,
    ctx: *RunCtx,
    view: *std.Io.Writer.Allocating,
    err: *std.ArrayList(u8),
    pane: *ResultPane,
) bool {
    const next = glacier.Session.openEmpty(ctx.gpa, ctx.io) catch {
        setErr(ctx.gpa, err, "could not open empty session");
        return false;
    };
    session.close();
    session.* = next;
    err.clearRetainingCapacity();
    pane.deinit();
    resetAlloc(view, ctx.gpa);
    view.writer.writeAll("  empty session. SELECT 1 works without a file.\n") catch {};
    return true;
}

fn releasePicker(
    session: *glacier.Session,
    ctx: *RunCtx,
    view: *std.Io.Writer.Allocating,
    pane: *ResultPane,
    sql_len: *usize,
    err: *std.ArrayList(u8),
) void {
    pane.deinit();
    sql_len.* = 0;
    err.clearRetainingCapacity();
    resetAlloc(view, ctx.gpa);
    if (session.path.len == 0 and session.files.len == 0) return;
    _ = adoptEmpty(session, ctx, view, err, pane);
}

fn cwdPath(gpa: std.mem.Allocator, io: std.Io) ![]u8 {
    var rbuf: [std.fs.max_path_bytes]u8 = undefined;
    var dir = std.Io.Dir.cwd().openDir(io, ".", .{}) catch {
        return linuxGetcwd(gpa, &rbuf);
    };
    defer dir.close(io);
    if (dir.realPath(io, &rbuf)) |n| {
        return gpa.dupe(u8, rbuf[0..n]);
    } else |_| {}
    return linuxGetcwd(gpa, &rbuf);
}

fn linuxGetcwd(gpa: std.mem.Allocator, rbuf: *[std.fs.max_path_bytes]u8) ![]u8 {
    if (comptime builtin.os.tag == .linux) {
        const rc = std.os.linux.getcwd(rbuf, rbuf.len);
        if (std.os.linux.errno(rc) == .SUCCESS and rc > 1) {
            return gpa.dupe(u8, rbuf[0 .. rc - 1]);
        }
    }
    return gpa.dupe(u8, ".");
}

fn runTui(session: *glacier.Session, ctx: *RunCtx) !void {
    if (comptime raw_tui_ok) {
        try runTuiApp(session, ctx);
        return;
    }
    try runTuiCooked(session, ctx);
}

fn runTuiApp(session: *glacier.Session, ctx: *RunCtx) !void {
    if (comptime raw_tui_ok) {
        const orig = enableRaw() catch return runTuiCooked(session, ctx);
        defer restoreRaw(orig);

        try ctx.out.writeAll("\x1b[?1049h");
        defer leaveAlt(ctx.out);

        var view: std.Io.Writer.Allocating = .init(ctx.gpa);
        defer view.deinit();

        var pane: ResultPane = .{};
        defer pane.deinit();

        var err: std.ArrayList(u8) = .empty;
        defer err.deinit(ctx.gpa);

        var items: std.ArrayList(BrowseItem) = .empty;
        defer {
            freeBrowse(ctx.gpa, &items);
            items.deinit(ctx.gpa);
        }

        var browse_path: std.ArrayList(u8) = .empty;
        defer browse_path.deinit(ctx.gpa);
        {
            const cwd = try cwdPath(ctx.gpa, ctx.io);
            defer ctx.gpa.free(cwd);
            try browse_path.appendSlice(ctx.gpa, cwd);
        }

        var screen: Screen = if (session.path.len > 0) .query else .source;
        var source_sel: usize = 0;
        var browse_sel: usize = 0;
        var browse_scroll: usize = 0;
        var url_kind: UrlKind = .s3;
        var url_buf: [2048]u8 = undefined;
        var url_len: usize = 0;
        var url_cur: usize = 0;
        var sql_buf: [4096]u8 = undefined;
        var sql_len: usize = 0;

        if (screen == .query) {
            try printSchema(&view.writer, session.*);
        }

        while (true) {
            const size = termSize();
            switch (screen) {
                .source => try paintSource(ctx.out, source_sel, err.items, size),
                .browse => try paintBrowse(ctx.out, browse_path.items, items.items, browse_sel, browse_scroll, err.items, size),
                .url => try paintUrl(ctx.out, url_kind, url_buf[0..url_len], url_cur, err.items, size),
                .query => try paintQuery(ctx.out, session.*, view.written(), sql_buf[0..sql_len], size, &pane),
            }
            try ctx.out.flush();

            const key = readKey() catch |e| switch (e) {
                error.EndOfStream => break,
                else => return e,
            };

            switch (screen) {
                .source => switch (key) {
                    .ctrl_c, .ctrl_d => break,
                    .char => |c| switch (c) {
                        'q', 'Q' => break,
                        'j', 'J' => source_sel = @min(source_sel + 1, 3),
                        'k', 'K' => source_sel -|= 1,
                        else => {},
                    },
                    .up => source_sel -|= 1,
                    .down => source_sel = @min(source_sel + 1, 3),
                    .enter => switch (source_sel) {
                        0 => {
                            err.clearRetainingCapacity();
                            loadBrowse(ctx.gpa, ctx.io, browse_path.items, &items) catch |e| {
                                setErr(ctx.gpa, &err, @errorName(e));
                            };
                            browse_sel = 0;
                            browse_scroll = 0;
                            screen = .browse;
                        },
                        1 => {
                            url_kind = .s3;
                            const p = "s3://";
                            @memcpy(url_buf[0..p.len], p);
                            url_len = p.len;
                            url_cur = url_len;
                            err.clearRetainingCapacity();
                            screen = .url;
                        },
                        2 => {
                            url_kind = .http;
                            const p = "https://";
                            @memcpy(url_buf[0..p.len], p);
                            url_len = p.len;
                            url_cur = url_len;
                            err.clearRetainingCapacity();
                            screen = .url;
                        },
                        else => {
                            if (adoptEmpty(session, ctx, &view, &err, &pane)) {
                                sql_len = 0;
                                screen = .query;
                            }
                        },
                    },
                    else => {},
                },
                .browse => switch (key) {
                    .ctrl_c, .ctrl_d => break,
                    .esc => {
                        releasePicker(session, ctx, &view, &pane, &sql_len, &err);
                        screen = .source;
                    },
                    .char => |c| switch (c) {
                        'q', 'Q' => break,
                        'j', 'J' => browseMove(&browse_sel, &browse_scroll, items.items.len, size, 1),
                        'k', 'K' => browseMove(&browse_sel, &browse_scroll, items.items.len, size, -1),
                        'o', 'O' => {
                            const target = blk: {
                                if (items.items.len > 0) {
                                    const item = items.items[browse_sel];
                                    if (item.kind == .dir) {
                                        break :blk std.fs.path.resolve(ctx.gpa, &.{ browse_path.items, item.name }) catch {
                                            setErr(ctx.gpa, &err, "path too long");
                                            continue;
                                        };
                                    }
                                }
                                break :blk ctx.gpa.dupe(u8, browse_path.items) catch {
                                    setErr(ctx.gpa, &err, "out of memory");
                                    continue;
                                };
                            };
                            defer ctx.gpa.free(target);
                            if (adoptOpen(session, ctx, target, &view, &err, &pane)) {
                                sql_len = 0;
                                screen = .query;
                            }
                        },
                        else => {},
                    },
                    .up => browseMove(&browse_sel, &browse_scroll, items.items.len, size, -1),
                    .down => browseMove(&browse_sel, &browse_scroll, items.items.len, size, 1),
                    .enter => {
                        if (items.items.len == 0) continue;
                        const item = items.items[browse_sel];
                        switch (item.kind) {
                            .parent, .dir => {
                                const next = std.fs.path.resolve(ctx.gpa, &.{ browse_path.items, item.name }) catch {
                                    setErr(ctx.gpa, &err, "path too long");
                                    continue;
                                };
                                defer ctx.gpa.free(next);
                                browse_path.clearRetainingCapacity();
                                browse_path.appendSlice(ctx.gpa, next) catch {};
                                err.clearRetainingCapacity();
                                loadBrowse(ctx.gpa, ctx.io, browse_path.items, &items) catch |e| {
                                    setErr(ctx.gpa, &err, @errorName(e));
                                };
                                browse_sel = 0;
                                browse_scroll = 0;
                            },
                            .file => {
                                const full = std.fs.path.resolve(ctx.gpa, &.{ browse_path.items, item.name }) catch {
                                    setErr(ctx.gpa, &err, "path too long");
                                    continue;
                                };
                                defer ctx.gpa.free(full);
                                if (adoptOpen(session, ctx, full, &view, &err, &pane)) {
                                    sql_len = 0;
                                    screen = .query;
                                }
                            },
                        }
                    },
                    else => {},
                },
                .url => switch (key) {
                    .ctrl_c, .ctrl_d => break,
                    .esc => {
                        releasePicker(session, ctx, &view, &pane, &sql_len, &err);
                        screen = .source;
                    },
                    .left => url_cur -|= 1,
                    .right => url_cur = @min(url_cur + 1, url_len),
                    .home => url_cur = 0,
                    .end => url_cur = url_len,
                    .backspace => backspaceAt(&url_buf, &url_len, &url_cur),
                    .delete => deleteAt(&url_buf, &url_len, &url_cur),
                    .none => {},
                    .char => |c| {
                        if (c >= 32 and c < 127) insertByte(&url_buf, &url_len, &url_cur, c);
                    },
                    .enter => {
                        const path = std.mem.trim(u8, url_buf[0..url_len], " \t");
                        if (path.len == 0) {
                            setErr(ctx.gpa, &err, "paste an s3:// or http(s):// URL");
                            continue;
                        }
                        if (adoptOpen(session, ctx, path, &view, &err, &pane)) {
                            sql_len = 0;
                            screen = .query;
                        }
                    },
                    else => {},
                },
                .query => switch (key) {
                    .ctrl_c, .ctrl_d => break,
                    .ctrl_o, .esc => {
                        releasePicker(session, ctx, &view, &pane, &sql_len, &err);
                        screen = .source;
                    },
                    .tab, .ctrl_e => toggleExpand(&pane),
                    .up => paneNav(&pane, size, -1, 0),
                    .down => paneNav(&pane, size, 1, 0),
                    .left => paneNav(&pane, size, 0, -1),
                    .right => paneNav(&pane, size, 0, 1),
                    .backspace => utf8Pop(&sql_buf, &sql_len),
                    .char => |c| {
                        if (c >= 32 and sql_len < sql_buf.len) {
                            sql_buf[sql_len] = c;
                            sql_len += 1;
                        }
                    },
                    .enter => {
                        const trimmed = std.mem.trim(u8, sql_buf[0..sql_len], " \t");
                        if (trimmed.len == 0) continue;
                        if (trimmed[0] == '.') {
                            const rest = std.mem.trim(u8, trimmed[1..], " \t");
                            if (eql(rest, "quit") or eql(rest, "exit")) break;
                            pane.deinit();
                            resetAlloc(&view, ctx.gpa);
                            try handleTuiDot(session, ctx, rest, &view.writer);
                            sql_len = 0;
                            continue;
                        }
                        resetAlloc(&view, ctx.gpa);
                        const t0 = std.Io.Clock.Timestamp.now(ctx.io, .awake);
                        const result = session.execute(trimmed) catch |exec_err| {
                            pane.deinit();
                            const ge = session.lastError() orelse glacier.GlacierError.fromZig(exec_err);
                            try view.writer.print("error: {s}\n", .{ge.message});
                            sql_len = 0;
                            continue;
                        };
                        pane.take(result, t0.durationTo(std.Io.Clock.Timestamp.now(ctx.io, .awake)), ctx.timer.*);
                        sql_len = 0;
                    },
                    .none, .home, .end, .delete => {},
                },
            }
        }
    } else {
        try runTuiCooked(session, ctx);
    }
}

fn browseMove(sel: *usize, scroll: *usize, n: usize, size: TermSize, delta: i32) void {
    if (n == 0) return;
    if (delta < 0) {
        sel.* -|= 1;
    } else {
        sel.* = @min(sel.* + 1, n - 1);
    }
    const vis = browseVisible(size);
    if (sel.* < scroll.*) scroll.* = sel.*;
    if (sel.* >= scroll.* + vis) scroll.* = sel.* + 1 - vis;
}

fn browseVisible(size: TermSize) usize {
    return @max(@as(usize, 1), @as(usize, size.rows) -| 8);
}

fn queryBodyRows(size: TermSize) usize {
    return @max(@as(usize, 3), @as(usize, size.rows) -| 6);
}

fn tableDataRows(size: TermSize) usize {
    return @max(@as(usize, 1), queryBodyRows(size) -| 8);
}

fn pagerInnerWidth(term_cols: u16) usize {
    return @max(@as(usize, 8), @as(usize, term_cols) -| 4);
}

fn pagerTextRows(size: TermSize) usize {
    return @max(@as(usize, 1), queryBodyRows(size) -| 4);
}

fn toggleExpand(pane: *ResultPane) void {
    const b = pane.batch() orelse return;
    if (b.len == 0 or b.columns.len == 0) return;
    pane.expanded = !pane.expanded;
    pane.wrap_scroll = 0;
}

fn paneNav(pane: *ResultPane, size: TermSize, d_row: i32, d_col: i32) void {
    const b = pane.batch() orelse return;
    if (b.len == 0 or b.columns.len == 0) return;
    pane.clampSel();

    if (pane.expanded and d_row != 0 and d_col == 0) {
        var tmp: [256]u8 = undefined;
        const raw = cellText(b.columns[pane.sel_col], pane.sel_row, &tmp) catch return;
        const n = countWrap(raw, pagerInnerWidth(size.cols));
        const max_scroll = n -| pagerTextRows(size);
        if (d_row < 0) pane.wrap_scroll -|= 1 else pane.wrap_scroll = @min(pane.wrap_scroll + 1, max_scroll);
        return;
    }

    if (d_row < 0) pane.sel_row -|= 1 else if (d_row > 0) pane.sel_row = @min(pane.sel_row + 1, b.len - 1);
    if (d_col < 0) pane.sel_col -|= 1 else if (d_col > 0) pane.sel_col = @min(pane.sel_col + 1, b.columns.len - 1);
    pane.wrap_scroll = 0;

    const vis = tableDataRows(size);
    if (pane.sel_row < pane.table_scroll) pane.table_scroll = pane.sel_row;
    if (pane.sel_row >= pane.table_scroll + vis) pane.table_scroll = pane.sel_row + 1 - vis;
}

fn runTuiCooked(session: *glacier.Session, ctx: *RunCtx) !void {
    try ctx.out.writeAll("\x1b[?1049h");
    defer leaveAlt(ctx.out);

    var view: std.Io.Writer.Allocating = .init(ctx.gpa);
    defer view.deinit();
    try view.writer.writeAll(welcome_text);

    var stdin_buf: [4096]u8 = undefined;
    var stdin_file = std.Io.File.stdin().readerStreaming(ctx.io, &stdin_buf);
    const stdin = &stdin_file.interface;

    while (true) {
        const size = termSize();
        try paintTui(ctx.out, session.*, view.written(), size);
        try ctx.out.flush();

        const line = stdin.takeDelimiter('\n') catch |e| switch (e) {
            error.StreamTooLong => {
                resetAlloc(&view, ctx.gpa);
                try view.writer.writeAll("line too long\n");
                continue;
            },
            else => return e,
        } orelse break;
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;

        if (trimmed[0] == '.') {
            const rest = std.mem.trim(u8, trimmed[1..], " \t");
            if (eql(rest, "quit") or eql(rest, "exit")) break;
            resetAlloc(&view, ctx.gpa);
            try handleTuiDot(session, ctx, rest, &view.writer);
            continue;
        }

        resetAlloc(&view, ctx.gpa);
        const t0 = std.Io.Clock.Timestamp.now(ctx.io, .awake);
        var result = session.execute(trimmed) catch |exec_err| {
            const ge = session.lastError() orelse glacier.GlacierError.fromZig(exec_err);
            try view.writer.print("error: {s}\n", .{ge.message});
            continue;
        };
        defer result.deinit();
        const elapsed = t0.durationTo(std.Io.Clock.Timestamp.now(ctx.io, .awake));
        const max_rows = @max(@as(usize, 5), @as(usize, size.rows) -| 8);
        if (result.nextBatch()) |batch| {
            try printBatchBoxed(&view.writer, batch, max_rows, size.cols);
        } else {
            try view.writer.writeAll("(no rows)\n");
        }
        if (ctx.timer.*) try printStats(&view.writer, elapsed, result.scan_stats);
    }
}

fn enableRaw() !std.posix.termios {
    switch (comptime builtin.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly => {
            const fd = std.posix.STDIN_FILENO;
            const orig = try std.posix.tcgetattr(fd);
            var raw = orig;
            raw.lflag.ECHO = false;
            raw.lflag.ICANON = false;
            raw.lflag.ISIG = false;
            raw.lflag.IEXTEN = false;
            if (@hasField(@TypeOf(raw.iflag), "IXON")) raw.iflag.IXON = false;
            if (@hasField(@TypeOf(raw.iflag), "ICRNL")) raw.iflag.ICRNL = false;
            raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
            raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;
            try std.posix.tcsetattr(fd, .FLUSH, raw);
            return orig;
        },
        else => return error.NotATerminal,
    }
}

fn restoreRaw(orig: std.posix.termios) void {
    switch (comptime builtin.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly => {
            std.posix.tcsetattr(std.posix.STDIN_FILENO, .FLUSH, orig) catch {};
        },
        else => {},
    }
}

fn readByte() !?u8 {
    switch (comptime builtin.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly => {
            const fd = std.posix.STDIN_FILENO;
            var b: [1]u8 = undefined;
            const n = try std.posix.read(fd, &b);
            if (n == 0) return null;
            return b[0];
        },
        else => return error.NotATerminal,
    }
}

fn readKey() !Key {
    switch (comptime builtin.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly => {
            const c = try readByte() orelse return error.EndOfStream;
            if (c == 0x03) return .ctrl_c;
            if (c == 0x04) return .ctrl_d;
            if (c == 0x05) return .ctrl_e;
            if (c == 0x09) return .tab;
            if (c == 0x0f) return .ctrl_o;
            if (c == 0x7f or c == 0x08) return .backspace;
            if (c == '\r' or c == '\n') return .enter;
            if (c != 0x1b) return .{ .char = c };

            const fd = std.posix.STDIN_FILENO;
            const now = std.posix.tcgetattr(fd) catch return .esc;
            var timed = now;
            timed.cc[@intFromEnum(std.posix.V.MIN)] = 0;
            timed.cc[@intFromEnum(std.posix.V.TIME)] = 1;
            std.posix.tcsetattr(fd, .NOW, timed) catch return .esc;
            defer std.posix.tcsetattr(fd, .NOW, now) catch {};

            const c2 = try readByte() orelse return .esc;
            if (c2 == '[') return parseCsi();
            if (c2 == 'O') {
                const c3 = try readByte() orelse return .none;
                return switch (c3) {
                    'A' => .up,
                    'B' => .down,
                    'C' => .right,
                    'D' => .left,
                    'H' => .home,
                    'F' => .end,
                    else => .none,
                };
            }
            return .none;
        },
        else => return error.NotATerminal,
    }
}

/// Caller has stdin in timed (MIN=0, TIME=1) mode. Unknown CSI is ignored, not Esc.
fn parseCsi() !Key {
    var params: [24]u8 = undefined;
    var n: usize = 0;
    while (true) {
        const b = try readByte() orelse return .none;
        if (b >= 0x40 and b <= 0x7e) return decodeCsi(params[0..n], b);
        if (n < params.len) {
            params[n] = b;
            n += 1;
        }
    }
}

fn decodeCsi(params: []const u8, final: u8) Key {
    return switch (final) {
        'A' => .up,
        'B' => .down,
        'C' => .right,
        'D' => .left,
        'H' => .home,
        'F' => .end,
        '~' => switch (csiFirstNum(params)) {
            1, 7 => .home,
            3 => .delete,
            4, 8 => .end,
            else => .none,
        },
        else => .none,
    };
}

fn csiFirstNum(params: []const u8) u32 {
    var v: u32 = 0;
    var any = false;
    for (params) |b| {
        if (b >= '0' and b <= '9') {
            v = v *% 10 + (b - '0');
            any = true;
        } else break;
    }
    return if (any) v else 1;
}

fn leaveAlt(out: *std.Io.Writer) void {
    out.writeAll("\x1b[?25h\x1b[?1049l\x1b[0m") catch {};
    out.flush() catch {};
}

fn resetAlloc(a: *std.Io.Writer.Allocating, gpa: std.mem.Allocator) void {
    a.deinit();
    a.* = .init(gpa);
}

fn handleTuiDot(
    session: *glacier.Session,
    ctx: *RunCtx,
    rest: []const u8,
    view: *std.Io.Writer,
) !void {
    if (eql(rest, "help")) {
        try view.writeAll(welcome_text);
        return;
    }
    if (eql(rest, "schema")) {
        try printSchema(view, session.*);
        return;
    }
    if (eql(rest, "timer on")) {
        ctx.timer.* = true;
        try view.writeAll("timer on\n");
        return;
    }
    if (eql(rest, "timer off")) {
        ctx.timer.* = false;
        try view.writeAll("timer off\n");
        return;
    }
    if (std.ascii.startsWithIgnoreCase(rest, "open ")) {
        const path = std.mem.trim(u8, rest[5..], " \t");
        if (path.len == 0) {
            try view.writeAll("usage: .open PATH\n");
            return;
        }
        const next = glacier.Session.open(ctx.gpa, ctx.io, path) catch {
            const ge = glacier.session.lastOpenError().?;
            try view.print("open failed: {s}\n", .{ge.message});
            return;
        };
        session.close();
        session.* = next;
        try view.print("opened {s}\n", .{session.path});
        return;
    }
    try view.print("unknown command: .{s}\n", .{rest});
}

fn paintTui(out: *std.Io.Writer, session: glacier.Session, view: []const u8, size: TermSize) !void {
    try out.writeAll("\x1b[2J\x1b[H");
    try out.writeAll("\x1b[1;36m glacier\x1b[0m ");
    try out.print("{s}", .{glacier.version});
    if (session.table_name.len > 0) {
        try out.print("  \x1b[1m{s}\x1b[0m", .{session.table_name});
    } else {
        try out.writeAll("  empty");
    }
    if (session.path.len > 0) try out.print("  {s}", .{session.path});
    if (peakRssBytes()) |rss| {
        var rbuf: [32]u8 = undefined;
        try out.print("  rss {s}", .{try formatBytes(rss, &rbuf)});
    }
    try out.writeByte('\n');
    try writeRule(out, size.cols);
    try out.writeByte('\n');

    const body_rows = queryBodyRows(size);
    const n = try writeViewLines(out, view, size.cols, body_rows);

    var i = n;
    while (i < body_rows) : (i += 1) try out.writeByte('\n');
    try writeRule(out, size.cols);
    try out.writeAll("\n SQL + Enter   .help  .open PATH  .quit\n");
    const prompt = if (session.table_name.len == 0) "glacier" else session.table_name;
    try out.print("{s}> ", .{prompt});
}

fn writeFit(out: *std.Io.Writer, s: []const u8, max: usize) !void {
    if (max == 0) return;
    if (s.len <= max) {
        try out.writeAll(s);
        return;
    }
    if (max <= 3) {
        try out.writeAll(s[0..max]);
        return;
    }
    try out.writeAll("...");
    try out.writeAll(s[s.len - (max - 3) ..]);
}

fn paintBar(out: *std.Io.Writer, size: TermSize) !void {
    try out.writeAll("\x1b[2J\x1b[H");
    try out.writeAll("\x1b[1;36m glacier\x1b[0m ");
    try out.print("{s}", .{glacier.version});
    if (peakRssBytes()) |rss| {
        var rbuf: [32]u8 = undefined;
        try out.print("  rss {s}", .{try formatBytes(rss, &rbuf)});
    }
    try out.writeByte('\n');
    try writeRule(out, size.cols);
}

fn paintErr(out: *std.Io.Writer, msg: []const u8, cols: u16) !void {
    if (msg.len == 0) {
        try out.writeByte('\n');
        return;
    }
    try out.writeAll("\x1b[31m");
    try writeFit(out, msg, @max(@as(usize, 1), @as(usize, cols)));
    try out.writeAll("\x1b[0m\n");
}

fn paintSource(out: *std.Io.Writer, sel: usize, err: []const u8, size: TermSize) !void {
    try out.writeAll("\x1b[?25l");
    try paintBar(out, size);
    try out.writeAll("\n  Open a table\n\n");
    const labels = [_][]const u8{
        "Local parquet / avro / Iceberg",
        "S3     s3://bucket/key",
        "HTTP   https://…",
        "Empty session   SELECT 1, no file",
    };
    for (labels, 0..) |label, i| {
        if (i == sel) {
            try out.print("  \x1b[7m {s} \x1b[0m\n", .{label});
        } else {
            try out.print("    {s}\n", .{label});
        }
    }
    try out.writeByte('\n');
    try paintErr(out, err, size.cols);
    try writeRule(out, size.cols);
    try out.writeAll("\n  ↑↓ Enter   q quit\n");
}

fn paintBrowse(
    out: *std.Io.Writer,
    path: []const u8,
    items: []const BrowseItem,
    sel: usize,
    scroll: usize,
    err: []const u8,
    size: TermSize,
) !void {
    try out.writeAll("\x1b[?25l");
    try paintBar(out, size);
    try out.writeAll("\n  ");
    try writeFit(out, path, @max(@as(usize, 1), @as(usize, size.cols) -| 2));
    try out.writeAll("\n\n");

    const vis = browseVisible(size);
    const end = @min(items.len, scroll + vis);
    var row: usize = scroll;
    while (row < end) : (row += 1) {
        const item = items[row];
        const mark = row == sel;
        if (mark) {
            try out.writeAll("  \x1b[7m ");
        } else {
            try out.writeAll("    ");
        }
        switch (item.kind) {
            .parent => try out.writeAll("../"),
            .dir => {
                try writeFit(out, item.name, 40);
                try out.writeAll("/");
                if (item.iceberg) try out.writeAll("  iceberg");
            },
            .file => try writeFit(out, item.name, 48),
        }
        if (mark) try out.writeAll(" \x1b[0m");
        try out.writeByte('\n');
    }
    if (items.len == 1) try out.writeAll("    (no parquet/avro here)\n");
    try out.writeByte('\n');
    try paintErr(out, err, size.cols);
    try writeRule(out, size.cols);
    try out.writeAll("\n  Enter open file / enter folder   o Iceberg   Esc back   q quit\n");
}

fn paintUrl(out: *std.Io.Writer, kind: UrlKind, url: []const u8, cursor: usize, err: []const u8, size: TermSize) !void {
    try paintBar(out, size);
    const title: []const u8 = switch (kind) {
        .s3 => "S3 URL",
        .http => "HTTP URL",
    };
    try out.print("\n  {s}\n\n  ", .{title});

    const cur = @min(cursor, url.len);
    const vis_max = @max(@as(usize, 1), @as(usize, size.cols) -| 2);
    var start: usize = 0;
    if (url.len > vis_max) {
        start = cur -| (vis_max / 2);
        if (start + vis_max > url.len) start = url.len - vis_max;
        if (cur >= start + vis_max) start = cur - vis_max + 1;
    }
    const shown = url[start..@min(url.len, start + vis_max)];
    try out.writeAll(shown);

    const col: usize = 3 + (cur - start);
    try out.print("\x1b[{d}G\x1b[s", .{col});

    try out.writeAll("\n\n");
    try paintErr(out, err, size.cols);
    try writeRule(out, size.cols);
    try out.writeAll("\n  Enter open   ←→ edit   Esc back   Ctrl+C quit\n");
    try out.writeAll("\x1b[u\x1b[?25h");
}

fn paintQuery(
    out: *std.Io.Writer,
    session: glacier.Session,
    view: []const u8,
    sql: []const u8,
    size: TermSize,
    pane: *const ResultPane,
) !void {
    const expanded = pane.expanded and pane.batch() != null;
    if (expanded) {
        try out.writeAll("\x1b[?25l");
    }
    try out.writeAll("\x1b[2J\x1b[H");
    try out.writeAll("\x1b[1;36m glacier\x1b[0m ");
    try out.print("{s}", .{glacier.version});
    if (session.table_name.len > 0) {
        try out.print("  \x1b[1m{s}\x1b[0m", .{session.table_name});
    } else {
        try out.writeAll("  empty");
    }
    if (session.path.len > 0) {
        try out.writeAll("  ");
        try writeFit(out, session.path, 40);
    }
    if (peakRssBytes()) |rss| {
        var rbuf: [32]u8 = undefined;
        try out.print("  rss {s}", .{try formatBytes(rss, &rbuf)});
    }
    try out.writeByte('\n');
    try writeRule(out, size.cols);
    try out.writeByte('\n');

    const body_rows = queryBodyRows(size);
    var n: usize = 0;
    if (pane.batch()) |batch| {
        if (pane.expanded) {
            n = try writeCellPager(out, batch, pane, size.cols, body_rows);
        } else {
            n = try writeBatchBoxed(out, batch, .{
                .max_rows = tableDataRows(size),
                .term_cols = size.cols,
                .sel_row = pane.sel_row,
                .sel_col = pane.sel_col,
                .row_offset = pane.table_scroll,
            });
            if (pane.show_timer) {
                if (pane.elapsed) |elapsed| {
                    try printStats(out, elapsed, pane.result.?.scan_stats);
                    n += 1;
                }
            }
        }
    } else {
        n = try writeViewLines(out, view, size.cols, body_rows);
    }

    var i = n;
    while (i < body_rows) : (i += 1) try out.writeByte('\n');
    try writeRule(out, size.cols);
    if (pane.batch() != null) {
        if (pane.expanded) {
            try out.writeAll("\n Tab fold   ↑↓ scroll   ←→ cell   Enter run   Ctrl+O open\n");
        } else {
            try out.writeAll("\n Tab expand   ↑↓←→ cell   Enter run   Ctrl+O open   Ctrl+C quit\n");
        }
    } else {
        try out.writeAll("\n Enter run   Ctrl+O open   Ctrl+C quit\n");
    }
    const prompt = if (session.table_name.len == 0) "glacier" else session.table_name;
    try out.print("{s}> {s}", .{ prompt, sql });
    if (!expanded) try out.writeAll("\x1b[?25h");
}

fn writeRule(out: *std.Io.Writer, cols: u16) !void {
    var i: u16 = 0;
    while (i < cols) : (i += 1) try out.writeAll("─");
}

fn termSize() TermSize {
    const fallback = TermSize{ .cols = 80, .rows = 24 };
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi) return fallback;
    var wsz: std.posix.winsize = .{ .row = 0, .col = 0, .xpixel = 0, .ypixel = 0 };
    const fd: std.posix.fd_t = 1;
    if (builtin.os.tag == .linux) {
        const rc = std.os.linux.ioctl(fd, std.os.linux.T.IOCGWINSZ, @intFromPtr(&wsz));
        if (std.os.linux.errno(rc) != .SUCCESS) return fallback;
    } else {
        if (std.c.ioctl(fd, std.posix.T.IOCGWINSZ, &wsz) != 0) return fallback;
    }
    if (wsz.col == 0 or wsz.row == 0) return fallback;
    return .{ .cols = wsz.col, .rows = wsz.row };
}

fn typeName(dt: glacier.batch.DataType) []const u8 {
    return switch (dt) {
        .boolean => "boolean",
        .int32 => "int32",
        .int64 => "int64",
        .float32 => "float32",
        .float64 => "float64",
        .utf8 => "utf8",
        .timestamp => "timestamp",
        .timestamptz => "timestamptz",
        .uuid => "uuid",
        .decimal128 => "decimal",
    };
}

fn numericAlign(dt: glacier.batch.DataType) bool {
    return switch (dt) {
        .boolean, .int32, .int64, .float32, .float64, .timestamp, .timestamptz, .decimal128 => true,
        .utf8, .uuid => false,
    };
}

fn printResult(ctx: *RunCtx, batch: glacier.Batch) !void {
    const tty = std.Io.File.stdout().isTty(ctx.io) catch false;
    if (tty) {
        const size = termSize();
        const max_rows = @max(@as(usize, 20), @as(usize, size.rows) -| 6);
        try printBatchBoxed(ctx.out, batch, max_rows, size.cols);
    } else {
        try printBatch(ctx.out, batch);
    }
}

const cell_cap = 40;
const cell_min = 3;
const ellipsis = "…";

fn utf8Next(s: []const u8, i: usize) usize {
    const n = std.unicode.utf8ByteSequenceLength(s[i]) catch return 1;
    if (i + n > s.len) return 1;
    return n;
}

fn visibleWidth(s: []const u8) usize {
    var w: usize = 0;
    var i: usize = 0;
    while (i < s.len) {
        i += utf8Next(s, i);
        w += 1;
    }
    return w;
}

fn utf8PrefixCols(s: []const u8, cols: usize) []const u8 {
    var i: usize = 0;
    var c: usize = 0;
    while (i < s.len and c < cols) {
        i += utf8Next(s, i);
        c += 1;
    }
    return s[0..i];
}

/// Controls become spaces. Over-wide cells end in `…`. Width is display columns, not bytes.
fn fitCell(src: []const u8, max_cols: usize, buf: []u8) []const u8 {
    if (max_cols == 0 or buf.len == 0) return "";
    var o: usize = 0;
    var i: usize = 0;
    while (i < src.len and o < buf.len) {
        const b = src[i];
        if (b < 0x20 or b == 0x7f) {
            buf[o] = ' ';
            o += 1;
            i += 1;
            continue;
        }
        const n = utf8Next(src, i);
        if (o + n > buf.len) break;
        @memcpy(buf[o..][0..n], src[i..][0..n]);
        o += n;
        i += n;
    }
    const clean = buf[0..o];
    if (visibleWidth(clean) <= max_cols) return clean;
    const keep = max_cols -| 1;
    const prefix = utf8PrefixCols(clean, keep);
    if (prefix.len + ellipsis.len > buf.len) return prefix;
    @memcpy(buf[prefix.len..][0..ellipsis.len], ellipsis);
    return buf[0 .. prefix.len + ellipsis.len];
}

fn contentBudget(term_cols: u16, n_cols: usize) usize {
    const chrome = 1 + 3 * n_cols;
    const cols = @max(@as(usize, 40), @as(usize, term_cols));
    return @max(n_cols * cell_min, cols -| chrome);
}

fn shrinkWidths(widths: []usize, budget: usize) void {
    while (true) {
        var sum: usize = 0;
        var widest: usize = 0;
        var wi: usize = 0;
        for (widths, 0..) |w, i| {
            sum += w;
            if (w > widest) {
                widest = w;
                wi = i;
            }
        }
        if (sum <= budget or widest <= cell_min) return;
        widths[wi] -= 1;
    }
}

const WrapLine = struct { line: []const u8, next: usize };

/// One display row, wrapping at spaces. `max_cols` is terminal columns, not bytes.
fn nextWrap(src: []const u8, start: usize, max_cols: usize) ?WrapLine {
    const width = @max(max_cols, @as(usize, 1));
    if (start >= src.len) return null;
    if (src[start] == '\n') return .{ .line = src[start..start], .next = start + 1 };
    if (src[start] == '\r') {
        var n = start + 1;
        if (n < src.len and src[n] == '\n') n += 1;
        return .{ .line = src[start..start], .next = n };
    }

    var i = start;
    var cols: usize = 0;
    var last_space: ?usize = null;
    while (i < src.len) {
        const b = src[i];
        if (b == '\n') return .{ .line = src[start..i], .next = i + 1 };
        if (b == '\r') {
            var n = i + 1;
            if (n < src.len and src[n] == '\n') n += 1;
            return .{ .line = src[start..i], .next = n };
        }
        const n = if (b < 0x20 or b == 0x7f) 1 else utf8Next(src, i);
        if (cols + 1 > width) {
            if (last_space) |sp| return .{ .line = src[start..sp], .next = sp + 1 };
            if (i == start) return .{ .line = src[start .. start + n], .next = start + n };
            return .{ .line = src[start..i], .next = i };
        }
        if (b == ' ' or b == '\t') last_space = i;
        cols += 1;
        i += n;
    }
    return .{ .line = src[start..src.len], .next = src.len };
}

fn countWrap(src: []const u8, max_cols: usize) usize {
    if (src.len == 0) return 0;
    var n: usize = 0;
    var i: usize = 0;
    while (nextWrap(src, i, max_cols)) |w| {
        n += 1;
        if (w.next <= i) break;
        i = w.next;
    }
    return n;
}

fn skipWrap(src: []const u8, max_cols: usize, skip: usize) usize {
    var i: usize = 0;
    var n: usize = 0;
    while (n < skip) {
        const w = nextWrap(src, i, max_cols) orelse return src.len;
        if (w.next <= i) return src.len;
        i = w.next;
        n += 1;
    }
    return i;
}

fn writeSanitized(out: *std.Io.Writer, s: []const u8) !void {
    var i: usize = 0;
    while (i < s.len) {
        const b = s[i];
        if (b < 0x20 or b == 0x7f) {
            try out.writeByte(' ');
            i += 1;
            continue;
        }
        const n = utf8Next(s, i);
        try out.writeAll(s[i .. i + n]);
        i += n;
    }
}

fn writeClipped(out: *std.Io.Writer, line: []const u8, cols: usize) !void {
    if (cols == 0) return;
    if (visibleWidth(line) <= cols) {
        try out.writeAll(line);
        return;
    }
    try out.writeAll(utf8PrefixCols(line, cols));
}

fn writeViewLines(out: *std.Io.Writer, view: []const u8, cols: u16, max_rows: usize) !usize {
    var n: usize = 0;
    var it = std.mem.splitScalar(u8, view, '\n');
    while (it.next()) |line| {
        if (n >= max_rows) break;
        try writeClipped(out, line, cols);
        try out.writeByte('\n');
        n += 1;
    }
    return n;
}

const BoxOpts = struct {
    max_rows: usize,
    term_cols: u16,
    sel_row: ?usize = null,
    sel_col: ?usize = null,
    row_offset: usize = 0,
};

fn printBatchBoxed(out: *std.Io.Writer, batch: glacier.Batch, max_rows: usize, term_cols: u16) !void {
    _ = try writeBatchBoxed(out, batch, .{ .max_rows = max_rows, .term_cols = term_cols });
}

fn writeBatchBoxed(out: *std.Io.Writer, batch: glacier.Batch, opts: BoxOpts) !usize {
    if (batch.columns.len == 0) {
        try out.print("(0 columns, {d} rows)\n", .{batch.len});
        return 1;
    }
    if (batch.columns.len > 32) {
        try printBatch(out, batch);
        return @min(batch.len + 2, opts.max_rows + 2);
    }

    var widths: [32]usize = undefined;
    const n_cols = batch.columns.len;
    var fit_buf: [256]u8 = undefined;
    for (batch.columns, 0..) |col, i| {
        widths[i] = visibleWidth(fitCell(col.name, cell_cap, &fit_buf));
        widths[i] = @max(widths[i], visibleWidth(typeName(col.data_type)));
        widths[i] = @max(widths[i], cell_min);
    }
    const offset = @min(opts.row_offset, batch.len);
    const show = @min(opts.max_rows, batch.len - offset);
    var tmp: [256]u8 = undefined;
    var row: usize = offset;
    while (row < offset + show) : (row += 1) {
        for (batch.columns, 0..) |col, i| {
            const t = try cellText(col, row, &tmp);
            widths[i] = @max(widths[i], visibleWidth(fitCell(t, cell_cap, &fit_buf)));
        }
    }
    shrinkWidths(widths[0..n_cols], contentBudget(opts.term_cols, n_cols));

    var lines: usize = 0;
    try writeBoxRow(out, widths[0..n_cols], .top);
    lines += 1;
    try writeHeaderCells(out, batch, widths[0..n_cols], .name);
    lines += 1;
    try writeHeaderCells(out, batch, widths[0..n_cols], .typ);
    lines += 1;
    try writeBoxRow(out, widths[0..n_cols], .mid);
    lines += 1;
    row = offset;
    while (row < offset + show) : (row += 1) {
        try out.writeAll("│");
        for (batch.columns, 0..) |col, i| {
            const t = try cellText(col, row, &tmp);
            const shown = fitCell(t, widths[i], &fit_buf);
            const hi = opts.sel_row == row and opts.sel_col == i;
            try out.writeByte(' ');
            if (hi) try out.writeAll("\x1b[7m");
            try padCell(out, shown, widths[i], numericAlign(col.data_type));
            if (hi) try out.writeAll("\x1b[0m");
            try out.writeAll(" │");
        }
        try out.writeByte('\n');
        lines += 1;
    }
    try writeBoxRow(out, widths[0..n_cols], .bot);
    lines += 1;
    if (batch.len > show) {
        try out.print("… {d} more rows\n", .{batch.len - show});
        lines += 1;
    }
    try out.print("{d} rows\n", .{batch.len});
    lines += 1;
    return lines;
}

fn writeCellPager(
    out: *std.Io.Writer,
    batch: glacier.Batch,
    pane: *const ResultPane,
    term_cols: u16,
    body_rows: usize,
) !usize {
    if (batch.columns.len == 0 or batch.len == 0) {
        try out.writeAll("(empty)\n");
        return 1;
    }
    const col = batch.columns[pane.sel_col];
    var tmp: [256]u8 = undefined;
    const raw = try cellText(col, pane.sel_row, &tmp);
    const inner = pagerInnerWidth(term_cols);
    const vis = @max(@as(usize, 1), body_rows -| 4);
    var widths = [_]usize{inner};
    var fit_buf: [256]u8 = undefined;
    var title_buf: [128]u8 = undefined;
    const title = std.fmt.bufPrint(&title_buf, "{s}  ·  row {d}/{d}", .{
        col.name,
        pane.sel_row + 1,
        batch.len,
    }) catch title_buf[0..];

    var lines: usize = 0;
    try writeBoxRow(out, &widths, .top);
    lines += 1;
    try out.writeAll("│ ");
    try padCell(out, fitCell(title, inner, &fit_buf), inner, false);
    try out.writeAll(" │\n");
    lines += 1;
    try writeBoxRow(out, &widths, .mid);
    lines += 1;

    const total = countWrap(raw, inner);
    var i = skipWrap(raw, inner, pane.wrap_scroll);
    var shown: usize = 0;
    while (shown < vis) {
        const w = nextWrap(raw, i, inner) orelse break;
        try out.writeAll("│ ");
        try writeSanitized(out, w.line);
        const pad = inner -| visibleWidth(w.line);
        var p: usize = 0;
        while (p < pad) : (p += 1) try out.writeByte(' ');
        try out.writeAll(" │\n");
        if (w.next <= i) break;
        i = w.next;
        shown += 1;
        lines += 1;
    }
    if (shown == 0) {
        try out.writeAll("│ ");
        var p: usize = 0;
        while (p < inner) : (p += 1) try out.writeByte(' ');
        try out.writeAll(" │\n");
        shown = 1;
        lines += 1;
    }
    try writeBoxRow(out, &widths, .bot);
    lines += 1;
    if (pane.wrap_scroll + shown < total) {
        try out.print("  ↓ {d} more lines\n", .{total - pane.wrap_scroll - shown});
        lines += 1;
    }
    return lines;
}

const BoxKind = enum { top, mid, bot };

fn writeBoxRow(out: *std.Io.Writer, widths: []const usize, kind: BoxKind) !void {
    const left: []const u8 = switch (kind) {
        .top => "┌",
        .mid => "├",
        .bot => "└",
    };
    const mid: []const u8 = switch (kind) {
        .top => "┬",
        .mid => "┼",
        .bot => "┴",
    };
    const right: []const u8 = switch (kind) {
        .top => "┐",
        .mid => "┤",
        .bot => "┘",
    };
    try out.writeAll(left);
    for (widths, 0..) |w, i| {
        var k: usize = 0;
        while (k < w + 2) : (k += 1) try out.writeAll("─");
        try out.writeAll(if (i + 1 == widths.len) right else mid);
    }
    try out.writeByte('\n');
}

fn writeHeaderCells(
    out: *std.Io.Writer,
    batch: glacier.Batch,
    widths: []const usize,
    which: enum { name, typ },
) !void {
    try out.writeAll("│");
    for (batch.columns, 0..) |col, i| {
        const raw = if (which == .name) col.name else typeName(col.data_type);
        var buf: [256]u8 = undefined;
        const t = fitCell(raw, widths[i], &buf);
        try out.writeByte(' ');
        try padCell(out, t, widths[i], false);
        try out.writeAll(" │");
    }
    try out.writeByte('\n');
}

fn padCell(out: *std.Io.Writer, text: []const u8, width: usize, right: bool) !void {
    const w = visibleWidth(text);
    const pad = width -| w;
    if (right) {
        var i: usize = 0;
        while (i < pad) : (i += 1) try out.writeByte(' ');
        try out.writeAll(text);
    } else {
        try out.writeAll(text);
        var i: usize = 0;
        while (i < pad) : (i += 1) try out.writeByte(' ');
    }
}

fn cellText(col: glacier.Column, row: usize, buf: []u8) ![]const u8 {
    if (col.isNull(row)) return "NULL";
    return switch (col.data_type) {
        .boolean => std.fmt.bufPrint(buf, "{d}", .{col.bools[row]}),
        .int32 => std.fmt.bufPrint(buf, "{d}", .{col.i32s[row]}),
        .int64, .timestamp, .timestamptz => std.fmt.bufPrint(buf, "{d}", .{col.i64s[row]}),
        .float32 => std.fmt.bufPrint(buf, "{d}", .{col.f32s[row]}),
        .float64 => std.fmt.bufPrint(buf, "{d}", .{col.f64s[row]}),
        .utf8 => col.strAt(row),
        .uuid => blk: {
            const hex = std.fmt.bytesToHex(&col.uuids[row], .lower);
            break :blk std.fmt.bufPrint(buf, "{s}-{s}-{s}-{s}-{s}", .{
                hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32],
            });
        },
        .decimal128 => formatDecimalRepl(col.i128s[row], col.decimal_scale, buf),
    };
}

const RunCtx = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    out: *std.Io.Writer,
    timer: *bool,
};

const LineResult = enum { ok, quit, fail };

fn die(out: *std.Io.Writer) noreturn {
    out.flush() catch {};
    std.process.exit(1);
}

fn handleLine(session: *glacier.Session, ctx: *RunCtx, line: []const u8) !LineResult {
    if (line.len > 0 and line[0] == '.') return handleDot(session, ctx, line);

    const t0 = std.Io.Clock.Timestamp.now(ctx.io, .awake);
    var result = session.execute(line) catch |err| {
        const ge = session.lastError() orelse glacier.GlacierError.fromZig(err);
        try ctx.out.print("error: {s}\n", .{ge.message});
        return .fail;
    };
    defer result.deinit();
    const elapsed = t0.durationTo(std.Io.Clock.Timestamp.now(ctx.io, .awake));

    const batch = result.nextBatch() orelse {
        try ctx.out.print("(no rows)\n", .{});
        if (ctx.timer.*) try printStats(ctx.out, elapsed, result.scan_stats);
        return .ok;
    };
    try printResult(ctx, batch);
    if (ctx.timer.*) try printStats(ctx.out, elapsed, result.scan_stats);
    return .ok;
}

fn handleDot(session: *glacier.Session, ctx: *RunCtx, line: []const u8) !LineResult {
    const rest = std.mem.trim(u8, line[1..], " \t");
    if (eql(rest, "quit") or eql(rest, "exit")) return .quit;
    if (eql(rest, "help")) {
        try ctx.out.writeAll(usage_text);
        return .ok;
    }
    if (eql(rest, "schema")) {
        try printSchema(ctx.out, session.*);
        return .ok;
    }
    if (eql(rest, "timer on")) {
        ctx.timer.* = true;
        try ctx.out.print("timer on\n", .{});
        return .ok;
    }
    if (eql(rest, "timer off")) {
        ctx.timer.* = false;
        try ctx.out.print("timer off\n", .{});
        return .ok;
    }
    if (std.ascii.startsWithIgnoreCase(rest, "open ")) {
        const path = std.mem.trim(u8, rest[5..], " \t");
        if (path.len == 0) {
            try ctx.out.print("usage: .open PATH\n", .{});
            return .fail;
        }
        const next = glacier.Session.open(ctx.gpa, ctx.io, path) catch |err| {
            const ge = glacier.session.lastOpenError() orelse glacier.GlacierError.fromZig(err);
            try ctx.out.print("open failed: {s}\n", .{ge.message});
            return .fail;
        };
        session.close();
        session.* = next;
        try printBanner(ctx.out, session.*);
        return .ok;
    }
    try ctx.out.print("unknown command: .{s}\n", .{rest});
    return .fail;
}

fn parseArgs(
    argv: []const []const u8,
    commands: *std.ArrayList([]const u8),
    headers: *std.ArrayList([]const u8),
    gpa: std.mem.Allocator,
) !Args {
    var out: Args = .{};
    var i: usize = 0;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        if (eql(a, "-h") or eql(a, "--help")) {
            out.help = true;
        } else if (eql(a, "-v") or eql(a, "--version")) {
            out.version = true;
        } else if (eql(a, "--timer")) {
            out.timer = true;
        } else if (eql(a, "--no-timer")) {
            out.timer = false;
        } else if (eql(a, "--no-tui")) {
            out.tui = false;
        } else if (eql(a, "-c") or eql(a, "--command")) {
            i += 1;
            if (i >= argv.len) return error.MissingCommand;
            try commands.append(gpa, argv[i]);
        } else if (eql(a, "--catalog")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.catalog = argv[i];
        } else if (eql(a, "--warehouse")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.warehouse = argv[i];
        } else if (eql(a, "--token")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.token = argv[i];
        } else if (eql(a, "--header")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            try headers.append(gpa, argv[i]);
        } else if (eql(a, "--auth")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.auth = parseAuth(argv[i]) orelse return error.UnknownOption;
        } else if (eql(a, "--oauth-client-id")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.oauth_client_id = argv[i];
        } else if (eql(a, "--oauth-client-secret")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.oauth_client_secret = argv[i];
        } else if (eql(a, "--oauth-server")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.oauth_server = argv[i];
        } else if (eql(a, "--oauth-scope")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.oauth_scope = argv[i];
        } else if (eql(a, "--sigv4-service")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.sigv4_service = argv[i];
        } else if (eql(a, "--namespace")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.namespace = argv[i];
        } else if (eql(a, "--listen")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.listen = argv[i];
        } else if (eql(a, "--flight")) {
            i += 1;
            if (i >= argv.len) return error.MissingOptionValue;
            out.flight = argv[i];
        } else if (a.len > 0 and a[0] == '-' and !std.mem.eql(u8, a, "-")) {
            return error.UnknownOption;
        } else if (eql(a, "serve") and !out.serve and out.path == null) {
            out.serve = true;
        } else {
            if (out.path != null) return error.ExtraPath;
            out.path = a;
        }
    }
    out.commands = commands.items;
    out.headers = headers.items;
    return out;
}

fn parseAuth(s: []const u8) ?glacier.rest_catalog.Auth {
    if (eql(s, "auto")) return .auto;
    if (eql(s, "none")) return .none;
    if (eql(s, "bearer")) return .bearer;
    if (eql(s, "oauth2")) return .oauth2;
    if (eql(s, "sigv4")) return .sigv4;
    return null;
}

fn splitStatements(allocator: std.mem.Allocator, src: []const u8) ![][]const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer out.deinit(allocator);
    var start: usize = 0;
    var i: usize = 0;
    var in_str = false;
    while (i < src.len) : (i += 1) {
        const c = src[i];
        if (c == '\'') in_str = !in_str;
        if (in_str or c != ';') continue;
        const stmt = std.mem.trim(u8, src[start..i], " \t\r\n");
        if (stmt.len > 0) try out.append(allocator, stmt);
        start = i + 1;
    }
    const tail = std.mem.trim(u8, src[start..], " \t\r\n");
    if (tail.len > 0) try out.append(allocator, tail);
    return out.toOwnedSlice(allocator);
}

fn readAllStdin(gpa: std.mem.Allocator, io: std.Io) ![]u8 {
    var buf: [4096]u8 = undefined;
    var stdin_file = std.Io.File.stdin().readerStreaming(io, &buf);
    const stdin = &stdin_file.interface;
    var all: std.ArrayList(u8) = .empty;
    errdefer all.deinit(gpa);
    while (true) {
        const line = stdin.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => {
                try all.appendSlice(gpa, buf[0..]);
                continue;
            },
            else => return err,
        } orelse break;
        try all.appendSlice(gpa, line);
        try all.append(gpa, '\n');
    }
    return all.toOwnedSlice(gpa);
}

fn printBanner(out: *std.Io.Writer, session: glacier.Session) !void {
    if (session.path.len == 0) {
        try out.print("glacier {s}\nempty session  .help  .open PATH  .quit\n", .{glacier.version});
    } else {
        try out.print("glacier {s}\nconnected: {s} ({s})\n.help  .schema  .quit\n", .{
            glacier.version,
            session.table_name,
            session.path,
        });
    }
}

fn printStats(out: *std.Io.Writer, elapsed: std.Io.Clock.Duration, stats: glacier.session.ScanStats) !void {
    var tbuf: [32]u8 = undefined;
    const t = try formatDuration(elapsed.raw.toNanoseconds(), &tbuf);
    try out.print("-- {s}", .{t});
    if (peakRssBytes()) |rss| {
        var rbuf: [32]u8 = undefined;
        try out.print("  rss {s}", .{try formatBytes(rss, &rbuf)});
    }
    if (stats.files_opened > 0 or stats.files_pruned > 0) {
        try out.print("  opened {d}  pruned {d}", .{ stats.files_opened, stats.files_pruned });
    }
    try out.writeByte('\n');
}

fn peakRssBytes() ?u64 {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi) {
        return null;
    } else {
        const u = std.posix.getrusage(0);
        const raw: u64 = @intCast(@max(u.maxrss, 0));
        return switch (builtin.os.tag) {
            .macos, .ios, .tvos, .watchos, .visionos, .driverkit, .maccatalyst => raw,
            else => raw * 1024,
        };
    }
}

fn formatDuration(ns: i96, buf: []u8) ![]const u8 {
    const mag: u96 = if (ns < 0) 0 else @intCast(ns);
    if (mag < 1_000_000) {
        const us: u64 = @intCast(@divTrunc(mag, 1000));
        return std.fmt.bufPrint(buf, "{d} us", .{us});
    }
    if (mag < 1_000_000_000) {
        const ms = @as(f64, @floatFromInt(@as(u64, @intCast(mag)))) / 1_000_000.0;
        return std.fmt.bufPrint(buf, "{d:.2} ms", .{ms});
    }
    const s = @as(f64, @floatFromInt(@as(u64, @intCast(mag)))) / 1_000_000_000.0;
    return std.fmt.bufPrint(buf, "{d:.2} s", .{s});
}

fn formatBytes(n: u64, buf: []u8) ![]const u8 {
    if (n < 1024) return std.fmt.bufPrint(buf, "{d} B", .{n});
    const kib = @as(f64, @floatFromInt(n)) / 1024.0;
    if (kib < 1024) return std.fmt.bufPrint(buf, "{d:.1} KiB", .{kib});
    const mib = kib / 1024.0;
    if (mib < 1024) return std.fmt.bufPrint(buf, "{d:.1} MiB", .{mib});
    return std.fmt.bufPrint(buf, "{d:.1} GiB", .{mib / 1024.0});
}

fn eql(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn printSchema(out: *std.Io.Writer, session: glacier.Session) !void {
    if (session.schema_fields.len > 0) {
        for (session.schema_fields) |f| {
            try out.print("  {d}  {s}  {s}\n", .{ f.id, f.name, f.type_name });
        }
    } else if (session.files.len == 0) {
        try out.print("  empty session. .open PATH or SELECT 1\n", .{});
    } else {
        try out.print("  {d} data file(s). Run SELECT * LIMIT 1 to inspect columns.\n", .{session.files.len});
    }
}

fn printBatch(out: *std.Io.Writer, batch: glacier.Batch) !void {
    if (batch.columns.len == 0) {
        try out.print("(0 columns, {d} rows)\n", .{batch.len});
        return;
    }
    for (batch.columns, 0..) |col, i| {
        if (i > 0) try out.writeAll(" | ");
        try out.writeAll(col.name);
    }
    try out.writeByte('\n');
    var row: usize = 0;
    const max_print: usize = @min(batch.len, 50);
    while (row < max_print) : (row += 1) {
        for (batch.columns, 0..) |col, i| {
            if (i > 0) try out.writeAll(" | ");
            try writeCell(out, col, row);
        }
        try out.writeByte('\n');
    }
    if (batch.len > max_print) {
        try out.print("… {d} more rows\n", .{batch.len - max_print});
    }
    try out.print("{d} rows\n", .{batch.len});
}

fn writeCell(out: *std.Io.Writer, col: glacier.Column, row: usize) !void {
    switch (col.data_type) {
        .boolean => try out.print("{d}", .{col.bools[row]}),
        .int32 => try out.print("{d}", .{col.i32s[row]}),
        .int64, .timestamp, .timestamptz => try out.print("{d}", .{col.i64s[row]}),
        .float32 => try out.print("{d}", .{col.f32s[row]}),
        .float64 => try out.print("{d}", .{col.f64s[row]}),
        .utf8 => try out.writeAll(col.strAt(row)),
        .uuid => {
            const hex = std.fmt.bytesToHex(&col.uuids[row], .lower);
            try out.print("{s}-{s}-{s}-{s}-{s}", .{
                hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32],
            });
        },
        .decimal128 => {
            var buf: [80]u8 = undefined;
            try out.writeAll(try formatDecimalRepl(col.i128s[row], col.decimal_scale, &buf));
        },
    }
}

fn formatDecimalRepl(unscaled: i128, scale: i32, buf: []u8) ![]const u8 {
    if (scale <= 0) return std.fmt.bufPrint(buf, "{d}", .{unscaled});
    var digits_buf: [64]u8 = undefined;
    const digits = try std.fmt.bufPrint(&digits_buf, "{d}", .{unscaled});
    const neg = digits[0] == '-';
    const body = if (neg) digits[1..] else digits;
    const scale_usz: usize = @intCast(scale);
    var o: usize = 0;
    if (neg) {
        buf[o] = '-';
        o += 1;
    }
    if (body.len <= scale_usz) {
        buf[o] = '0';
        o += 1;
        buf[o] = '.';
        o += 1;
        var z: usize = 0;
        while (z < scale_usz - body.len) : (z += 1) {
            buf[o] = '0';
            o += 1;
        }
        @memcpy(buf[o..][0..body.len], body);
        o += body.len;
        return buf[0..o];
    }
    const split = body.len - scale_usz;
    @memcpy(buf[o..][0..split], body[0..split]);
    o += split;
    buf[o] = '.';
    o += 1;
    @memcpy(buf[o..][0..scale_usz], body[split..]);
    o += scale_usz;
    return buf[0..o];
}

test "parseArgs empty session and -c" {
    const gpa = std.testing.allocator;
    var cmds: std.ArrayList([]const u8) = .empty;
    defer cmds.deinit(gpa);
    var hdrs: std.ArrayList([]const u8) = .empty;
    defer hdrs.deinit(gpa);
    const a = try parseArgs(&.{ "-c", "SELECT 1" }, &cmds, &hdrs, gpa);
    try std.testing.expect(a.path == null);
    try std.testing.expectEqual(@as(usize, 1), a.commands.len);
    try std.testing.expectEqualStrings("SELECT 1", a.commands[0]);
    try std.testing.expect(a.timer);
}

test "parseArgs path and --no-timer" {
    const gpa = std.testing.allocator;
    var cmds: std.ArrayList([]const u8) = .empty;
    defer cmds.deinit(gpa);
    var hdrs: std.ArrayList([]const u8) = .empty;
    defer hdrs.deinit(gpa);
    const a = try parseArgs(&.{ "sales.parquet", "--no-timer", "-c", "SELECT COUNT(*)" }, &cmds, &hdrs, gpa);
    try std.testing.expectEqualStrings("sales.parquet", a.path.?);
    try std.testing.expect(!a.timer);
    try std.testing.expectEqualStrings("SELECT COUNT(*)", a.commands[0]);
}

test "parseArgs REST catalog flags" {
    const gpa = std.testing.allocator;
    var cmds: std.ArrayList([]const u8) = .empty;
    defer cmds.deinit(gpa);
    var hdrs: std.ArrayList([]const u8) = .empty;
    defer hdrs.deinit(gpa);
    const a = try parseArgs(&.{
        "--catalog",       "http://127.0.0.1:8181",
        "--warehouse",     "s3://wh",
        "--token",         "t",
        "--header",        "X-Goog-User-Project: p",
        "--auth",          "bearer",
        "default.prune",   "-c",
        "SELECT COUNT(*)",
    }, &cmds, &hdrs, gpa);
    try std.testing.expectEqualStrings("http://127.0.0.1:8181", a.catalog.?);
    try std.testing.expectEqualStrings("s3://wh", a.warehouse);
    try std.testing.expectEqualStrings("t", a.token.?);
    try std.testing.expectEqual(@as(usize, 1), a.headers.len);
    try std.testing.expectEqual(glacier.rest_catalog.Auth.bearer, a.auth);
    try std.testing.expectEqualStrings("default.prune", a.path.?);
}

test "parseArgs serve warehouse and listen" {
    const gpa = std.testing.allocator;
    var cmds: std.ArrayList([]const u8) = .empty;
    defer cmds.deinit(gpa);
    var hdrs: std.ArrayList([]const u8) = .empty;
    defer hdrs.deinit(gpa);
    const a = try parseArgs(&.{ "serve", "/tmp/wh", "--listen", "127.0.0.1:0", "--flight", "127.0.0.1:0" }, &cmds, &hdrs, gpa);
    try std.testing.expect(a.serve);
    try std.testing.expectEqualStrings("/tmp/wh", a.path.?);
    try std.testing.expectEqualStrings("127.0.0.1:0", a.listen);
    try std.testing.expectEqualStrings("127.0.0.1:0", a.flight);
}

test "splitStatements trims and skips empty" {
    const gpa = std.testing.allocator;
    const stmts = try splitStatements(gpa, "SELECT 1;  ; SELECT 2");
    defer gpa.free(stmts);
    try std.testing.expectEqual(@as(usize, 2), stmts.len);
    try std.testing.expectEqualStrings("SELECT 1", stmts[0]);
    try std.testing.expectEqualStrings("SELECT 2", stmts[1]);
}

test "typeName and numericAlign" {
    try std.testing.expectEqualStrings("int64", typeName(.int64));
    try std.testing.expectEqualStrings("decimal", typeName(.decimal128));
    try std.testing.expect(numericAlign(.int64));
    try std.testing.expect(!numericAlign(.utf8));
}

test "isDataFile parquet avro only" {
    try std.testing.expect(isDataFile("sales.parquet"));
    try std.testing.expect(isDataFile("SALES.PARQUET"));
    try std.testing.expect(isDataFile("sales.avro"));
    try std.testing.expect(!isDataFile("sales.csv"));
    try std.testing.expect(!isDataFile("metadata"));
}

test "utf8Pop deletes last character" {
    var buf = "café".*;
    var len: usize = buf.len;
    utf8Pop(&buf, &len);
    try std.testing.expectEqualStrings("caf", buf[0..len]);
    utf8Pop(&buf, &len);
    try std.testing.expectEqualStrings("ca", buf[0..len]);
}

test "insertByte backspaceAt deleteAt at cursor" {
    var buf: [16]u8 = undefined;
    var len: usize = 0;
    var cur: usize = 0;
    insertByte(&buf, &len, &cur, 'a');
    insertByte(&buf, &len, &cur, 'c');
    cur = 1;
    insertByte(&buf, &len, &cur, 'b');
    try std.testing.expectEqualStrings("abc", buf[0..len]);
    try std.testing.expectEqual(@as(usize, 2), cur);
    backspaceAt(&buf, &len, &cur);
    try std.testing.expectEqualStrings("ac", buf[0..len]);
    try std.testing.expectEqual(@as(usize, 1), cur);
    deleteAt(&buf, &len, &cur);
    try std.testing.expectEqualStrings("a", buf[0..len]);
}

test "decodeCsi arrows are not Esc" {
    try std.testing.expectEqual(Key.left, decodeCsi("", 'D'));
    try std.testing.expectEqual(Key.right, decodeCsi("", 'C'));
    try std.testing.expectEqual(Key.left, decodeCsi("1;5", 'D'));
    try std.testing.expectEqual(Key.home, decodeCsi("", 'H'));
    try std.testing.expectEqual(Key.end, decodeCsi("", 'F'));
    try std.testing.expectEqual(Key.delete, decodeCsi("3", '~'));
    try std.testing.expectEqual(Key.none, decodeCsi("200", '~'));
    try std.testing.expectEqual(Key.none, decodeCsi("", 'M'));
}

test "fitCell ellipsizes text and numbers" {
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("hello", fitCell("hello", 10, &buf));
    try std.testing.expectEqualStrings("abcd…", fitCell("abcdefghij", 5, &buf));
    try std.testing.expectEqualStrings("1234567…", fitCell("123456789012345", 8, &buf));
    try std.testing.expectEqualStrings("a b", fitCell("a\nb", 10, &buf));
    try std.testing.expectEqual(@as(usize, 4), visibleWidth("café"));
    try std.testing.expectEqual(@as(usize, 5), visibleWidth("abcd…"));
}

test "nextWrap word wrap and hard break" {
    const a = nextWrap("hello world", 0, 8).?;
    try std.testing.expectEqualStrings("hello", a.line);
    const b = nextWrap("hello world", a.next, 8).?;
    try std.testing.expectEqualStrings("world", b.line);
    try std.testing.expectEqual(b.next, "hello world".len);

    const c = nextWrap("abcdefghij", 0, 5).?;
    try std.testing.expectEqualStrings("abcde", c.line);
    const d = nextWrap("abcdefghij", c.next, 5).?;
    try std.testing.expectEqualStrings("fghij", d.line);

    const e = nextWrap("hello\n\nworld", 0, 20).?;
    try std.testing.expectEqualStrings("hello", e.line);
    const f = nextWrap("hello\n\nworld", e.next, 20).?;
    try std.testing.expectEqualStrings("", f.line);
    const g = nextWrap("hello\n\nworld", f.next, 20).?;
    try std.testing.expectEqualStrings("world", g.line);

    try std.testing.expectEqual(@as(usize, 4), visibleWidth(nextWrap("café au lait", 0, 6).?.line));
    try std.testing.expectEqual(@as(usize, 3), countWrap("one two three", 5));
    try std.testing.expect(countWrap("abcdefghij", 5) == 2);

    const article = "The game began development in 2010 , carrying over a large portion.";
    var i: usize = 0;
    while (nextWrap(article, i, 20)) |w| {
        try std.testing.expect(visibleWidth(w.line) <= 20);
        try std.testing.expect(w.next > i);
        i = w.next;
    }
}
