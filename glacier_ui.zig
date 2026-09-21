//! glacier-ui: Linux session workspace. Catalog tree, SQL, result grid.
//! Separate binary. Not in libglacier, WASM, or the wheel.

const std = @import("std");
const dvui = @import("dvui");
const glacier = @import("glacier");

pub const dvui_app: dvui.App = .{
    .config = .{ .startFn = &startOptions },
    .frameFn = appFrame,
    .initFn = appInit,
    .deinitFn = appDeinit,
};
pub const main = dvui.App.main;
pub const panic = dvui.App.panic;
pub const std_options: std.Options = .{
    .logFn = dvui.App.logFn,
};

const max_display_rows: usize = 500;
const max_cell_chars: usize = 80;
const file_filters = [_][]const u8{ "*.parquet", "*.avro", "*.glacier", "*.json" };

const ice = dvui.Color.fromHex("#7a9aa8");
const ice_bright = dvui.Color.fromHex("#9ec4d0");

var gpa: std.mem.Allocator = undefined;
var io: std.Io = undefined;
var session: ?glacier.Session = null;
var result_arena: std.heap.ArenaAllocator = undefined;
var tree_arena: std.heap.ArenaAllocator = undefined;
var started: bool = false;

var sql_buf: [32768]u8 = @splat(0);
var rest_buf: [512]u8 = @splat(0);
var status_buf: [512]u8 = @splat(0);
var status_len: usize = 0;
var elapsed_buf: [32]u8 = @splat(0);
var elapsed_text: []const u8 = "";
var query_error: bool = false;

var headers: []const []const u8 = &.{};
var types: []const []const u8 = &.{};
var cells: []const []const u8 = &.{};
var n_cols: usize = 0;
var n_rows: usize = 0;
var n_total: usize = 0;
var files_opened: usize = 0;
var files_pruned: usize = 0;
var has_result: bool = false;

var cats: []CatView = &.{};
var history: std.ArrayList([]const u8) = .empty;
var side_split: f32 = 0.26;
var editor_split: f32 = 0.40;
var argv_path: []const u8 = "";

const CatView = struct {
    name: []const u8,
    kind: []const u8,
    title: []const u8,
    tables: []TblView,
};

const TblView = struct {
    name: []const u8,
    catalog: []const u8,
    namespace: []const u8,
    label: []const u8,
};

fn startOptions() dvui.App.StartOptions {
    const init = dvui.App.main_init orelse @panic("glacier-ui needs process.Init");
    gpa = init.gpa;
    io = init.io;
    if (!started) {
        started = true;
        result_arena = std.heap.ArenaAllocator.init(gpa);
        tree_arena = std.heap.ArenaAllocator.init(gpa);
        var it = std.process.Args.Iterator.init(init.minimal.args);
        _ = it.next();
        if (it.next()) |p| argv_path = p;
        setSql("SHOW CATALOGS");
        setStatus("catalog session", .{});
    }
    return .{
        .size = .{ .w = 1280, .h = 800 },
        .min_size = .{ .w = 720, .h = 480 },
        .title = "Glacier",
        .org = "glacier",
        .persist_window_geometry = false,
        .gpa = gpa,
        .io = io,
    };
}

fn glacialTheme() dvui.Theme {
    var t = dvui.Theme.builtin.adwaita_dark;
    t.name = "Glacier";
    t.dark = true;
    t.corner = .round(2);
    t.focus = .fromHex("#7ec8d4");
    t.fill = .fromHex("#0e1a24");
    t.fill_hover = .fromHex("#173044");
    t.fill_press = .fromHex("#1e3c52");
    t.text = .fromHex("#d6eaf0");
    t.text_select = .fromHex("#1e4a5c");
    t.border = .fromHex("#3a6878");
    t.control = .{
        .fill = .fromHex("#163042"),
        .fill_hover = .fromHex("#1e4054"),
        .fill_press = .fromHex("#245468"),
        .text = .fromHex("#d6eaf0"),
        .border = .fromHex("#3a6878"),
    };
    t.window = .{
        .fill = .fromHex("#070d12"),
        .text = .fromHex("#d6eaf0"),
        .border = .fromHex("#1c3644"),
    };
    t.highlight = .{
        .fill = .fromHex("#2a6a7a"),
        .fill_hover = .fromHex("#348090"),
        .fill_press = .fromHex("#1e5868"),
        .text = .fromHex("#f0fafc"),
        .border = .fromHex("#7ec8d4"),
    };
    t.err = .{
        .fill = .fromHex("#8a3a38"),
        .text = .fromHex("#f0e4e0"),
        .border = .fromHex("#c45c4a"),
    };
    return t;
}

fn appInit(win: *dvui.Window) !void {
    win.themeSet(glacialTheme());
    if (argv_path.len > 0) {
        openReplace(argv_path);
        setSql("SELECT * LIMIT 50");
        runSql();
    } else {
        openEmptySession();
        runSql();
    }
}

fn appDeinit(_: *dvui.Window) void {
    if (session) |*s| s.close();
    session = null;
    for (history.items) |h| gpa.free(h);
    history.deinit(gpa);
    if (started) {
        result_arena.deinit();
        tree_arena.deinit();
    }
}

fn appFrame() !dvui.App.Result {
    consumeRunKeys();

    var root = dvui.box(@src(), .{ .dir = .vertical }, .{
        .expand = .both,
        .background = true,
        .style = .window,
    });
    defer root.deinit();

    if (drawChrome()) return .close;

    var split = dvui.paned(@src(), .{
        .direction = .horizontal,
        .collapsed_size = 220,
        .split_ratio = &side_split,
        .handle_size = 5,
        .uncollapse_ratio = 0.26,
    }, .{ .expand = .both });
    defer split.deinit();

    if (split.showFirst()) drawSidebar();
    if (split.showSecond()) drawWorkspace();

    return .ok;
}

fn consumeRunKeys() void {
    for (dvui.events()) |*e| {
        if (e.handled) continue;
        switch (e.evt) {
            .key => |ke| {
                if (ke.action != .down and ke.action != .repeat) continue;
                const run = ke.code == .f5 or (ke.code == .enter and ke.mod.control());
                if (!run) continue;
                e.handled = true;
                runSql();
            },
            else => {},
        }
    }
}

fn drawChrome() bool {
    var chrome = dvui.box(@src(), .{ .dir = .vertical }, .{
        .expand = .horizontal,
        .background = true,
        .style = .window,
        .border = .{ .x = 0, .y = 0, .w = 0, .h = 1 },
    });
    defer chrome.deinit();

    var header = dvui.box(@src(), .{ .dir = .horizontal }, .{
        .expand = .horizontal,
        .padding = .{ .x = 12, .y = 8, .w = 12, .h = 8 },
    });
    var close = false;
    {
        defer header.deinit();
        dvui.label(@src(), "GLACIER", .{}, .{ .font = .theme(.title), .gravity_y = 0.5 });
        dvui.label(@src(), "{s}", .{glacier.version}, .{
            .gravity_y = 0.5,
            .color_text = .{ .color = ice },
            .font = .theme(.mono),
        });
        dvui.label(@src(), "{s}", .{sessionSummary()}, .{
            .gravity_y = 0.5,
            .color_text = .{ .color = ice_bright },
            .font = .theme(.mono),
        });
        _ = dvui.spacer(@src(), .{ .expand = .horizontal });
        if (dvui.button(@src(), "Run  F5", .{}, .{ .style = .highlight })) runSql();
        if (dvui.button(@src(), "Exit", .{}, .{})) close = true;
    }
    drawStatus();
    return close;
}

fn drawSidebar() void {
    var pane = dvui.box(@src(), .{ .dir = .vertical }, .{
        .expand = .both,
        .background = true,
        .style = .window,
        .padding = .{ .x = 8, .y = 8, .w = 8, .h = 8 },
        .border = .{ .x = 0, .y = 0, .w = 1, .h = 0 },
    });
    defer pane.deinit();

    dvui.label(@src(), "SESSION", .{}, .{
        .font = .theme(.heading),
        .color_text = .{ .color = ice_bright },
    });
    {
        var row = dvui.box(@src(), .{ .dir = .horizontal }, .{ .expand = .horizontal });
        defer row.deinit();
        if (dvui.button(@src(), "New", .{}, .{ .expand = .horizontal })) {
            openEmptySession();
            setSql("SHOW CATALOGS");
            runSql();
        }
        if (dvui.button(@src(), "File", .{}, .{ .expand = .horizontal })) pickAndOpen(.replace_file);
        if (dvui.button(@src(), "Lake", .{}, .{ .expand = .horizontal })) pickAndOpen(.replace_folder);
    }
    dvui.label(@src(), "into this session", .{}, .{ .color_text = .{ .color = ice } });
    {
        var row = dvui.box(@src(), .{ .dir = .horizontal }, .{ .expand = .horizontal });
        defer row.deinit();
        if (dvui.button(@src(), "Attach file", .{}, .{ .expand = .horizontal })) pickAndOpen(.attach_file);
        if (dvui.button(@src(), "Attach lake", .{}, .{ .expand = .horizontal })) pickAndOpen(.attach_folder);
    }
    {
        var row = dvui.box(@src(), .{ .dir = .horizontal }, .{ .expand = .horizontal });
        defer row.deinit();
        var rest_te = dvui.textEntry(@src(), .{
            .text = .{ .buffer = &rest_buf },
            .placeholder = "REST http(s)://",
        }, .{ .expand = .horizontal, .font = .theme(.mono) });
        defer rest_te.deinit();
        if (rest_te.enter_pressed or dvui.button(@src(), "REST", .{}, .{})) {
            const uri = trimText(rest_te.textGet());
            if (uri.len > 0) attachOrOpen(uri);
        }
    }

    _ = dvui.separator(@src(), .{ .expand = .horizontal, .margin = .{ .x = 0, .y = 8, .w = 0, .h = 8 } });
    dvui.label(@src(), "CATALOGS", .{}, .{
        .font = .theme(.heading),
        .color_text = .{ .color = ice_bright },
    });

    var scroll = dvui.scrollArea(@src(), .{}, .{ .expand = .both, .background = false });
    defer scroll.deinit();

    if (cats.len == 0) {
        dvui.label(@src(), "Nothing attached.\nNew / File / Lake, or ATTACH in SQL.", .{}, .{
            .color_text = .{ .color = ice },
        });
    }

    for (cats, 0..) |cat, i| {
        if (dvui.expander(@src(), cat.title, .{ .default_expanded = true }, .{
            .expand = .horizontal,
            .id_extra = i,
        })) {
            {
                var row = dvui.box(@src(), .{ .dir = .horizontal }, .{ .expand = .horizontal, .id_extra = i });
                defer row.deinit();
                dvui.label(@src(), "{s}", .{cat.kind}, .{
                    .color_text = .{ .color = ice },
                    .font = .theme(.mono),
                    .id_extra = i,
                });
                if (dvui.button(@src(), "USE", .{}, .{ .id_extra = i })) {
                    runTextFmt("USE {s}", .{cat.name});
                }
            }
            if (cat.tables.len == 0) {
                dvui.label(@src(), "  (no tables)", .{}, .{
                    .color_text = .{ .color = ice },
                    .id_extra = i,
                });
            }
            for (cat.tables, 0..) |tbl, j| {
                const extra = i * 64 + j + 1;
                if (dvui.button(@src(), tbl.label, .{}, .{
                    .expand = .horizontal,
                    .id_extra = extra,
                })) {
                    previewTable(tbl);
                }
            }
        }
    }

    if (history.items.len > 0) {
        _ = dvui.separator(@src(), .{ .expand = .horizontal, .margin = .{ .x = 0, .y = 8, .w = 0, .h = 8 } });
        if (dvui.expander(@src(), "HISTORY", .{ .default_expanded = false }, .{ .expand = .horizontal })) {
            var k: usize = history.items.len;
            while (k > 0) {
                k -= 1;
                const item = history.items[k];
                const shown = if (item.len > 42) item[0..42] else item;
                if (dvui.button(@src(), shown, .{}, .{
                    .expand = .horizontal,
                    .id_extra = k,
                    .font = .theme(.mono),
                })) {
                    setSql(item);
                }
            }
        }
    }
}

fn drawWorkspace() void {
    var split = dvui.paned(@src(), .{
        .direction = .vertical,
        .collapsed_size = 140,
        .split_ratio = &editor_split,
        .handle_size = 5,
        .uncollapse_ratio = 0.40,
    }, .{ .expand = .both });
    defer split.deinit();

    if (split.showFirst()) drawEditor();
    if (split.showSecond()) drawResults();
}

fn drawEditor() void {
    var pane = dvui.box(@src(), .{ .dir = .vertical }, .{
        .expand = .both,
        .background = true,
        .padding = .{ .x = 10, .y = 8, .w = 10, .h = 6 },
    });
    defer pane.deinit();

    {
        var row = dvui.box(@src(), .{ .dir = .horizontal }, .{ .expand = .horizontal });
        defer row.deinit();
        dvui.label(@src(), "QUERY", .{}, .{
            .font = .theme(.heading),
            .gravity_y = 0.5,
            .color_text = .{ .color = ice_bright },
        });
        dvui.label(@src(), "one statement  ·  Ctrl+Enter", .{}, .{
            .gravity_y = 0.5,
            .color_text = .{ .color = ice },
        });
        _ = dvui.spacer(@src(), .{ .expand = .horizontal });
        if (dvui.button(@src(), "SHOW CATALOGS", .{}, .{})) runText("SHOW CATALOGS");
        if (dvui.button(@src(), "SHOW TABLES", .{}, .{})) runText("SHOW TABLES");
    }

    var sql_te = dvui.textEntry(@src(), .{
        .text = .{ .buffer = &sql_buf },
        .multiline = true,
        .placeholder = "SQL against the catalog session",
    }, .{
        .expand = .both,
        .font = .theme(.mono),
        .min_size_content = .{ .w = 200, .h = 120 },
    });
    defer sql_te.deinit();
}

fn drawResults() void {
    var pane = dvui.box(@src(), .{ .dir = .vertical }, .{
        .expand = .both,
        .background = true,
        .style = .window,
        .padding = .{ .x = 10, .y = 6, .w = 10, .h = 8 },
        .border = .{ .x = 0, .y = 1, .w = 0, .h = 0 },
    });
    defer pane.deinit();

    {
        var row = dvui.box(@src(), .{ .dir = .horizontal }, .{ .expand = .horizontal });
        defer row.deinit();
        dvui.label(@src(), "RESULT", .{}, .{
            .font = .theme(.heading),
            .gravity_y = 0.5,
            .color_text = .{ .color = ice_bright },
        });
        if (has_result) {
            dvui.label(@src(), "{s}", .{resultSummary()}, .{
                .gravity_y = 0.5,
                .font = .theme(.mono),
                .color_text = .{ .color = if (query_error) .fromHex("#e8b4ae") else ice_bright },
            });
            _ = dvui.spacer(@src(), .{ .expand = .horizontal });
            if (n_cols > 0 and dvui.button(@src(), "Copy", .{}, .{})) copyTsv();
        }
    }

    if (query_error) {
        dvui.label(@src(), "{s}", .{status_buf[0..status_len]}, .{
            .style = .err,
            .expand = .horizontal,
            .background = true,
            .padding = .all(8),
        });
        return;
    }

    if (!has_result) {
        dvui.label(@src(), "Pick a table in the session tree, or run SQL.\nF5 / Ctrl+Enter runs the statement in the editor.", .{}, .{
            .color_text = .{ .color = ice },
        });
        return;
    }

    if (n_cols == 0) {
        dvui.label(@src(), "Statement finished. No result grid (DDL / empty).", .{}, .{
            .color_text = .{ .color = ice },
        });
        return;
    }

    var grid = dvui.grid(@src(), .{ .rows = n_rows }, .{ .expand = .both });
    defer grid.deinit();

    for (headers, 0..) |h, c| {
        var hdr = grid.colHeader(.{ .col = c }, .{ .id_extra = c });
        defer hdr.deinit();
        var col = dvui.box(@src(), .{ .dir = .vertical }, .{ .id_extra = c });
        defer col.deinit();
        dvui.label(@src(), "{s}", .{h}, .{ .id_extra = c, .font = .theme(.heading) });
        if (c < types.len) {
            dvui.label(@src(), "{s}", .{types[c]}, .{
                .id_extra = c,
                .font = .theme(.mono),
                .color_text = .{ .color = ice },
            });
        }
    }

    var r: usize = 0;
    while (r < n_rows) : (r += 1) {
        var c: usize = 0;
        while (c < n_cols) : (c += 1) {
            const extra = r * n_cols + c;
            var cell = grid.cell(.{ .col = c, .row = r }, .{ .id_extra = extra });
            defer cell.deinit();
            dvui.label(@src(), "{s}", .{cells[extra]}, .{
                .id_extra = extra,
                .font = .theme(.mono),
            });
        }
    }
}

fn drawStatus() void {
    var bar = dvui.box(@src(), .{ .dir = .horizontal }, .{
        .expand = .horizontal,
        .padding = .{ .x = 12, .y = 4, .w = 12, .h = 6 },
        .border = .{ .x = 0, .y = 1, .w = 0, .h = 0 },
    });
    defer bar.deinit();
    dvui.label(@src(), "{s}", .{status_buf[0..status_len]}, .{
        .color_text = .{ .color = if (query_error) .fromHex("#e8b4ae") else ice_bright },
        .font = .theme(.mono),
    });
}

fn sessionSummary() []const u8 {
    const sess = if (session) |*s| s else return "no session";
    const cat = if (sess.default_catalog.len > 0) sess.default_catalog else "files";
    const path = sess.path;
    if (path.len == 0) {
        return std.fmt.bufPrint(&chrome_scratch, "{s}  empty", .{cat}) catch cat;
    }
    const base = std.fs.path.basename(path);
    return std.fmt.bufPrint(&chrome_scratch, "{s}  {s}", .{ cat, base }) catch cat;
}

var chrome_scratch: [256]u8 = undefined;
var summary_scratch: [256]u8 = undefined;

fn resultSummary() []const u8 {
    const shown = if (n_total > n_rows)
        std.fmt.bufPrint(&summary_scratch, "{d} rows ({d} shown)  {d} cols  {s}  opened {d}  pruned {d}", .{
            n_total, n_rows, n_cols, elapsed_text, files_opened, files_pruned,
        }) catch "rows"
    else
        std.fmt.bufPrint(&summary_scratch, "{d} rows  {d} cols  {s}  opened {d}  pruned {d}", .{
            n_total, n_cols, elapsed_text, files_opened, files_pruned,
        }) catch "rows";
    return shown;
}

fn trimText(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, " \t\r\n");
}

fn sqlText() []const u8 {
    const end = std.mem.indexOfScalar(u8, &sql_buf, 0) orelse sql_buf.len;
    return trimText(sql_buf[0..end]);
}

fn setSql(s: []const u8) void {
    @memset(&sql_buf, 0);
    const n = @min(s.len, sql_buf.len - 1);
    @memcpy(sql_buf[0..n], s[0..n]);
}

fn setStatus(comptime fmt: []const u8, args: anytype) void {
    const msg = std.fmt.bufPrint(&status_buf, fmt, args) catch {
        const fallback = "status too long";
        @memcpy(status_buf[0..fallback.len], fallback);
        status_len = fallback.len;
        return;
    };
    status_len = msg.len;
    if (status_len < status_buf.len) status_buf[status_len] = 0;
}

const PickKind = enum { replace_file, replace_folder, attach_file, attach_folder };

fn pickAndOpen(kind: PickKind) void {
    const path = switch (kind) {
        .replace_file, .attach_file => dvui.dialogNativeFileOpen(gpa, .{
            .title = if (kind == .attach_file) "Attach file" else "Open file",
            .filters = &file_filters,
            .filter_description = "Parquet / Avro / Glacier / Iceberg",
        }) catch {
            failStatus("file dialog failed", .{});
            return;
        },
        .replace_folder, .attach_folder => dvui.dialogNativeFolderSelect(gpa, .{
            .title = if (kind == .attach_folder) "Attach lake" else "Open lake",
        }) catch {
            failStatus("folder dialog failed", .{});
            return;
        },
    };
    if (path) |p| {
        defer gpa.free(p);
        switch (kind) {
            .replace_file, .replace_folder => {
                openReplace(p);
                setSql("SELECT * LIMIT 50");
                runSql();
            },
            .attach_file, .attach_folder => attachOrOpen(p),
        }
    }
}

fn openEmptySession() void {
    if (session) |*s| s.close();
    session = glacier.Session.openEmpty(gpa, io) catch |err| {
        failStatus("open failed: {s}", .{glacier.err.staticMessage(err)});
        return;
    };
    query_error = false;
    setStatus("empty catalog session", .{});
    refreshTree();
}

fn openReplace(path: []const u8) void {
    if (session) |*s| s.close();
    session = null;
    session = glacier.Session.open(gpa, io, path) catch {
        const ge = glacier.session.lastOpenError();
        failStatus("open failed: {s}", .{if (ge) |e| e.message else "error"});
        return;
    };
    query_error = false;
    setStatus("opened {s}", .{path});
    refreshTree();
}

fn attachOrOpen(path: []const u8) void {
    if (session == null) {
        openReplace(path);
        setSql("SHOW CATALOGS");
        runSql();
        return;
    }
    var name_buf: [64]u8 = undefined;
    const name = identFromPath(path, &name_buf);
    var esc_buf: [1024]u8 = undefined;
    const escaped = escapeSql(path, &esc_buf);
    var stmt_buf: [1200]u8 = undefined;
    const stmt = std.fmt.bufPrint(&stmt_buf, "ATTACH '{s}' AS {s}", .{ escaped, name }) catch {
        failStatus("ATTACH too long", .{});
        return;
    };
    setSql(stmt);
    runSql();
}

fn identFromPath(path: []const u8, buf: []u8) []const u8 {
    const stem = std.fs.path.stem(path);
    var i: usize = 0;
    for (stem) |ch| {
        if (i >= buf.len) break;
        if (std.ascii.isAlphanumeric(ch)) {
            buf[i] = std.ascii.toLower(ch);
            i += 1;
        } else if (i > 0 and buf[i - 1] != '_') {
            buf[i] = '_';
            i += 1;
        }
    }
    if (i == 0) return "extra";
    if (buf[i - 1] == '_') i -= 1;
    return buf[0..i];
}

fn escapeSql(s: []const u8, buf: []u8) []const u8 {
    var i: usize = 0;
    for (s) |ch| {
        if (i >= buf.len) break;
        buf[i] = ch;
        i += 1;
        if (ch == '\'' and i < buf.len) {
            buf[i] = '\'';
            i += 1;
        }
    }
    return buf[0..i];
}

fn previewTable(tbl: TblView) void {
    var buf: [256]u8 = undefined;
    const sql = if (tbl.namespace.len > 0)
        std.fmt.bufPrint(&buf, "SELECT * FROM {s}.{s}.{s} LIMIT 50", .{ tbl.catalog, tbl.namespace, tbl.name }) catch return
    else
        std.fmt.bufPrint(&buf, "SELECT * FROM {s}.{s} LIMIT 50", .{ tbl.catalog, tbl.name }) catch return;
    setSql(sql);
    runSql();
}

fn runText(sql: []const u8) void {
    setSql(sql);
    runSql();
}

fn runTextFmt(comptime fmt: []const u8, args: anytype) void {
    var buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, fmt, args) catch return;
    runText(sql);
}

fn runSql() void {
    const sql = sqlText();
    if (sql.len == 0) {
        failStatus("type SQL", .{});
        return;
    }
    var sess = if (session) |*s| s else {
        failStatus("no session", .{});
        return;
    };
    const t0 = std.Io.Clock.Timestamp.now(io, .awake);
    var result = sess.execute(sql) catch |err| {
        const ge = sess.lastError() orelse glacier.GlacierError.fromZig(err);
        failStatus("{s}", .{ge.message});
        clearGrid();
        has_result = true;
        refreshTree();
        return;
    };
    defer result.deinit();
    const ns = t0.durationTo(std.Io.Clock.Timestamp.now(io, .awake)).raw.toNanoseconds();
    elapsed_text = formatDuration(ns, &elapsed_buf) catch "";
    files_opened = result.scan_stats.files_opened;
    files_pruned = result.scan_stats.files_pruned;
    query_error = false;
    captureBatch(result.batch);
    pushHistory(sql);
    refreshTree();
}

fn failStatus(comptime fmt: []const u8, args: anytype) void {
    query_error = true;
    setStatus(fmt, args);
}

fn pushHistory(sql: []const u8) void {
    if (history.items.len > 0 and std.mem.eql(u8, history.items[history.items.len - 1], sql)) return;
    const copy = gpa.dupe(u8, sql) catch return;
    history.append(gpa, copy) catch {
        gpa.free(copy);
        return;
    };
    if (history.items.len > 24) {
        gpa.free(history.orderedRemove(0));
    }
}

fn refreshTree() void {
    _ = tree_arena.reset(.retain_capacity);
    cats = &.{};
    const sess = if (session) |*s| s else return;
    const a = tree_arena.allocator();
    const default_cat = sess.default_catalog;
    var names: [][]const u8 = &.{};
    var kinds: [][]const u8 = &.{};
    {
        var result = sess.execute("SHOW CATALOGS") catch return;
        defer result.deinit();
        const batch = result.batch;
        names = a.alloc([]const u8, batch.len) catch return;
        kinds = a.alloc([]const u8, batch.len) catch return;
        var i: usize = 0;
        while (i < batch.len) : (i += 1) {
            names[i] = a.dupe(u8, batch.columns[0].strAt(i)) catch "";
            kinds[i] = if (batch.columns.len > 1) a.dupe(u8, batch.columns[1].strAt(i)) catch "catalog" else "catalog";
        }
    }
    const list = a.alloc(CatView, names.len) catch return;
    for (names, kinds, 0..) |name, kind, i| {
        const use = std.ascii.eqlIgnoreCase(name, default_cat);
        const title = if (use)
            (a.dupe(u8, std.fmt.bufPrint(&chrome_scratch, "{s}  (use)", .{name}) catch name) catch name)
        else
            name;
        list[i] = .{
            .name = name,
            .kind = kind,
            .title = title,
            .tables = loadTables(a, sess, name),
        };
    }
    cats = list;
}

fn loadTables(a: std.mem.Allocator, sess: *glacier.Session, cat: []const u8) []TblView {
    var stmt_buf: [128]u8 = undefined;
    const stmt = std.fmt.bufPrint(&stmt_buf, "SHOW TABLES FROM {s}", .{cat}) catch return &.{};
    var result = sess.execute(stmt) catch return &.{};
    defer result.deinit();
    const batch = result.batch;
    if (batch.len == 0) return &.{};
    const list = a.alloc(TblView, batch.len) catch return &.{};
    var i: usize = 0;
    while (i < batch.len) : (i += 1) {
        const ns = if (batch.columns.len > 2) batch.columns[2].strAt(i) else "";
        const tbl_name = a.dupe(u8, batch.columns[0].strAt(i)) catch "?";
        const ns_owned = a.dupe(u8, ns) catch "";
        const label = if (ns_owned.len == 0)
            (a.dupe(u8, std.fmt.bufPrint(&chrome_scratch, "  {s}", .{tbl_name}) catch tbl_name) catch tbl_name)
        else
            (a.dupe(u8, std.fmt.bufPrint(&chrome_scratch, "  {s}.{s}", .{ ns_owned, tbl_name }) catch tbl_name) catch tbl_name);
        list[i] = .{
            .name = tbl_name,
            .catalog = a.dupe(u8, if (batch.columns.len > 1) batch.columns[1].strAt(i) else cat) catch cat,
            .namespace = ns_owned,
            .label = label,
        };
    }
    return list;
}

fn clearGrid() void {
    _ = result_arena.reset(.retain_capacity);
    headers = &.{};
    types = &.{};
    cells = &.{};
    n_cols = 0;
    n_rows = 0;
    n_total = 0;
}

fn captureBatch(batch: glacier.Batch) void {
    _ = result_arena.reset(.retain_capacity);
    const a = result_arena.allocator();
    has_result = true;
    n_cols = batch.columns.len;
    n_total = batch.len;
    n_rows = @min(batch.len, max_display_rows);
    if (n_cols == 0) {
        headers = &.{};
        types = &.{};
        cells = &.{};
        setStatus("{s}  {d} rows", .{ elapsed_text, n_total });
        return;
    }
    const hdrs = a.alloc([]const u8, n_cols) catch return oomGrid();
    const typs = a.alloc([]const u8, n_cols) catch return oomGrid();
    const vals = a.alloc([]const u8, n_rows * n_cols) catch return oomGrid();
    for (batch.columns, 0..) |col, c| {
        hdrs[c] = a.dupe(u8, col.name) catch "?";
        typs[c] = a.dupe(u8, @tagName(col.data_type)) catch "?";
    }
    var tmp: [256]u8 = undefined;
    var clip: [max_cell_chars]u8 = undefined;
    for (0..n_rows) |r| {
        for (batch.columns, 0..) |col, c| {
            const t = cellText(col, r, &tmp) catch "?";
            vals[r * n_cols + c] = a.dupe(u8, clipCell(t, &clip)) catch "?";
        }
    }
    headers = hdrs;
    types = typs;
    cells = vals;
    setStatus("{s}  {d} rows  {d} cols", .{ elapsed_text, n_total, n_cols });
}

fn oomGrid() void {
    n_cols = 0;
    n_rows = 0;
    failStatus("oom", .{});
}

fn copyTsv() void {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    for (headers, 0..) |h, c| {
        if (c > 0) out.append(gpa, '\t') catch return;
        out.appendSlice(gpa, h) catch return;
    }
    out.append(gpa, '\n') catch return;
    var r: usize = 0;
    while (r < n_rows) : (r += 1) {
        var c: usize = 0;
        while (c < n_cols) : (c += 1) {
            if (c > 0) out.append(gpa, '\t') catch return;
            out.appendSlice(gpa, cells[r * n_cols + c]) catch return;
        }
        out.append(gpa, '\n') catch return;
    }
    dvui.clipboardTextSet(out.items);
    setStatus("copied {d} rows", .{n_rows});
    query_error = false;
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

fn clipCell(s: []const u8, buf: []u8) []const u8 {
    if (s.len <= buf.len) return s;
    const n = buf.len;
    const keep = n - 3;
    @memcpy(buf[0..keep], s[0..keep]);
    buf[keep] = '.';
    buf[keep + 1] = '.';
    buf[keep + 2] = '.';
    return buf[0..n];
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
        .decimal128 => formatDecimal(col.i128s[row], col.decimal_scale, buf),
    };
}

fn formatDecimal(unscaled: i128, scale: i32, buf: []u8) ![]const u8 {
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
