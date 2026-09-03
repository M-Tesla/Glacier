//! SQL surface:
//! SELECT [DISTINCT] … [FROM t | (SELECT …) [AS a] [INNER] JOIN t2 | (SELECT …) [AS b] ON a.x = b.y]
//!   [WHERE bool] [GROUP BY cols] [HAVING bool]
//!   [ORDER BY col [ASC|DESC], …] [LIMIT n] [OFFSET m]
//! WITH t AS (SELECT …) [, …] SELECT …
//! COPY TO 'file.glacier'
//! COPY (SELECT …) TO 'file.glacier'
//! Window: agg/ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD OVER ([PARTITION BY …] [ORDER BY …] [ROWS|RANGE …])
//! Literals: `SELECT 1` / `SELECT NULL` (no FROM). Escalars: abs, round, cast, coalesce, CASE.
//! WHERE: comparisons, `IS [NOT] NULL`, `LIKE`, `IN (lits | SELECT)`, `BETWEEN`, scalar subquery.
//! UNION [ALL] of two SELECTs (same schema). Correlated subquery → UnsupportedSql.
//! JOIN: [INNER] JOIN / LEFT [OUTER] JOIN / RIGHT [OUTER] JOIN / FULL [OUTER] JOIN … ON eq.
//! USING / NATURAL / comma-join → UnsupportedJoin. EXISTS / recursive CTE / named WINDOW → UnsupportedSql.

const std = @import("std");

pub const Error = error{
    InvalidSyntax,
    UnexpectedEof,
    InvalidNumber,
    UnsupportedSql,
    UnsupportedJoin,
};

const ParseError = Error || std.mem.Allocator.Error;

pub const CmpOp = enum { eq, ne, lt, le, gt, ge };

pub const Literal = union(enum) {
    int: i64,
    float: f64,
    string: []const u8,
    null,
};

pub const AggKind = enum { count, sum, avg, min, max };

pub const Agg = struct {
    kind: AggKind,
    /// null means `*` (COUNT only).
    arg: ?[]const u8,
    alias: ?[]const u8,
};

pub const CmpLeft = union(enum) {
    column: []const u8,
    agg: struct {
        kind: AggKind,
        arg: ?[]const u8,
    },
};

pub const Cmp = struct {
    op: CmpOp,
    left: CmpLeft,
    literal: Literal,
};

pub const IsNull = struct {
    left: CmpLeft,
    negated: bool,
};

pub const Like = struct {
    left: CmpLeft,
    pattern: []const u8,
    negated: bool,
};

pub const InList = struct {
    left: CmpLeft,
    values: []const Literal,
    negated: bool,
};

pub const InQuery = struct {
    left: CmpLeft,
    query: *Query,
    negated: bool,
};

pub const CmpQuery = struct {
    op: CmpOp,
    left: CmpLeft,
    query: *Query,
};

pub const Between = struct {
    left: CmpLeft,
    lo: Literal,
    hi: Literal,
    negated: bool,
};

pub const BoolExpr = union(enum) {
    cmp: Cmp,
    isnull: IsNull,
    like: Like,
    in_list: InList,
    in_query: InQuery,
    cmp_query: CmpQuery,
    between: Between,
    @"and": Binary,
    @"or": Binary,
};

pub const Binary = struct {
    left: *BoolExpr,
    right: *BoolExpr,
};

pub const ColumnRef = struct {
    name: []const u8,
    qualifier: ?[]const u8 = null,
    alias: ?[]const u8,
};

pub const QualName = struct {
    qualifier: ?[]const u8 = null,
    name: []const u8,
};

pub const JoinKind = enum { inner, left, right, full };

pub const JoinEq = struct {
    left: QualName,
    right: QualName,
};

pub const Join = struct {
    kind: JoinKind = .inner,
    table: []const u8,
    alias: ?[]const u8 = null,
    sub: ?*Query = null,
    eqs: []const JoinEq,
};

pub const Cte = struct {
    name: []const u8,
    query: *Query,
};

pub const WindowKind = enum { count, sum, avg, min, max, row_number, rank, dense_rank, lag, lead };

pub const FrameBound = union(enum) {
    unbounded_preceding,
    unbounded_following,
    current_row,
    preceding: u64,
    following: u64,
};

pub const FrameUnit = enum { rows, range };

pub const WindowFrame = struct {
    unit: FrameUnit,
    start: FrameBound,
    end: FrameBound,
};

pub const WindowSpec = struct {
    partition_by: []const []const u8 = &.{},
    order_by: []const OrderBy = &.{},
    frame: ?WindowFrame = null,
};

pub const Window = struct {
    kind: WindowKind,
    arg: ?[]const u8,
    alias: ?[]const u8,
    spec: WindowSpec,
    /// LAG/LEAD offset; default 1.
    offset: u64 = 1,
    default_lit: ?Literal = null,
};

pub const CastType = enum { int64, float64, boolean, utf8 };

pub const ScalarKind = enum { abs, round, cast, coalesce };

pub const Scalar = struct {
    kind: ScalarKind,
    arg: []const u8,
    cast_type: ?CastType = null,
    coalesce_col: ?[]const u8 = null,
    coalesce_lit: ?Literal = null,
    alias: ?[]const u8,
};

pub const LiteralItem = struct {
    value: Literal,
    name: []const u8,
};

pub const CaseValue = union(enum) {
    literal: Literal,
    column: []const u8,
    null,
};

pub const CaseArm = struct {
    when: *BoolExpr,
    then: CaseValue,
};

pub const Case = struct {
    arms: []const CaseArm,
    else_value: CaseValue = .null,
    alias: ?[]const u8,
};

pub const SelectItem = union(enum) {
    star,
    column: ColumnRef,
    agg: Agg,
    window: Window,
    scalar: Scalar,
    literal: LiteralItem,
    case: Case,
};

pub const OrderBy = struct {
    column: []const u8,
    desc: bool = false,
};

pub const Query = struct {
    items: []const SelectItem,
    from: []const u8,
    from_alias: ?[]const u8 = null,
    from_sub: ?*Query = null,
    join: ?Join = null,
    where: ?*BoolExpr,
    group_by: []const []const u8,
    having: ?*BoolExpr,
    order_by: []const OrderBy,
    distinct: bool,
    limit: ?u64,
    offset: ?u64,
    union_all: bool = false,
    union_right: ?*Query = null,
    ctes: []const Cte = &.{},

    pub fn isStar(self: Query) bool {
        return self.items.len == 1 and self.items[0] == .star;
    }

    pub fn hasAgg(self: Query) bool {
        for (self.items) |item| {
            if (item == .agg) return true;
        }
        return false;
    }

    pub fn hasWindow(self: Query) bool {
        for (self.items) |item| {
            if (item == .window) return true;
        }
        return false;
    }

    pub fn needsAgg(self: Query) bool {
        return self.hasAgg() or self.group_by.len > 0;
    }

    /// `SELECT 1` / `SELECT 1, 'x'` with no FROM — one row, no scan.
    pub fn isLiteralOnly(self: Query) bool {
        if (self.union_right != null or self.from_sub != null) return false;
        if (self.from.len != 0) return false;
        if (self.where != null or self.having != null or self.needsAgg() or self.distinct) return false;
        if (self.group_by.len != 0 or self.order_by.len != 0) return false;
        if (self.items.len == 0) return false;
        for (self.items) |item| {
            if (item != .literal) return false;
        }
        return true;
    }
};

pub const Copy = struct {
    /// null means `SELECT *` of the connected table.
    query: ?Query = null,
    path: []const u8,
};

pub const Stmt = union(enum) {
    query: Query,
    copy: Copy,
};

pub fn aggArgsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    const aa = a orelse return false;
    const bb = b orelse return false;
    return std.ascii.eqlIgnoreCase(aa, bb);
}

const Kind = enum {
    select,
    from,
    where,
    limit,
    offset,
    group,
    by,
    as,
    having,
    order,
    asc,
    desc,
    distinct,
    all,
    copy,
    to,
    with,
    join,
    inner,
    left,
    right,
    full,
    outer,
    on,
    over,
    partition,
    @"and",
    @"or",
    not,
    like,
    in,
    between,
    case,
    when,
    then,
    @"else",
    end,
    null,
    @"union",
    ident,
    number,
    string,
    star,
    comma,
    dot,
    lparen,
    rparen,
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    eof,
};

const Token = struct {
    kind: Kind,
    start: usize,
    len: usize,

    fn slice(self: Token, sql: []const u8) []const u8 {
        return sql[self.start..][0..self.len];
    }
};

const Lexer = struct {
    sql: []const u8,
    pos: usize = 0,
    peeked: ?Token = null,

    fn skipWs(self: *Lexer) void {
        while (self.pos < self.sql.len) {
            const c = self.sql[self.pos];
            if (c == ' ' or c == '\t' or c == '\n' or c == '\r') {
                self.pos += 1;
            } else if (c == '-' and self.pos + 1 < self.sql.len and self.sql[self.pos + 1] == '-') {
                self.pos += 2;
                while (self.pos < self.sql.len and self.sql[self.pos] != '\n') self.pos += 1;
            } else break;
        }
    }

    fn peek(self: *Lexer) Error!Token {
        if (self.peeked) |t| return t;
        const t = try self.lex();
        self.peeked = t;
        return t;
    }

    fn next(self: *Lexer) Error!Token {
        if (self.peeked) |t| {
            self.peeked = null;
            return t;
        }
        return self.lex();
    }

    fn lex(self: *Lexer) Error!Token {
        self.skipWs();
        if (self.pos >= self.sql.len) return .{ .kind = .eof, .start = self.pos, .len = 0 };

        const start = self.pos;
        const c = self.sql[start];

        if (c == '*') {
            self.pos += 1;
            return .{ .kind = .star, .start = start, .len = 1 };
        }
        if (c == ',') {
            self.pos += 1;
            return .{ .kind = .comma, .start = start, .len = 1 };
        }
        if (c == '.') {
            self.pos += 1;
            return .{ .kind = .dot, .start = start, .len = 1 };
        }
        if (c == '(') {
            self.pos += 1;
            return .{ .kind = .lparen, .start = start, .len = 1 };
        }
        if (c == ')') {
            self.pos += 1;
            return .{ .kind = .rparen, .start = start, .len = 1 };
        }
        if (c == '=') {
            self.pos += 1;
            return .{ .kind = .eq, .start = start, .len = 1 };
        }
        if (c == '!' and start + 1 < self.sql.len and self.sql[start + 1] == '=') {
            self.pos += 2;
            return .{ .kind = .ne, .start = start, .len = 2 };
        }
        if (c == '<') {
            if (start + 1 < self.sql.len and self.sql[start + 1] == '>') {
                self.pos += 2;
                return .{ .kind = .ne, .start = start, .len = 2 };
            }
            if (start + 1 < self.sql.len and self.sql[start + 1] == '=') {
                self.pos += 2;
                return .{ .kind = .le, .start = start, .len = 2 };
            }
            self.pos += 1;
            return .{ .kind = .lt, .start = start, .len = 1 };
        }
        if (c == '>') {
            if (start + 1 < self.sql.len and self.sql[start + 1] == '=') {
                self.pos += 2;
                return .{ .kind = .ge, .start = start, .len = 2 };
            }
            self.pos += 1;
            return .{ .kind = .gt, .start = start, .len = 1 };
        }
        if (c == '\'') {
            self.pos += 1;
            while (self.pos < self.sql.len) {
                if (self.sql[self.pos] == '\'') {
                    if (self.pos + 1 < self.sql.len and self.sql[self.pos + 1] == '\'') {
                        self.pos += 2;
                        continue;
                    }
                    self.pos += 1;
                    return .{ .kind = .string, .start = start, .len = self.pos - start };
                }
                self.pos += 1;
            }
            return error.UnexpectedEof;
        }
        if (c == '-' or isDigit(c)) {
            if (c == '-') self.pos += 1;
            if (self.pos >= self.sql.len or !isDigit(self.sql[self.pos])) return error.InvalidNumber;
            while (self.pos < self.sql.len and isDigit(self.sql[self.pos])) self.pos += 1;
            if (self.pos < self.sql.len and self.sql[self.pos] == '.') {
                self.pos += 1;
                while (self.pos < self.sql.len and isDigit(self.sql[self.pos])) self.pos += 1;
            }
            return .{ .kind = .number, .start = start, .len = self.pos - start };
        }
        if (isIdentStart(c)) {
            self.pos += 1;
            while (self.pos < self.sql.len and isIdentCont(self.sql[self.pos])) self.pos += 1;
            const lexeme = self.sql[start..self.pos];
            const kind: Kind = if (eqlKw(lexeme, "select"))
                .select
            else if (eqlKw(lexeme, "from"))
                .from
            else if (eqlKw(lexeme, "where"))
                .where
            else if (eqlKw(lexeme, "limit"))
                .limit
            else if (eqlKw(lexeme, "offset"))
                .offset
            else if (eqlKw(lexeme, "group"))
                .group
            else if (eqlKw(lexeme, "by"))
                .by
            else if (eqlKw(lexeme, "as"))
                .as
            else if (eqlKw(lexeme, "having"))
                .having
            else if (eqlKw(lexeme, "order"))
                .order
            else if (eqlKw(lexeme, "asc"))
                .asc
            else if (eqlKw(lexeme, "desc"))
                .desc
            else if (eqlKw(lexeme, "distinct"))
                .distinct
            else if (eqlKw(lexeme, "all"))
                .all
            else if (eqlKw(lexeme, "copy"))
                .copy
            else if (eqlKw(lexeme, "to"))
                .to
            else if (eqlKw(lexeme, "with"))
                .with
            else if (eqlKw(lexeme, "join"))
                .join
            else if (eqlKw(lexeme, "inner"))
                .inner
            else if (eqlKw(lexeme, "left"))
                .left
            else if (eqlKw(lexeme, "right"))
                .right
            else if (eqlKw(lexeme, "full"))
                .full
            else if (eqlKw(lexeme, "outer"))
                .outer
            else if (eqlKw(lexeme, "on"))
                .on
            else if (eqlKw(lexeme, "over"))
                .over
            else if (eqlKw(lexeme, "partition"))
                .partition
            else if (eqlKw(lexeme, "and"))
                .@"and"
            else if (eqlKw(lexeme, "or"))
                .@"or"
            else if (eqlKw(lexeme, "not"))
                .not
            else if (eqlKw(lexeme, "like"))
                .like
            else if (eqlKw(lexeme, "in"))
                .in
            else if (eqlKw(lexeme, "between"))
                .between
            else if (eqlKw(lexeme, "case"))
                .case
            else if (eqlKw(lexeme, "when"))
                .when
            else if (eqlKw(lexeme, "then"))
                .then
            else if (eqlKw(lexeme, "else"))
                .@"else"
            else if (eqlKw(lexeme, "end"))
                .end
            else if (eqlKw(lexeme, "null"))
                .null
            else if (eqlKw(lexeme, "union"))
                .@"union"
            else if (eqlKw(lexeme, "natural") or eqlKw(lexeme, "using") or eqlKw(lexeme, "cross"))
                return error.UnsupportedJoin
            else if (eqlKw(lexeme, "window") or eqlKw(lexeme, "except") or eqlKw(lexeme, "intersect") or
                eqlKw(lexeme, "exists") or eqlKw(lexeme, "recursive"))
                return error.UnsupportedSql
            else
                .ident;
            return .{ .kind = kind, .start = start, .len = self.pos - start };
        }
        return error.InvalidSyntax;
    }
};

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}
fn isIdentStart(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
}
fn isIdentCont(c: u8) bool {
    return isIdentStart(c) or isDigit(c);
}
fn eqlKw(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn expect(lexer: *Lexer, kind: Kind) Error!Token {
    const t = try lexer.next();
    if (t.kind != kind) return error.InvalidSyntax;
    return t;
}

fn optionalAlias(lexer: *Lexer, sql: []const u8) Error!?[]const u8 {
    if ((try lexer.peek()).kind == .as) {
        _ = try lexer.next();
        const name = try expect(lexer, .ident);
        return name.slice(sql);
    }
    return null;
}

fn parseAggKind(name: []const u8) ?AggKind {
    if (eqlKw(name, "count")) return .count;
    if (eqlKw(name, "sum")) return .sum;
    if (eqlKw(name, "avg")) return .avg;
    if (eqlKw(name, "min")) return .min;
    if (eqlKw(name, "max")) return .max;
    return null;
}

fn parseRankingKind(name: []const u8) ?WindowKind {
    if (eqlKw(name, "row_number")) return .row_number;
    if (eqlKw(name, "rank")) return .rank;
    if (eqlKw(name, "dense_rank")) return .dense_rank;
    return null;
}

fn parseLagLeadKind(name: []const u8) ?WindowKind {
    if (eqlKw(name, "lag")) return .lag;
    if (eqlKw(name, "lead")) return .lead;
    return null;
}

fn tokIsKw(tok: Token, sql: []const u8, kw: []const u8) bool {
    return tok.kind == .ident and eqlKw(tok.slice(sql), kw);
}

fn windowKindFromAgg(kind: AggKind) WindowKind {
    return switch (kind) {
        .count => .count,
        .sum => .sum,
        .avg => .avg,
        .min => .min,
        .max => .max,
    };
}

fn parseWindowSpec(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) ParseError!WindowSpec {
    _ = try expect(lexer, .over);
    _ = try expect(lexer, .lparen);
    var partition_by: std.ArrayList([]const u8) = .empty;
    defer partition_by.deinit(allocator);
    var order_by: std.ArrayList(OrderBy) = .empty;
    defer order_by.deinit(allocator);

    if ((try lexer.peek()).kind == .partition) {
        _ = try lexer.next();
        _ = try expect(lexer, .by);
        try partition_by.append(allocator, try parseColumnName(lexer, sql));
        while ((try lexer.peek()).kind == .comma) {
            _ = try lexer.next();
            try partition_by.append(allocator, try parseColumnName(lexer, sql));
        }
    }
    if ((try lexer.peek()).kind == .order) {
        _ = try lexer.next();
        _ = try expect(lexer, .by);
        while (true) {
            const col = try parseColumnName(lexer, sql);
            var desc = false;
            const dir = try lexer.peek();
            if (dir.kind == .desc) {
                _ = try lexer.next();
                desc = true;
            } else if (dir.kind == .asc) {
                _ = try lexer.next();
            }
            try order_by.append(allocator, .{ .column = col, .desc = desc });
            if ((try lexer.peek()).kind != .comma) break;
            _ = try lexer.next();
        }
    }
    var frame: ?WindowFrame = null;
    if ((try lexer.peek()).kind != .rparen) {
        frame = try parseWindowFrame(lexer, sql);
        if ((try lexer.peek()).kind != .rparen) return error.UnsupportedSql;
    }
    _ = try lexer.next();
    return .{
        .partition_by = try partition_by.toOwnedSlice(allocator),
        .order_by = try order_by.toOwnedSlice(allocator),
        .frame = frame,
    };
}

fn parseWindowFrame(lexer: *Lexer, sql: []const u8) ParseError!WindowFrame {
    const unit_tok = try lexer.peek();
    const unit: FrameUnit = if (tokIsKw(unit_tok, sql, "rows"))
        .rows
    else if (tokIsKw(unit_tok, sql, "range"))
        .range
    else
        return error.UnsupportedSql;
    _ = try lexer.next();
    if ((try lexer.peek()).kind == .between) {
        _ = try lexer.next();
        const start = try parseFrameBound(lexer, sql);
        _ = try expect(lexer, .@"and");
        const end = try parseFrameBound(lexer, sql);
        return .{ .unit = unit, .start = start, .end = end };
    }
    const start = try parseFrameBound(lexer, sql);
    return .{ .unit = unit, .start = start, .end = .current_row };
}

fn parseFrameBound(lexer: *Lexer, sql: []const u8) ParseError!FrameBound {
    const t = try lexer.next();
    if (tokIsKw(t, sql, "unbounded")) {
        const dir = try lexer.next();
        if (tokIsKw(dir, sql, "preceding")) return .unbounded_preceding;
        if (tokIsKw(dir, sql, "following")) return .unbounded_following;
        return error.InvalidSyntax;
    }
    if (tokIsKw(t, sql, "current")) {
        const row = try lexer.next();
        if (!tokIsKw(row, sql, "row")) return error.InvalidSyntax;
        return .current_row;
    }
    if (t.kind == .number) {
        const n = try parseU64(t, sql);
        const dir = try lexer.next();
        if (tokIsKw(dir, sql, "preceding")) return .{ .preceding = n };
        if (tokIsKw(dir, sql, "following")) return .{ .following = n };
        return error.InvalidSyntax;
    }
    return error.InvalidSyntax;
}

fn parseCastType(name: []const u8) ?CastType {
    if (eqlKw(name, "int") or eqlKw(name, "integer") or eqlKw(name, "bigint") or
        eqlKw(name, "long") or eqlKw(name, "int64")) return .int64;
    if (eqlKw(name, "float") or eqlKw(name, "double") or eqlKw(name, "real") or
        eqlKw(name, "float64")) return .float64;
    if (eqlKw(name, "bool") or eqlKw(name, "boolean")) return .boolean;
    if (eqlKw(name, "string") or eqlKw(name, "varchar") or eqlKw(name, "text") or
        eqlKw(name, "utf8")) return .utf8;
    return null;
}

fn parseScalarKind(name: []const u8) ?ScalarKind {
    if (eqlKw(name, "abs")) return .abs;
    if (eqlKw(name, "round")) return .round;
    if (eqlKw(name, "cast")) return .cast;
    if (eqlKw(name, "coalesce")) return .coalesce;
    return null;
}

fn parseSelectItem(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator) ParseError!SelectItem {
    const t = try lexer.next();
    if (t.kind == .star) return .star;
    if (t.kind == .lparen) return error.UnsupportedSql;
    if (t.kind == .null) {
        const alias = try optionalAlias(lexer, sql);
        return .{ .literal = .{ .value = .null, .name = alias orelse "null" } };
    }
    if (t.kind == .case) return parseCaseItem(lexer, sql, allocator);
    if (t.kind == .number or t.kind == .string) {
        const value: Literal = if (t.kind == .number)
            try parseNumber(t.slice(sql))
        else
            .{ .string = try unescape(allocator, unquote(sql, t)) };
        const alias = try optionalAlias(lexer, sql);
        const name = alias orelse if (t.kind == .string) value.string else t.slice(sql);
        return .{ .literal = .{ .value = value, .name = name } };
    }
    if (t.kind != .ident) return error.InvalidSyntax;

    if ((try lexer.peek()).kind == .lparen) {
        _ = try lexer.next();
        if (parseRankingKind(t.slice(sql))) |kind| {
            _ = try expect(lexer, .rparen);
            if ((try lexer.peek()).kind != .over) return error.InvalidSyntax;
            const spec = try parseWindowSpec(allocator, lexer, sql);
            const alias = try optionalAlias(lexer, sql);
            return .{ .window = .{ .kind = kind, .arg = null, .alias = alias, .spec = spec } };
        }
        if (parseLagLeadKind(t.slice(sql))) |kind| {
            const arg_tok = try lexer.next();
            if (arg_tok.kind != .ident) return error.InvalidSyntax;
            var offset: u64 = 1;
            var default_lit: ?Literal = null;
            if ((try lexer.peek()).kind == .comma) {
                _ = try lexer.next();
                const off_tok = try lexer.next();
                if (off_tok.kind != .number) return error.InvalidSyntax;
                offset = try parseU64(off_tok, sql);
                if ((try lexer.peek()).kind == .comma) {
                    _ = try lexer.next();
                    default_lit = try parseLiteralToken(lexer, sql, allocator);
                }
            }
            _ = try expect(lexer, .rparen);
            if ((try lexer.peek()).kind != .over) return error.InvalidSyntax;
            const spec = try parseWindowSpec(allocator, lexer, sql);
            const alias = try optionalAlias(lexer, sql);
            return .{ .window = .{
                .kind = kind,
                .arg = arg_tok.slice(sql),
                .alias = alias,
                .spec = spec,
                .offset = offset,
                .default_lit = default_lit,
            } };
        }
        if (parseAggKind(t.slice(sql))) |kind| {
            const arg_tok = try lexer.next();
            const arg: ?[]const u8 = switch (arg_tok.kind) {
                .star => blk: {
                    if (kind != .count) return error.InvalidSyntax;
                    break :blk null;
                },
                .ident => arg_tok.slice(sql),
                else => return error.InvalidSyntax,
            };
            _ = try expect(lexer, .rparen);
            if ((try lexer.peek()).kind == .over) {
                const spec = try parseWindowSpec(allocator, lexer, sql);
                const alias = try optionalAlias(lexer, sql);
                return .{ .window = .{
                    .kind = windowKindFromAgg(kind),
                    .arg = arg,
                    .alias = alias,
                    .spec = spec,
                } };
            }
            const alias = try optionalAlias(lexer, sql);
            return .{ .agg = .{ .kind = kind, .arg = arg, .alias = alias } };
        }
        const skind = parseScalarKind(t.slice(sql)) orelse return error.UnsupportedSql;
        return parseScalarItem(lexer, sql, allocator, skind);
    }

    const alias = try optionalAlias(lexer, sql);
    var qualifier: ?[]const u8 = null;
    var name = t.slice(sql);
    if (alias == null and (try lexer.peek()).kind == .dot) {
        _ = try lexer.next();
        qualifier = name;
        name = (try expect(lexer, .ident)).slice(sql);
        const out_alias = try optionalAlias(lexer, sql);
        return .{ .column = .{ .name = name, .qualifier = qualifier, .alias = out_alias } };
    }
    return .{ .column = .{ .name = name, .qualifier = qualifier, .alias = alias } };
}

fn parseScalarItem(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator, kind: ScalarKind) ParseError!SelectItem {
    const arg_tok = try expect(lexer, .ident);
    const arg = arg_tok.slice(sql);
    var cast_type: ?CastType = null;
    var coalesce_col: ?[]const u8 = null;
    var coalesce_lit: ?Literal = null;
    switch (kind) {
        .abs, .round => {},
        .cast => {
            _ = try expect(lexer, .as);
            const ty = try expect(lexer, .ident);
            cast_type = parseCastType(ty.slice(sql)) orelse return error.UnsupportedSql;
        },
        .coalesce => {
            _ = try expect(lexer, .comma);
            const second = try lexer.next();
            switch (second.kind) {
                .ident => coalesce_col = second.slice(sql),
                .number => coalesce_lit = try parseNumber(second.slice(sql)),
                .string => coalesce_lit = .{ .string = try unescape(allocator, unquote(sql, second)) },
                .null => coalesce_lit = .null,
                else => return error.InvalidSyntax,
            }
        },
    }
    _ = try expect(lexer, .rparen);
    const alias = try optionalAlias(lexer, sql);
    return .{ .scalar = .{
        .kind = kind,
        .arg = arg,
        .cast_type = cast_type,
        .coalesce_col = coalesce_col,
        .coalesce_lit = coalesce_lit,
        .alias = alias,
    } };
}

fn parseCaseValue(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator) ParseError!CaseValue {
    const t = try lexer.next();
    return switch (t.kind) {
        .null => .null,
        .number => .{ .literal = try parseNumber(t.slice(sql)) },
        .string => .{ .literal = .{ .string = try unescape(allocator, unquote(sql, t)) } },
        .ident => blk: {
            if ((try lexer.peek()).kind == .dot) {
                _ = try lexer.next();
                const rest = try expect(lexer, .ident);
                break :blk .{ .column = sql[t.start .. rest.start + rest.len] };
            }
            break :blk .{ .column = t.slice(sql) };
        },
        else => error.InvalidSyntax,
    };
}

fn parseCaseItem(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator) ParseError!SelectItem {
    var arms: std.ArrayList(CaseArm) = .empty;
    defer arms.deinit(allocator);
    while ((try lexer.peek()).kind == .when) {
        _ = try lexer.next();
        const when = try parseBool(allocator, lexer, sql, false);
        _ = try expect(lexer, .then);
        const then_v = try parseCaseValue(lexer, sql, allocator);
        try arms.append(allocator, .{ .when = when, .then = then_v });
    }
    if (arms.items.len == 0) return error.InvalidSyntax;
    var else_value: CaseValue = .null;
    if ((try lexer.peek()).kind == .@"else") {
        _ = try lexer.next();
        else_value = try parseCaseValue(lexer, sql, allocator);
    }
    _ = try expect(lexer, .end);
    const alias = try optionalAlias(lexer, sql);
    return .{ .case = .{
        .arms = try arms.toOwnedSlice(allocator),
        .else_value = else_value,
        .alias = alias,
    } };
}

fn allocExpr(allocator: std.mem.Allocator, expr: BoolExpr) ParseError!*BoolExpr {
    const p = try allocator.create(BoolExpr);
    p.* = expr;
    return p;
}

fn parseBool(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*BoolExpr {
    return parseOr(allocator, lexer, sql, allow_agg);
}

fn parseOr(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*BoolExpr {
    var left = try parseAnd(allocator, lexer, sql, allow_agg);
    while ((try lexer.peek()).kind == .@"or") {
        _ = try lexer.next();
        const right = try parseAnd(allocator, lexer, sql, allow_agg);
        left = try allocExpr(allocator, .{ .@"or" = .{ .left = left, .right = right } });
    }
    return left;
}

fn parseAnd(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*BoolExpr {
    var left = try parsePrimary(allocator, lexer, sql, allow_agg);
    while ((try lexer.peek()).kind == .@"and") {
        _ = try lexer.next();
        const right = try parsePrimary(allocator, lexer, sql, allow_agg);
        left = try allocExpr(allocator, .{ .@"and" = .{ .left = left, .right = right } });
    }
    return left;
}

fn parsePrimary(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*BoolExpr {
    if ((try lexer.peek()).kind == .lparen) {
        _ = try lexer.next();
        const inner = try parseOr(allocator, lexer, sql, allow_agg);
        _ = try expect(lexer, .rparen);
        return inner;
    }
    return parsePred(allocator, lexer, sql, allow_agg);
}

fn parsePred(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*BoolExpr {
    const ident = try expect(lexer, .ident);
    const left: CmpLeft = if ((try lexer.peek()).kind == .lparen) blk: {
        if (!allow_agg) return error.InvalidSyntax;
        _ = try lexer.next();
        const kind = parseAggKind(ident.slice(sql)) orelse return error.UnsupportedSql;
        const arg_tok = try lexer.next();
        const arg: ?[]const u8 = switch (arg_tok.kind) {
            .star => inner: {
                if (kind != .count) return error.InvalidSyntax;
                break :inner null;
            },
            .ident => arg_tok.slice(sql),
            else => return error.InvalidSyntax,
        };
        _ = try expect(lexer, .rparen);
        break :blk .{ .agg = .{ .kind = kind, .arg = arg } };
    } else blk: {
        if ((try lexer.peek()).kind == .dot) {
            _ = try lexer.next();
            const rest = try expect(lexer, .ident);
            break :blk .{ .column = sql[ident.start .. rest.start + rest.len] };
        }
        break :blk .{ .column = ident.slice(sql) };
    };

    const op_tok = try lexer.next();
    if (op_tok.kind == .ident and eqlKw(op_tok.slice(sql), "is")) {
        var negated = false;
        var t = try lexer.next();
        if (t.kind == .not or (t.kind == .ident and eqlKw(t.slice(sql), "not"))) {
            negated = true;
            t = try lexer.next();
        }
        if (t.kind != .null and !(t.kind == .ident and eqlKw(t.slice(sql), "null"))) return error.InvalidSyntax;
        return allocExpr(allocator, .{ .isnull = .{ .left = left, .negated = negated } });
    }

    var negated = false;
    var kind = op_tok.kind;
    if (kind == .not) {
        negated = true;
        kind = (try lexer.next()).kind;
    }

    if (kind == .like) {
        const pat = try expect(lexer, .string);
        const pattern = try unescape(allocator, unquote(sql, pat));
        return allocExpr(allocator, .{ .like = .{ .left = left, .pattern = pattern, .negated = negated } });
    }
    if (kind == .in) {
        _ = try expect(lexer, .lparen);
        if ((try lexer.peek()).kind == .select or (try lexer.peek()).kind == .with) {
            const sub = try allocator.create(Query);
            sub.* = try parseQuery(allocator, lexer, sql, .rparen);
            _ = try expect(lexer, .rparen);
            return allocExpr(allocator, .{ .in_query = .{
                .left = left,
                .query = sub,
                .negated = negated,
            } });
        }
        var values: std.ArrayList(Literal) = .empty;
        defer values.deinit(allocator);
        while (true) {
            try values.append(allocator, try parseLiteralToken(lexer, sql, allocator));
            if ((try lexer.peek()).kind != .comma) break;
            _ = try lexer.next();
        }
        _ = try expect(lexer, .rparen);
        if (values.items.len == 0) return error.InvalidSyntax;
        return allocExpr(allocator, .{ .in_list = .{
            .left = left,
            .values = try values.toOwnedSlice(allocator),
            .negated = negated,
        } });
    }
    if (kind == .between) {
        const lo = try parseLiteralToken(lexer, sql, allocator);
        _ = try expect(lexer, .@"and");
        const hi = try parseLiteralToken(lexer, sql, allocator);
        return allocExpr(allocator, .{ .between = .{ .left = left, .lo = lo, .hi = hi, .negated = negated } });
    }
    if (negated) return error.InvalidSyntax;

    const op: CmpOp = switch (kind) {
        .eq => .eq,
        .ne => .ne,
        .lt => .lt,
        .le => .le,
        .gt => .gt,
        .ge => .ge,
        else => return error.InvalidSyntax,
    };
    if ((try lexer.peek()).kind == .lparen) {
        _ = try lexer.next();
        if ((try lexer.peek()).kind == .select or (try lexer.peek()).kind == .with) {
            const sub = try allocator.create(Query);
            sub.* = try parseQuery(allocator, lexer, sql, .rparen);
            _ = try expect(lexer, .rparen);
            return allocExpr(allocator, .{ .cmp_query = .{ .op = op, .left = left, .query = sub } });
        }
        return error.UnsupportedSql;
    }
    const literal = try parseLiteralToken(lexer, sql, allocator);
    return allocExpr(allocator, .{ .cmp = .{ .op = op, .left = left, .literal = literal } });
}

fn parseLiteralToken(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator) ParseError!Literal {
    const lit_tok = try lexer.next();
    return switch (lit_tok.kind) {
        .number => try parseNumber(lit_tok.slice(sql)),
        .string => .{ .string = try unescape(allocator, unquote(sql, lit_tok)) },
        .null => .null,
        else => error.InvalidSyntax,
    };
}

fn parseU64(tok: Token, sql: []const u8) Error!u64 {
    return std.fmt.parseInt(u64, tok.slice(sql), 10) catch error.InvalidNumber;
}

pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !Query {
    switch (try parseStmt(allocator, sql)) {
        .query => |q| return q,
        .copy => return error.InvalidSyntax,
    }
}

pub fn parseStmt(allocator: std.mem.Allocator, sql: []const u8) !Stmt {
    var lexer: Lexer = .{ .sql = sql };
    if ((try lexer.peek()).kind == .copy) {
        return .{ .copy = try parseCopy(allocator, &lexer, sql) };
    }
    return .{ .query = try parseQuery(allocator, &lexer, sql, .eof) };
}

fn parseQuery(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, stop: Kind) !Query {
    if ((try lexer.peek()).kind == .with) return parseWith(allocator, lexer, sql, stop);
    return parseSelect(allocator, lexer, sql, stop);
}

fn parseWith(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, stop: Kind) !Query {
    _ = try expect(lexer, .with);
    var ctes: std.ArrayList(Cte) = .empty;
    defer ctes.deinit(allocator);
    while (true) {
        const name = (try expect(lexer, .ident)).slice(sql);
        for (ctes.items) |prev| {
            if (std.ascii.eqlIgnoreCase(prev.name, name)) return error.InvalidSyntax;
        }
        _ = try expect(lexer, .as);
        const body = try parseSubquery(allocator, lexer, sql);
        try ctes.append(allocator, .{ .name = name, .query = body });
        if ((try lexer.peek()).kind != .comma) break;
        _ = try lexer.next();
    }
    var q = try parseSelect(allocator, lexer, sql, stop);
    q.ctes = try ctes.toOwnedSlice(allocator);
    return q;
}

fn parseSubquery(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) ParseError!*Query {
    _ = try expect(lexer, .lparen);
    const q = try allocator.create(Query);
    q.* = try parseQuery(allocator, lexer, sql, .rparen);
    _ = try expect(lexer, .rparen);
    return q;
}

fn parseCopy(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !Copy {
    _ = try expect(lexer, .copy);
    var query: ?Query = null;
    const next = try lexer.peek();
    if (next.kind == .lparen) {
        _ = try lexer.next();
        query = try parseQuery(allocator, lexer, sql, .rparen);
        _ = try expect(lexer, .rparen);
    } else if (next.kind == .ident) {
        const from = try parseFrom(lexer, sql);
        query = .{
            .items = try allocator.dupe(SelectItem, &.{.star}),
            .from = from,
            .where = null,
            .group_by = &.{},
            .having = null,
            .order_by = &.{},
            .distinct = false,
            .limit = null,
            .offset = null,
        };
    }
    _ = try expect(lexer, .to);
    const path_tok = try expect(lexer, .string);
    const end = try lexer.next();
    if (end.kind != .eof) return error.InvalidSyntax;
    return .{ .query = query, .path = unquote(sql, path_tok) };
}

fn parseSelect(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, stop: Kind) !Query {
    _ = try expect(lexer, .select);

    var distinct = false;
    if ((try lexer.peek()).kind == .distinct) {
        _ = try lexer.next();
        distinct = true;
    } else if ((try lexer.peek()).kind == .all) {
        _ = try lexer.next();
    }

    var items: std.ArrayList(SelectItem) = .empty;
    defer items.deinit(allocator);

    try items.append(allocator, try parseSelectItem(lexer, sql, allocator));
    if (items.items[0] == .star) {
        if ((try lexer.peek()).kind == .comma) return error.UnsupportedSql;
    } else {
        while ((try lexer.peek()).kind == .comma) {
            _ = try lexer.next();
            const item = try parseSelectItem(lexer, sql, allocator);
            if (item == .star) return error.UnsupportedSql;
            try items.append(allocator, item);
        }
    }

    var from: []const u8 = "";
    var from_alias: ?[]const u8 = null;
    var from_sub: ?*Query = null;
    var join: ?Join = null;
    if ((try lexer.peek()).kind == .from) {
        _ = try lexer.next();
        if ((try lexer.peek()).kind == .lparen) {
            from_sub = try parseSubquery(allocator, lexer, sql);
            from_alias = try parseTableAlias(lexer, sql);
            from = from_alias orelse "";
        } else {
            from = try parseFrom(lexer, sql);
            from_alias = try parseTableAlias(lexer, sql);
        }
        join = try parseOptionalJoin(allocator, lexer, sql);
    }

    var where_expr: ?*BoolExpr = null;
    if ((try lexer.peek()).kind == .where) {
        _ = try lexer.next();
        where_expr = try parseBool(allocator, lexer, sql, false);
    }

    var group_by: std.ArrayList([]const u8) = .empty;
    defer group_by.deinit(allocator);

    if ((try lexer.peek()).kind == .group) {
        _ = try lexer.next();
        _ = try expect(lexer, .by);
        try group_by.append(allocator, try parseColumnName(lexer, sql));
        while ((try lexer.peek()).kind == .comma) {
            _ = try lexer.next();
            try group_by.append(allocator, try parseColumnName(lexer, sql));
        }
    }

    var having_expr: ?*BoolExpr = null;
    if ((try lexer.peek()).kind == .having) {
        _ = try lexer.next();
        having_expr = try parseBool(allocator, lexer, sql, true);
    }

    var order_by: std.ArrayList(OrderBy) = .empty;
    defer order_by.deinit(allocator);
    if ((try lexer.peek()).kind == .order) {
        _ = try lexer.next();
        _ = try expect(lexer, .by);
        while (true) {
            const col = try parseColumnName(lexer, sql);
            var desc = false;
            const dir = try lexer.peek();
            if (dir.kind == .desc) {
                _ = try lexer.next();
                desc = true;
            } else if (dir.kind == .asc) {
                _ = try lexer.next();
            }
            try order_by.append(allocator, .{ .column = col, .desc = desc });
            if ((try lexer.peek()).kind != .comma) break;
            _ = try lexer.next();
        }
    }

    var limit: ?u64 = null;
    var offset: ?u64 = null;
    if ((try lexer.peek()).kind == .limit) {
        _ = try lexer.next();
        limit = try parseU64(try expect(lexer, .number), sql);
        if ((try lexer.peek()).kind == .offset) {
            _ = try lexer.next();
            offset = try parseU64(try expect(lexer, .number), sql);
        }
    } else if ((try lexer.peek()).kind == .offset) {
        _ = try lexer.next();
        offset = try parseU64(try expect(lexer, .number), sql);
        if ((try lexer.peek()).kind == .limit) {
            _ = try lexer.next();
            limit = try parseU64(try expect(lexer, .number), sql);
        }
    }

    if (having_expr != null and group_by.items.len == 0 and !hasAggItems(items.items)) {
        return error.InvalidSyntax;
    }

    var q: Query = .{
        .items = try items.toOwnedSlice(allocator),
        .from = from,
        .from_alias = from_alias,
        .from_sub = from_sub,
        .join = join,
        .where = where_expr,
        .group_by = try group_by.toOwnedSlice(allocator),
        .having = having_expr,
        .order_by = try order_by.toOwnedSlice(allocator),
        .distinct = distinct,
        .limit = limit,
        .offset = offset,
    };

    if ((try lexer.peek()).kind == .@"union") {
        _ = try lexer.next();
        var union_all = false;
        if ((try lexer.peek()).kind == .all) {
            _ = try lexer.next();
            union_all = true;
        }
        const right = try allocator.create(Query);
        right.* = try parseSelect(allocator, lexer, sql, stop);
        q.union_all = union_all;
        q.union_right = right;
        return q;
    }

    const end = try lexer.peek();
    if (end.kind != stop) {
        if (end.kind == .ident or end.kind == .select) return error.InvalidSyntax;
        return error.UnsupportedSql;
    }
    if (stop == .eof) _ = try lexer.next();
    return q;
}

fn hasAggItems(items: []const SelectItem) bool {
    for (items) |item| {
        if (item == .agg) return true;
    }
    return false;
}

fn parseFrom(lexer: *Lexer, sql: []const u8) Error![]const u8 {
    const t = try lexer.next();
    if (t.kind == .string) return unquote(sql, t);
    if (t.kind != .ident) return error.InvalidSyntax;
    const start = t.start;
    var end = t.start + t.len;
    if ((try lexer.peek()).kind == .dot) {
        _ = try lexer.next();
        const rest = try expect(lexer, .ident);
        end = rest.start + rest.len;
    }
    return sql[start..end];
}

fn parseTableAlias(lexer: *Lexer, sql: []const u8) Error!?[]const u8 {
    if ((try lexer.peek()).kind == .as) {
        _ = try lexer.next();
        return (try expect(lexer, .ident)).slice(sql);
    }
    if ((try lexer.peek()).kind == .ident) {
        return (try lexer.next()).slice(sql);
    }
    return null;
}

fn parseOptionalJoin(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) ParseError!?Join {
    if ((try lexer.peek()).kind == .comma) return error.UnsupportedJoin;
    switch ((try lexer.peek()).kind) {
        .join, .inner, .left, .right, .full => return try parseJoin(allocator, lexer, sql),
        else => return null,
    }
}

fn parseJoin(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) ParseError!Join {
    var kind: JoinKind = .inner;
    switch ((try lexer.peek()).kind) {
        .left => {
            _ = try lexer.next();
            if ((try lexer.peek()).kind == .outer) _ = try lexer.next();
            kind = .left;
        },
        .right => {
            _ = try lexer.next();
            if ((try lexer.peek()).kind == .outer) _ = try lexer.next();
            kind = .right;
        },
        .full => {
            _ = try lexer.next();
            if ((try lexer.peek()).kind == .outer) _ = try lexer.next();
            kind = .full;
        },
        .inner => _ = try lexer.next(),
        else => {},
    }
    _ = try expect(lexer, .join);
    var table: []const u8 = undefined;
    var alias: ?[]const u8 = null;
    var sub: ?*Query = null;
    if ((try lexer.peek()).kind == .lparen) {
        sub = try parseSubquery(allocator, lexer, sql);
        alias = try parseTableAlias(lexer, sql);
        if (alias == null) return error.InvalidSyntax;
        table = alias.?;
    } else {
        table = try parseFrom(lexer, sql);
        alias = try parseTableAlias(lexer, sql);
    }
    if ((try lexer.peek()).kind != .on) return error.UnsupportedJoin;
    _ = try lexer.next();
    var eqs: std.ArrayList(JoinEq) = .empty;
    defer eqs.deinit(allocator);
    while (true) {
        const left = try parseQualName(lexer, sql);
        if ((try lexer.peek()).kind != .eq) return error.UnsupportedJoin;
        _ = try lexer.next();
        const rhs = try lexer.peek();
        if (rhs.kind == .number or rhs.kind == .string) return error.UnsupportedJoin;
        const right = try parseQualName(lexer, sql);
        try eqs.append(allocator, .{ .left = left, .right = right });
        if ((try lexer.peek()).kind != .@"and") break;
        _ = try lexer.next();
    }
    return .{
        .kind = kind,
        .table = table,
        .alias = alias,
        .sub = sub,
        .eqs = try eqs.toOwnedSlice(allocator),
    };
}

fn parseQualName(lexer: *Lexer, sql: []const u8) Error!QualName {
    const first = try expect(lexer, .ident);
    if ((try lexer.peek()).kind == .dot) {
        _ = try lexer.next();
        const second = try expect(lexer, .ident);
        return .{ .qualifier = first.slice(sql), .name = second.slice(sql) };
    }
    return .{ .qualifier = null, .name = first.slice(sql) };
}

fn parseColumnName(lexer: *Lexer, sql: []const u8) Error![]const u8 {
    const first = try expect(lexer, .ident);
    if ((try lexer.peek()).kind != .dot) return first.slice(sql);
    _ = try lexer.next();
    const second = try expect(lexer, .ident);
    return sql[first.start .. second.start + second.len];
}

fn unquote(sql: []const u8, t: Token) []const u8 {
    return sql[t.start + 1 .. t.start + t.len - 1];
}

fn unescape(allocator: std.mem.Allocator, inner: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, inner, '\'') == null) return inner;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < inner.len) {
        if (inner[i] == '\'' and i + 1 < inner.len and inner[i + 1] == '\'') {
            try out.append(allocator, '\'');
            i += 2;
        } else {
            try out.append(allocator, inner[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(allocator);
}

fn parseNumber(s: []const u8) Error!Literal {
    if (std.mem.indexOfScalar(u8, s, '.') != null) {
        const v = std.fmt.parseFloat(f64, s) catch return error.InvalidNumber;
        return .{ .float = v };
    }
    const v = std.fmt.parseInt(i64, s, 10) catch return error.InvalidNumber;
    return .{ .int = v };
}

fn fillAliases(q: *const Query, buf: *[16][]const u8) usize {
    var n: usize = 0;
    addAlias(buf, &n, q.from);
    if (q.from_alias) |a| addAlias(buf, &n, a);
    if (q.join) |j| {
        addAlias(buf, &n, j.table);
        if (j.alias) |a| addAlias(buf, &n, a);
    }
    return n;
}

fn addAlias(buf: *[16][]const u8, n: *usize, s: []const u8) void {
    if (s.len == 0) return;
    for (buf.*[0..n.*]) |prev| {
        if (std.ascii.eqlIgnoreCase(prev, s)) return;
    }
    if (n.* >= buf.len) return;
    buf[n.*] = s;
    n.* += 1;
}

fn nameIn(names: []const []const u8, s: []const u8) bool {
    for (names) |n| {
        if (std.ascii.eqlIgnoreCase(n, s)) return true;
    }
    return false;
}

fn qualifierOf(name: []const u8) ?[]const u8 {
    const dot = std.mem.indexOfScalar(u8, name, '.') orelse return null;
    if (dot == 0) return null;
    return name[0..dot];
}

fn qualRefersOuter(qual: []const u8, inner: []const []const u8, outer: []const []const u8) bool {
    if (nameIn(inner, qual)) return false;
    return nameIn(outer, qual);
}

fn columnRefersOuter(name: []const u8, inner: []const []const u8, outer: []const []const u8) bool {
    const q = qualifierOf(name) orelse return false;
    return qualRefersOuter(q, inner, outer);
}

fn leftRefsOuter(left: CmpLeft, inner: []const []const u8, outer: []const []const u8) bool {
    return switch (left) {
        .column => |n| columnRefersOuter(n, inner, outer),
        .agg => false,
    };
}

fn stackAliases(dst: *[32][]const u8, a: []const []const u8, b: []const []const u8) usize {
    var n: usize = 0;
    for (a) |s| {
        if (n < dst.len) {
            dst[n] = s;
            n += 1;
        }
    }
    for (b) |s| {
        if (n < dst.len) {
            dst[n] = s;
            n += 1;
        }
    }
    return n;
}

fn exprRefsOuter(expr: *const BoolExpr, inner: []const []const u8, outer: []const []const u8) bool {
    return switch (expr.*) {
        .cmp => |c| leftRefsOuter(c.left, inner, outer),
        .isnull => |p| leftRefsOuter(p.left, inner, outer),
        .like => |p| leftRefsOuter(p.left, inner, outer),
        .in_list => |p| leftRefsOuter(p.left, inner, outer),
        .between => |p| leftRefsOuter(p.left, inner, outer),
        .in_query => |p| leftRefsOuter(p.left, inner, outer) or isCorrelatedTo(p.query, inner, outer),
        .cmp_query => |p| leftRefsOuter(p.left, inner, outer) or isCorrelatedTo(p.query, inner, outer),
        .@"and", .@"or" => |b| exprRefsOuter(b.left, inner, outer) or exprRefsOuter(b.right, inner, outer),
    };
}

fn itemRefsOuter(item: SelectItem, inner: []const []const u8, outer: []const []const u8) bool {
    return switch (item) {
        .column => |c| if (c.qualifier) |q| qualRefersOuter(q, inner, outer) else false,
        .case => |cs| blk: {
            for (cs.arms) |arm| {
                if (exprRefsOuter(arm.when, inner, outer)) break :blk true;
                if (caseValueRefsOuter(arm.then, inner, outer)) break :blk true;
            }
            break :blk caseValueRefsOuter(cs.else_value, inner, outer);
        },
        else => false,
    };
}

fn caseValueRefsOuter(v: CaseValue, inner: []const []const u8, outer: []const []const u8) bool {
    return switch (v) {
        .column => |n| columnRefersOuter(n, inner, outer),
        else => false,
    };
}

fn isCorrelatedTo(inner: *const Query, parent_aliases: []const []const u8, outer: []const []const u8) bool {
    var stacked: [32][]const u8 = undefined;
    const sn = stackAliases(&stacked, parent_aliases, outer);
    return queryRefsOuter(inner, stacked[0..sn]);
}

fn queryRefsOuter(q: *const Query, outer: []const []const u8) bool {
    var inner_buf: [16][]const u8 = undefined;
    const inner = inner_buf[0..fillAliases(q, &inner_buf)];
    if (q.where) |e| {
        if (exprRefsOuter(e, inner, outer)) return true;
    }
    if (q.having) |e| {
        if (exprRefsOuter(e, inner, outer)) return true;
    }
    for (q.items) |item| {
        if (itemRefsOuter(item, inner, outer)) return true;
    }
    if (q.join) |j| {
        for (j.eqs) |eq| {
            if (eq.left.qualifier) |qual| {
                if (qualRefersOuter(qual, inner, outer)) return true;
            }
            if (eq.right.qualifier) |qual| {
                if (qualRefersOuter(qual, inner, outer)) return true;
            }
        }
        if (j.sub) |sub| {
            if (isCorrelatedTo(sub, inner, outer)) return true;
        }
    }
    if (q.from_sub) |sub| {
        if (isCorrelatedTo(sub, inner, outer)) return true;
    }
    if (q.union_right) |r| {
        if (queryRefsOuter(r, outer)) return true;
    }
    return false;
}

/// True when `inner` references a qualifier that belongs to `outer` (correlated subquery).
pub fn isCorrelated(inner: *const Query, outer: *const Query) bool {
    var buf: [16][]const u8 = undefined;
    const n = fillAliases(outer, &buf);
    return queryRefsOuter(inner, buf[0..n]);
}

pub fn usesTableName(q: *const Query, name: []const u8) bool {
    if (q.from_sub == null and q.from.len > 0 and std.ascii.eqlIgnoreCase(q.from, name)) return true;
    if (q.join) |j| {
        if (j.sub == null and std.ascii.eqlIgnoreCase(j.table, name)) return true;
        if (j.sub) |s| {
            if (usesTableName(s, name)) return true;
        }
    }
    if (q.from_sub) |s| {
        if (usesTableName(s, name)) return true;
    }
    if (q.where) |e| {
        if (exprUsesTable(e, name)) return true;
    }
    if (q.having) |e| {
        if (exprUsesTable(e, name)) return true;
    }
    if (q.union_right) |r| {
        if (usesTableName(r, name)) return true;
    }
    return false;
}

fn exprUsesTable(expr: *const BoolExpr, name: []const u8) bool {
    return switch (expr.*) {
        .in_query => |p| usesTableName(p.query, name),
        .cmp_query => |p| usesTableName(p.query, name),
        .@"and", .@"or" => |b| exprUsesTable(b.left, name) or exprUsesTable(b.right, name),
        else => false,
    };
}

test "parse select star where limit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(arena.allocator(), "SELECT * FROM sales WHERE price > 100 LIMIT 2");
    try std.testing.expect(q.isStar());
    try std.testing.expectEqualStrings("sales", q.from);
    const where = q.where orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(CmpOp.gt, where.cmp.op);
    try std.testing.expectEqualStrings("price", where.cmp.left.column);
    try std.testing.expectEqual(@as(i64, 100), where.cmp.literal.int);
    try std.testing.expectEqual(@as(u64, 2), q.limit.?);
}

test "parse or and parens" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(arena.allocator(), "SELECT * FROM sales WHERE price > 100 OR category = 'fruit'");
    const where = q.where orelse return error.TestUnexpectedResult;
    try std.testing.expect(where.* == .@"or");
}

test "parse order having offset" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(
        arena.allocator(),
        "SELECT category, COUNT(*) AS n FROM sales GROUP BY category HAVING COUNT(*) > 2 ORDER BY n DESC LIMIT 1 OFFSET 0",
    );
    try std.testing.expect(q.having != null);
    try std.testing.expectEqual(@as(usize, 1), q.order_by.len);
    try std.testing.expectEqualStrings("n", q.order_by[0].column);
    try std.testing.expect(q.order_by[0].desc);
    try std.testing.expectEqual(@as(u64, 1), q.limit.?);
    try std.testing.expectEqual(@as(u64, 0), q.offset.?);
}

test "parse count and group by" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(arena.allocator(), "SELECT category, COUNT(*) AS n, SUM(price) FROM sales GROUP BY category");
    try std.testing.expect(q.hasAgg());
    try std.testing.expectEqualStrings("category", q.group_by[0]);
    try std.testing.expectEqual(AggKind.count, q.items[1].agg.kind);
}

test "join without on is unsupported" {
    try std.testing.expectError(error.UnsupportedJoin, parse(std.testing.allocator, "SELECT * FROM a JOIN b"));
}

test "comma join is unsupported" {
    try std.testing.expectError(error.UnsupportedJoin, parse(std.testing.allocator, "SELECT * FROM a, b"));
}

test "left join parses" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(
        arena.allocator(),
        "SELECT a.id, b.price FROM sales a LEFT OUTER JOIN items b ON a.id = b.id",
    );
    const j = q.join orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(JoinKind.left, j.kind);
    try std.testing.expectEqualStrings("items", j.table);
}

test "right and full join parse" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try parse(arena.allocator(), "SELECT * FROM a RIGHT JOIN b ON a.id = b.id");
    try std.testing.expectEqual(JoinKind.right, r.join.?.kind);
    const f = try parse(arena.allocator(), "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id");
    try std.testing.expectEqual(JoinKind.full, f.join.?.kind);
}

test "using and natural stay unsupported" {
    try std.testing.expectError(error.UnsupportedJoin, parse(std.testing.allocator, "SELECT * FROM a JOIN b USING (id)"));
    try std.testing.expectError(error.UnsupportedJoin, parse(std.testing.allocator, "SELECT * FROM a NATURAL JOIN b"));
}

test "parse inner join on equality" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(
        arena.allocator(),
        "SELECT a.id, b.price FROM sales a INNER JOIN items b ON a.id = b.order_id AND a.category = b.category",
    );
    try std.testing.expectEqualStrings("sales", q.from);
    try std.testing.expectEqualStrings("a", q.from_alias.?);
    const j = q.join orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("items", j.table);
    try std.testing.expectEqualStrings("b", j.alias.?);
    try std.testing.expectEqual(@as(usize, 2), j.eqs.len);
    try std.testing.expectEqualStrings("a", j.eqs[0].left.qualifier.?);
    try std.testing.expectEqualStrings("id", j.eqs[0].left.name);
    try std.testing.expectEqualStrings("b", j.eqs[0].right.qualifier.?);
    try std.testing.expectEqualStrings("order_id", j.eqs[0].right.name);
    try std.testing.expectEqualStrings("id", q.items[0].column.name);
    try std.testing.expectEqualStrings("a", q.items[0].column.qualifier.?);
}

test "parse window over partition order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(
        arena.allocator(),
        "SELECT id, COUNT(*) OVER () AS n, SUM(price) OVER (PARTITION BY category), ROW_NUMBER() OVER (ORDER BY id) FROM sales",
    );
    try std.testing.expect(q.hasWindow());
    try std.testing.expect(!q.hasAgg());
    try std.testing.expectEqual(WindowKind.count, q.items[1].window.kind);
    try std.testing.expectEqualStrings("n", q.items[1].window.alias.?);
    try std.testing.expectEqual(@as(usize, 0), q.items[1].window.spec.partition_by.len);
    try std.testing.expectEqual(WindowKind.sum, q.items[2].window.kind);
    try std.testing.expectEqualStrings("category", q.items[2].window.spec.partition_by[0]);
    try std.testing.expectEqual(WindowKind.row_number, q.items[3].window.kind);
    try std.testing.expectEqualStrings("id", q.items[3].window.spec.order_by[0].column);
}

test "window frame LAG LEAD parse" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const rows = try parse(a, "SELECT SUM(price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM sales");
    try std.testing.expectEqual(WindowKind.sum, rows.items[0].window.kind);
    const rf = rows.items[0].window.spec.frame.?;
    try std.testing.expectEqual(FrameUnit.rows, rf.unit);
    try std.testing.expectEqual(FrameBound.unbounded_preceding, rf.start);
    try std.testing.expectEqual(FrameBound.current_row, rf.end);

    const between = try parse(
        a,
        "SELECT SUM(price) OVER (PARTITION BY category ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales",
    );
    const bf = between.items[0].window.spec.frame.?;
    try std.testing.expectEqual(FrameUnit.rows, bf.unit);
    try std.testing.expectEqual(@as(u64, 1), bf.start.preceding);
    try std.testing.expectEqual(FrameBound.current_row, bf.end);
    try std.testing.expectEqualStrings("category", between.items[0].window.spec.partition_by[0]);

    const range = try parse(a, "SELECT SUM(price) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales");
    try std.testing.expectEqual(FrameUnit.range, range.items[0].window.spec.frame.?.unit);

    const lag = try parse(a, "SELECT LAG(price) OVER (ORDER BY id) FROM sales");
    try std.testing.expectEqual(WindowKind.lag, lag.items[0].window.kind);
    try std.testing.expectEqualStrings("price", lag.items[0].window.arg.?);
    try std.testing.expectEqual(@as(u64, 1), lag.items[0].window.offset);

    const lead = try parse(a, "SELECT LEAD(price, 2) OVER (ORDER BY id) AS nxt FROM sales");
    try std.testing.expectEqual(WindowKind.lead, lead.items[0].window.kind);
    try std.testing.expectEqual(@as(u64, 2), lead.items[0].window.offset);
    try std.testing.expectEqualStrings("nxt", lead.items[0].window.alias.?);

    try std.testing.expectError(
        error.UnsupportedSql,
        parse(std.testing.allocator, "SELECT SUM(price) OVER (ORDER BY id GROUPS UNBOUNDED PRECEDING) FROM sales"),
    );
}

test "parse select literal without from" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(arena.allocator(), "select 1");
    try std.testing.expect(q.isLiteralOnly());
    try std.testing.expectEqual(@as(i64, 1), q.items[0].literal.value.int);
    try std.testing.expectEqualStrings("1", q.items[0].literal.name);

    const aliased = try parse(arena.allocator(), "SELECT 1 AS x, 'hi' AS s");
    try std.testing.expect(aliased.isLiteralOnly());
    try std.testing.expectEqualStrings("x", aliased.items[0].literal.name);
    try std.testing.expectEqualStrings("hi", aliased.items[1].literal.value.string);
}

test "parse distinct order by many scalars" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(
        arena.allocator(),
        "SELECT DISTINCT abs(price) AS p, CAST(id AS DOUBLE), coalesce(category, 'x') FROM sales ORDER BY category ASC, price DESC",
    );
    try std.testing.expect(q.distinct);
    try std.testing.expectEqual(@as(usize, 2), q.order_by.len);
    try std.testing.expectEqualStrings("category", q.order_by[0].column);
    try std.testing.expect(!q.order_by[0].desc);
    try std.testing.expectEqualStrings("price", q.order_by[1].column);
    try std.testing.expect(q.order_by[1].desc);
    try std.testing.expectEqual(ScalarKind.abs, q.items[0].scalar.kind);
    try std.testing.expectEqual(CastType.float64, q.items[1].scalar.cast_type.?);
    try std.testing.expectEqualStrings("x", q.items[2].scalar.coalesce_lit.?.string);
}

test "parse is null" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(arena.allocator(), "SELECT * FROM sales WHERE qty IS NULL");
    const where = q.where orelse return error.TestUnexpectedResult;
    try std.testing.expect(where.* == .isnull);
    try std.testing.expect(!where.isnull.negated);
    try std.testing.expectEqualStrings("qty", where.isnull.left.column);

    const q2 = try parse(arena.allocator(), "SELECT * FROM sales WHERE qty IS NOT NULL");
    const w2 = q2.where orelse return error.TestUnexpectedResult;
    try std.testing.expect(w2.isnull.negated);
}

test "parse select without from" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const q = try parse(arena.allocator(), "SELECT * WHERE price > 100 LIMIT 2");
    try std.testing.expectEqualStrings("", q.from);
    try std.testing.expect(q.where != null);
    try std.testing.expectEqual(@as(u64, 2), q.limit.?);
}

test "parse CASE LIKE IN BETWEEN NULL UNION" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const nul = try parse(a, "SELECT NULL");
    try std.testing.expect(nul.isLiteralOnly());
    try std.testing.expect(nul.items[0].literal.value == .null);
    try std.testing.expectEqualStrings("null", nul.items[0].literal.name);

    const like = try parse(a, "SELECT * FROM sales WHERE category LIKE 'f%'");
    try std.testing.expect(like.where.?.* == .like);
    try std.testing.expectEqualStrings("f%", like.where.?.like.pattern);
    try std.testing.expect(!like.where.?.like.negated);

    const nlike = try parse(a, "SELECT id FROM sales WHERE category NOT LIKE '%g'");
    try std.testing.expect(nlike.where.?.like.negated);

    const in_l = try parse(a, "SELECT * FROM sales WHERE price IN (50, 80, 90)");
    try std.testing.expect(in_l.where.?.* == .in_list);
    try std.testing.expectEqual(@as(usize, 3), in_l.where.?.in_list.values.len);

    const nin = try parse(a, "SELECT * FROM sales WHERE id NOT IN (1, 2)");
    try std.testing.expect(nin.where.?.in_list.negated);

    const bet = try parse(a, "SELECT * FROM sales WHERE price BETWEEN 100 AND 150");
    try std.testing.expect(bet.where.?.* == .between);
    try std.testing.expectEqual(@as(i64, 100), bet.where.?.between.lo.int);
    try std.testing.expectEqual(@as(i64, 150), bet.where.?.between.hi.int);

    const cs = try parse(a, "SELECT CASE WHEN price > 100 THEN 'hi' ELSE 'lo' END AS band FROM sales");
    try std.testing.expect(cs.items[0] == .case);
    try std.testing.expectEqual(@as(usize, 1), cs.items[0].case.arms.len);
    try std.testing.expectEqualStrings("hi", cs.items[0].case.arms[0].then.literal.string);
    try std.testing.expectEqualStrings("lo", cs.items[0].case.else_value.literal.string);
    try std.testing.expectEqualStrings("band", cs.items[0].case.alias.?);

    const u = try parse(a, "SELECT 1 UNION SELECT 2");
    try std.testing.expect(u.union_right != null);
    try std.testing.expect(!u.union_all);
    try std.testing.expectEqual(@as(i64, 2), u.union_right.?.items[0].literal.value.int);

    const ua = try parse(a, "SELECT 1 UNION ALL SELECT 1");
    try std.testing.expect(ua.union_all);

    const chain = try parse(a, "SELECT 1 UNION SELECT 2 UNION ALL SELECT 3");
    try std.testing.expect(!chain.union_all);
    try std.testing.expect(chain.union_right.?.union_all);
    try std.testing.expectEqual(@as(i64, 3), chain.union_right.?.union_right.?.items[0].literal.value.int);
}

test "parse WITH FROM subquery IN subquery" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const from_sub = try parse(a, "SELECT * FROM (SELECT 1 AS x) t");
    try std.testing.expect(from_sub.from_sub != null);
    try std.testing.expectEqualStrings("t", from_sub.from);
    try std.testing.expectEqualStrings("t", from_sub.from_alias.?);

    const inq = try parse(a, "SELECT * FROM sales WHERE id IN (SELECT id FROM sales)");
    try std.testing.expect(inq.where.?.* == .in_query);

    const cmpq = try parse(a, "SELECT * FROM sales WHERE price > (SELECT MIN(price) FROM sales)");
    try std.testing.expect(cmpq.where.?.* == .cmp_query);
    try std.testing.expectEqual(CmpOp.gt, cmpq.where.?.cmp_query.op);

    const with_q = try parse(a, "WITH t AS (SELECT 1 AS x) SELECT * FROM t");
    try std.testing.expectEqual(@as(usize, 1), with_q.ctes.len);
    try std.testing.expectEqualStrings("t", with_q.ctes[0].name);
    try std.testing.expectEqualStrings("t", with_q.from);

    const join_sub = try parse(a, "SELECT a.id FROM sales a LEFT JOIN (SELECT id FROM sales) b ON a.id = b.id");
    try std.testing.expect(join_sub.join.?.sub != null);
    try std.testing.expectEqualStrings("b", join_sub.join.?.alias.?);

    try std.testing.expectError(error.UnsupportedSql, parse(a, "SELECT (SELECT 1) FROM sales"));
    try std.testing.expectError(error.UnsupportedSql, parse(a, "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t"));
}

test "parse COPY to glacier" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    {
        const s = try parseStmt(a, "COPY TO 'out.glacier'");
        try std.testing.expectEqualStrings("out.glacier", s.copy.path);
        try std.testing.expect(s.copy.query == null);
    }
    {
        const s = try parseStmt(a, "COPY (SELECT id, price FROM sales WHERE price > 100) TO '/tmp/x.glacier'");
        try std.testing.expectEqualStrings("/tmp/x.glacier", s.copy.path);
        const q = s.copy.query orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(usize, 2), q.items.len);
        try std.testing.expectEqualStrings("sales", q.from);
        try std.testing.expect(q.where != null);
    }
    {
        const s = try parseStmt(a, "COPY sales TO 't.glacier'");
        try std.testing.expectEqualStrings("sales", s.copy.query.?.from);
        try std.testing.expect(s.copy.query.?.isStar());
    }
}
