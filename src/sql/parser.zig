//! SQL surface:
//! SELECT [DISTINCT] … [FROM t | (SELECT …) [AS a] [INNER] JOIN t2 | (SELECT …) [AS b] ON a.x = b.y]
//!   [WHERE bool] [GROUP BY cols] [HAVING bool]
//!   [ORDER BY col [ASC|DESC], …] [LIMIT n] [OFFSET m]
//! WITH t AS (SELECT …) [, …] SELECT …
//! COPY TO 'file.glacier'
//! COPY (SELECT …) TO 'file.glacier'
//! COPY t FROM 'file.parquet'   (ingest into an Iceberg table via the catalog)
//! SHOW CATALOGS
//! SHOW NAMESPACES [FROM catalog]
//! SHOW TABLES [FROM catalog[.namespace]]
//! ATTACH [CATALOG] 'path-or-uri' AS name
//! DESCRIBE [TABLE] catalog[.namespace].table
//! USE catalog[.namespace]
//! DETACH [CATALOG] name
//! CREATE NAMESPACE [catalog.]ns
//! CREATE TABLE [catalog.][ns.]name (col BIGINT|INT|STRING|DOUBLE|FLOAT|BOOLEAN, …)
//! CREATE TABLE [catalog.][ns.]name AS SELECT …
//! INSERT INTO [catalog.][ns.]name VALUES (…)[, …] | SELECT …
//! DELETE FROM [catalog.][ns.]name [WHERE bool]   (Iceberg equality deletes)
//! UPDATE [catalog.][ns.]name SET col = lit|col [, …] [WHERE bool]
//! MERGE INTO t [AS a] USING u [AS b] ON a.k = b.k
//!   WHEN MATCHED THEN DELETE | UPDATE SET …
//!   WHEN NOT MATCHED THEN INSERT [(cols)] VALUES (…)
//! ALTER TABLE t ADD COLUMN col TYPE
//! DROP TABLE [catalog.][ns.]name
//! FROM glacier.catalogs / glacier.tables / glacier.snapshots / glacier.files (Iceberg)
//! FROM t.snapshots / t.files (Iceberg metadata of t)
//! Window: agg/ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD OVER ([PARTITION BY …] [ORDER BY …] [ROWS|RANGE …])
//! Literals: `SELECT 1` / `SELECT NULL` (no FROM). Escalars: abs, round, cast, coalesce, lower, upper, length, trim, replace, substr, concat, left, right, starts_with, date_trunc, extract, year/month/day/hour/minute/second, ceil, floor, sign, greatest, least, CASE.
//! Expr: `+ - * / %`, unary minus, col vs col, nested calls (`abs(price * 2)`), scalar subquery `(SELECT …)`.
//! Aggregates: COUNT/SUM/AVG/MIN/MAX(DISTINCT col). `VALUES (1), (2)` as a query / FROM subquery.
//! WHERE: comparisons (expr vs expr), `IS [NOT] NULL`, `LIKE`, `IN (lits | SELECT)`, `BETWEEN`, scalar subquery, `EXISTS` / `NOT EXISTS` (uncorrelated).
//! UNION [ALL] of two SELECTs (same schema). Correlated subquery → UnsupportedSql.
//! JOIN: [INNER] JOIN / LEFT [OUTER] JOIN / RIGHT [OUTER] JOIN / FULL [OUTER] JOIN … ON eq.
//! USING / NATURAL / comma-join → UnsupportedJoin. Recursive CTE / named WINDOW → UnsupportedSql.

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

pub const ArithOp = enum { add, sub, mul, div, mod };

pub const Expr = union(enum) {
    literal: Literal,
    column: []const u8,
    agg: struct {
        kind: AggKind,
        arg: ?[]const u8,
        distinct: bool = false,
    },
    call: Call,
    binary: struct {
        op: ArithOp,
        left: *Expr,
        right: *Expr,
    },
    unary_minus: *Expr,
    subquery: *Query,
};

pub const Call = struct {
    kind: ScalarKind,
    args: []const *Expr,
    cast_type: ?CastType = null,
};

pub const Computed = struct {
    expr: *Expr,
    alias: ?[]const u8,
    sql_text: []const u8,
};

pub const AggKind = enum { count, sum, avg, min, max };

pub const Agg = struct {
    kind: AggKind,
    /// null means `*` (COUNT only).
    arg: ?[]const u8,
    alias: ?[]const u8,
    distinct: bool = false,
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
    left: *Expr,
    right: *Expr,
};

pub fn asColumn(e: *const Expr) ?[]const u8 {
    return switch (e.*) {
        .column => |n| n,
        else => null,
    };
}

pub fn asLiteral(e: *const Expr) ?Literal {
    return switch (e.*) {
        .literal => |v| v,
        else => null,
    };
}

pub fn asAgg(e: *const Expr) ?CmpLeft {
    return switch (e.*) {
        .agg => |a| .{ .agg = .{ .kind = a.kind, .arg = a.arg } },
        .column => |n| .{ .column = n },
        else => null,
    };
}

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
    left: *Expr,
    query: *Query,
};

pub const Between = struct {
    left: CmpLeft,
    lo: Literal,
    hi: Literal,
    negated: bool,
};

pub const Exists = struct {
    query: *Query,
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
    exists: Exists,
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

pub const ScalarKind = enum {
    abs,
    round,
    cast,
    coalesce,
    lower,
    upper,
    length,
    trim,
    ltrim,
    rtrim,
    replace,
    substr,
    concat,
    date_trunc,
    extract,
    year,
    month,
    day,
    hour,
    minute,
    second,
    left,
    right,
    starts_with,
    ends_with,
    contains,
    position,
    ceil,
    floor,
    sign,
    greatest,
    least,
};

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
    expr: Computed,
};

pub const OrderBy = struct {
    column: []const u8,
    desc: bool = false,
};

pub const AsOf = union(enum) {
    snapshot: i64,
    timestamp_ms: i64,
};

pub const Query = struct {
    items: []const SelectItem,
    from: []const u8,
    from_alias: ?[]const u8 = null,
    from_sub: ?*Query = null,
    as_of: ?AsOf = null,
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

    /// `SELECT 1` / `SELECT 1, 'x'` with no FROM: one row, no scan.
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

    /// `SELECT 1` / `SELECT 1+2` / `SELECT abs(1)` with no FROM: one row, no scan.
    /// `SELECT *` / `SELECT id` without FROM still scan the connected table.
    pub fn isNoFrom(self: Query) bool {
        if (self.union_right != null or self.from_sub != null or self.join != null) return false;
        if (self.from.len != 0) return false;
        if (self.where != null or self.having != null or self.needsAgg() or self.distinct) return false;
        if (self.group_by.len != 0) return false;
        if (self.items.len == 0) return false;
        for (self.items) |item| {
            switch (item) {
                .literal => {},
                .expr => |ex| if (!exprIsConstant(ex.expr)) return false,
                else => return false,
            }
        }
        return true;
    }
};

fn exprIsConstant(e: *const Expr) bool {
    return switch (e.*) {
        .literal => true,
        .column, .agg, .subquery => false,
        .unary_minus => |a| exprIsConstant(a),
        .binary => |b| exprIsConstant(b.left) and exprIsConstant(b.right),
        .call => |c| blk: {
            for (c.args) |arg| {
                if (!exprIsConstant(arg)) break :blk false;
            }
            break :blk true;
        },
    };
}

pub const Copy = struct {
    /// null means `SELECT *` of the connected table (`COPY TO`).
    query: ?Query = null,
    path: []const u8,
    /// Set for `COPY table FROM 'path'`. Then `query` is unused.
    from_table: ?[]const u8 = null,
};

pub const Show = union(enum) {
    catalogs,
    namespaces: struct { catalog: ?[]const u8 = null },
    tables: struct { catalog: ?[]const u8 = null, namespace: ?[]const u8 = null },
};

pub const Attach = struct {
    source: []const u8,
    name: []const u8,
};

pub const Describe = struct {
    ident: []const u8,
};

pub const Use = struct {
    catalog: []const u8,
    namespace: ?[]const u8 = null,
};

pub const Detach = struct {
    name: []const u8,
};

pub const ColDef = struct {
    name: []const u8,
    type_name: []const u8,
};

pub const CreateNamespace = struct {
    ident: []const u8,
};

pub const CreateTable = struct {
    ident: []const u8,
    columns: []const ColDef = &.{},
    as_select: ?Query = null,
    /// Iceberg identity columns. Empty = unpartitioned.
    partition_by: []const []const u8 = &.{},
};

pub const Insert = struct {
    ident: []const u8,
    query: Query,
};

pub const DropTable = struct {
    ident: []const u8,
};

pub const Delete = struct {
    ident: []const u8,
    where: ?*BoolExpr = null,
};

pub const AssignValue = union(enum) {
    literal: Literal,
    column: QualName,
};

pub const Assignment = struct {
    column: []const u8,
    value: AssignValue,
};

pub const Update = struct {
    ident: []const u8,
    assignments: []const Assignment,
    where: ?*BoolExpr = null,
};

pub const MergeMatched = union(enum) {
    delete,
    update: []const Assignment,
};

pub const MergeInsert = struct {
    columns: []const []const u8 = &.{},
    values: []const AssignValue = &.{},
};

pub const Merge = struct {
    ident: []const u8,
    alias: ?[]const u8 = null,
    source: []const u8,
    source_alias: ?[]const u8 = null,
    source_sub: ?*Query = null,
    eqs: []const JoinEq,
    matched: ?MergeMatched = null,
    not_matched: ?MergeInsert = null,
};

pub const AlterTable = struct {
    ident: []const u8,
    column: ColDef,
};

pub const Stmt = union(enum) {
    query: Query,
    copy: Copy,
    show: Show,
    attach: Attach,
    describe: Describe,
    use: Use,
    detach: Detach,
    create_namespace: CreateNamespace,
    create_table: CreateTable,
    insert: Insert,
    delete: Delete,
    update: Update,
    merge: Merge,
    alter_table: AlterTable,
    drop_table: DropTable,
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
    show,
    attach,
    describe,
    use,
    detach,
    create,
    insert,
    drop,
    delete,
    update,
    merge,
    alter,
    set,
    using,
    into,
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
    values,
    exists,
    @"for",
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
    plus,
    minus,
    slash,
    percent,
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
        if (c == '+') {
            self.pos += 1;
            return .{ .kind = .plus, .start = start, .len = 1 };
        }
        if (c == '/') {
            self.pos += 1;
            return .{ .kind = .slash, .start = start, .len = 1 };
        }
        if (c == '%') {
            self.pos += 1;
            return .{ .kind = .percent, .start = start, .len = 1 };
        }
        if (c == '-') {
            self.pos += 1;
            return .{ .kind = .minus, .start = start, .len = 1 };
        }
        if (isDigit(c)) {
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
            else if (eqlKw(lexeme, "show"))
                .show
            else if (eqlKw(lexeme, "attach"))
                .attach
            else if (eqlKw(lexeme, "describe"))
                .describe
            else if (eqlKw(lexeme, "use"))
                .use
            else if (eqlKw(lexeme, "detach"))
                .detach
            else if (eqlKw(lexeme, "create"))
                .create
            else if (eqlKw(lexeme, "insert"))
                .insert
            else if (eqlKw(lexeme, "drop"))
                .drop
            else if (eqlKw(lexeme, "delete"))
                .delete
            else if (eqlKw(lexeme, "update"))
                .update
            else if (eqlKw(lexeme, "merge"))
                .merge
            else if (eqlKw(lexeme, "alter"))
                .alter
            else if (eqlKw(lexeme, "set"))
                .set
            else if (eqlKw(lexeme, "using"))
                .using
            else if (eqlKw(lexeme, "into"))
                .into
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
            else if (eqlKw(lexeme, "values"))
                .values
            else if (eqlKw(lexeme, "natural") or eqlKw(lexeme, "cross"))
                return error.UnsupportedJoin
            else if (eqlKw(lexeme, "exists"))
                .exists
            else if (eqlKw(lexeme, "for"))
                .@"for"
            else if (eqlKw(lexeme, "window") or eqlKw(lexeme, "except") or eqlKw(lexeme, "intersect") or
                eqlKw(lexeme, "recursive"))
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
    if (eqlKw(name, "lower")) return .lower;
    if (eqlKw(name, "upper")) return .upper;
    if (eqlKw(name, "length") or eqlKw(name, "char_length") or eqlKw(name, "character_length")) return .length;
    if (eqlKw(name, "trim")) return .trim;
    if (eqlKw(name, "ltrim")) return .ltrim;
    if (eqlKw(name, "rtrim")) return .rtrim;
    if (eqlKw(name, "replace")) return .replace;
    if (eqlKw(name, "substr") or eqlKw(name, "substring")) return .substr;
    if (eqlKw(name, "concat")) return .concat;
    if (eqlKw(name, "date_trunc")) return .date_trunc;
    if (eqlKw(name, "extract")) return .extract;
    if (eqlKw(name, "year")) return .year;
    if (eqlKw(name, "month")) return .month;
    if (eqlKw(name, "day")) return .day;
    if (eqlKw(name, "hour")) return .hour;
    if (eqlKw(name, "minute")) return .minute;
    if (eqlKw(name, "second")) return .second;
    if (eqlKw(name, "left")) return .left;
    if (eqlKw(name, "right")) return .right;
    if (eqlKw(name, "starts_with") or eqlKw(name, "startswith")) return .starts_with;
    if (eqlKw(name, "ends_with") or eqlKw(name, "endswith")) return .ends_with;
    if (eqlKw(name, "contains")) return .contains;
    if (eqlKw(name, "strpos") or eqlKw(name, "instr")) return .position;
    if (eqlKw(name, "ceil") or eqlKw(name, "ceiling")) return .ceil;
    if (eqlKw(name, "floor")) return .floor;
    if (eqlKw(name, "sign")) return .sign;
    if (eqlKw(name, "greatest")) return .greatest;
    if (eqlKw(name, "least")) return .least;
    return null;
}

fn lexerCursor(lexer: *Lexer) usize {
    if (lexer.peeked) |t| return t.start;
    return lexer.pos;
}

fn trimSql(s: []const u8) []const u8 {
    var a: usize = 0;
    var b: usize = s.len;
    while (a < b and (s[a] == ' ' or s[a] == '\t' or s[a] == '\n' or s[a] == '\r')) a += 1;
    while (b > a and (s[b - 1] == ' ' or s[b - 1] == '\t' or s[b - 1] == '\n' or s[b - 1] == '\r')) b -= 1;
    return s[a..b];
}

fn identFollowedByLparen(lexer: *Lexer, sql: []const u8) bool {
    const t = lexer.peek() catch return false;
    if (t.kind != .ident) return false;
    var i = t.start + t.len;
    while (i < sql.len) : (i += 1) {
        const c = sql[i];
        if (c == ' ' or c == '\t' or c == '\n' or c == '\r') continue;
        return c == '(';
    }
    return false;
}

fn allocVal(allocator: std.mem.Allocator, expr: Expr) ParseError!*Expr {
    const p = try allocator.create(Expr);
    p.* = expr;
    return p;
}

fn parseValueExpr(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*Expr {
    return parseSum(allocator, lexer, sql, allow_agg);
}

fn parseSum(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*Expr {
    var left = try parseProduct(allocator, lexer, sql, allow_agg);
    while (true) {
        const k = (try lexer.peek()).kind;
        const op: ArithOp = switch (k) {
            .plus => .add,
            .minus => .sub,
            else => return left,
        };
        _ = try lexer.next();
        const right = try parseProduct(allocator, lexer, sql, allow_agg);
        left = try allocVal(allocator, .{ .binary = .{ .op = op, .left = left, .right = right } });
    }
}

fn parseProduct(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*Expr {
    var left = try parsePrefix(allocator, lexer, sql, allow_agg);
    while (true) {
        const k = (try lexer.peek()).kind;
        const op: ArithOp = switch (k) {
            .star => .mul,
            .slash => .div,
            .percent => .mod,
            else => return left,
        };
        _ = try lexer.next();
        const right = try parsePrefix(allocator, lexer, sql, allow_agg);
        left = try allocVal(allocator, .{ .binary = .{ .op = op, .left = left, .right = right } });
    }
}

fn parsePrefix(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*Expr {
    if ((try lexer.peek()).kind == .minus) {
        _ = try lexer.next();
        const arg = try parsePrefix(allocator, lexer, sql, allow_agg);
        if (arg.* == .literal) {
            switch (arg.literal) {
                .int => |v| arg.literal = .{ .int = -v },
                .float => |v| arg.literal = .{ .float = -v },
                else => return allocVal(allocator, .{ .unary_minus = arg }),
            }
            return arg;
        }
        return allocVal(allocator, .{ .unary_minus = arg });
    }
    return parseAtom(allocator, lexer, sql, allow_agg);
}

fn parseAtom(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*Expr {
    const t = try lexer.next();
    switch (t.kind) {
        .number => return allocVal(allocator, .{ .literal = try parseNumber(t.slice(sql)) }),
        .string => return allocVal(allocator, .{ .literal = .{ .string = try unescape(allocator, unquote(sql, t)) } }),
        .null => return allocVal(allocator, .{ .literal = .null }),
        .left, .right => {
            if ((try lexer.peek()).kind != .lparen) return error.InvalidSyntax;
            _ = try lexer.next();
            const fname: []const u8 = if (t.kind == .left) "left" else "right";
            return parseCallOrAgg(allocator, lexer, sql, fname, allow_agg);
        },
        .lparen => {
            const pk = (try lexer.peek()).kind;
            if (pk == .select or pk == .with) {
                const sub = try allocator.create(Query);
                sub.* = try parseQuery(allocator, lexer, sql, .rparen);
                _ = try expect(lexer, .rparen);
                return allocVal(allocator, .{ .subquery = sub });
            }
            const inner = try parseValueExpr(allocator, lexer, sql, allow_agg);
            _ = try expect(lexer, .rparen);
            return inner;
        },
        .ident => {
            if ((try lexer.peek()).kind == .lparen) {
                _ = try lexer.next();
                return parseCallOrAgg(allocator, lexer, sql, t.slice(sql), allow_agg);
            }
            var name = t.slice(sql);
            if ((try lexer.peek()).kind == .dot) {
                _ = try lexer.next();
                const rest = try expect(lexer, .ident);
                name = sql[t.start .. rest.start + rest.len];
            }
            return allocVal(allocator, .{ .column = name });
        },
        else => return error.InvalidSyntax,
    }
}

fn parseCallOrAgg(
    allocator: std.mem.Allocator,
    lexer: *Lexer,
    sql: []const u8,
    name: []const u8,
    allow_agg: bool,
) ParseError!*Expr {
    if (parseAggKind(name)) |kind| {
        if (!allow_agg) return error.UnsupportedSql;
        const parsed = try parseAggArg(lexer, sql, kind);
        return allocVal(allocator, .{ .agg = .{ .kind = kind, .arg = parsed.arg, .distinct = parsed.distinct } });
    }
    const skind = parseScalarKind(name) orelse return error.UnsupportedSql;
    if (skind == .extract) {
        return parseExtractCall(allocator, lexer, sql, allow_agg);
    }
    var args: std.ArrayList(*Expr) = .empty;
    defer args.deinit(allocator);
    var cast_type: ?CastType = null;
    if (skind == .cast) {
        try args.append(allocator, try parseValueExpr(allocator, lexer, sql, allow_agg));
        _ = try expect(lexer, .as);
        const ty = try expect(lexer, .ident);
        cast_type = parseCastType(ty.slice(sql)) orelse return error.UnsupportedSql;
        _ = try expect(lexer, .rparen);
    } else {
        if ((try lexer.peek()).kind != .rparen) {
            while (true) {
                try args.append(allocator, try parseValueExpr(allocator, lexer, sql, allow_agg));
                if ((try lexer.peek()).kind != .comma) break;
                _ = try lexer.next();
            }
        }
        _ = try expect(lexer, .rparen);
        try checkCallArity(skind, args.items.len);
    }
    return allocVal(allocator, .{ .call = .{
        .kind = skind,
        .args = try args.toOwnedSlice(allocator),
        .cast_type = cast_type,
    } });
}

fn parseExtractCall(
    allocator: std.mem.Allocator,
    lexer: *Lexer,
    sql: []const u8,
    allow_agg: bool,
) ParseError!*Expr {
    const first = try lexer.peek();
    if (first.kind == .ident) {
        const unit_tok = try lexer.next();
        const sep = (try lexer.peek()).kind;
        if (sep != .from and sep != .comma) return error.InvalidSyntax;
        _ = try lexer.next();
        const arg = try parseValueExpr(allocator, lexer, sql, allow_agg);
        _ = try expect(lexer, .rparen);
        const unit_e = try allocVal(allocator, .{ .literal = .{ .string = unit_tok.slice(sql) } });
        const args = try allocator.alloc(*Expr, 2);
        args[0] = unit_e;
        args[1] = arg;
        return allocVal(allocator, .{ .call = .{ .kind = .extract, .args = args } });
    }
    var args: std.ArrayList(*Expr) = .empty;
    defer args.deinit(allocator);
    try args.append(allocator, try parseValueExpr(allocator, lexer, sql, allow_agg));
    _ = try expect(lexer, .comma);
    try args.append(allocator, try parseValueExpr(allocator, lexer, sql, allow_agg));
    _ = try expect(lexer, .rparen);
    return allocVal(allocator, .{ .call = .{
        .kind = .extract,
        .args = try args.toOwnedSlice(allocator),
    } });
}

const ParsedAggArg = struct { arg: ?[]const u8, distinct: bool };

fn parseAggArg(lexer: *Lexer, sql: []const u8, kind: AggKind) ParseError!ParsedAggArg {
    const arg_tok = try lexer.next();
    if (arg_tok.kind == .distinct) {
        const col = try expect(lexer, .ident);
        _ = try expect(lexer, .rparen);
        return .{ .arg = col.slice(sql), .distinct = true };
    }
    const arg: ?[]const u8 = switch (arg_tok.kind) {
        .star => blk: {
            if (kind != .count) return error.InvalidSyntax;
            break :blk null;
        },
        .ident => arg_tok.slice(sql),
        else => return error.InvalidSyntax,
    };
    _ = try expect(lexer, .rparen);
    return .{ .arg = arg, .distinct = false };
}

fn checkCallArity(kind: ScalarKind, n: usize) ParseError!void {
    switch (kind) {
        .abs, .round, .lower, .upper, .length, .trim, .ltrim, .rtrim, .year, .month, .day, .hour, .minute, .second, .ceil, .floor, .sign => if (n != 1) return error.InvalidSyntax,
        .cast => {},
        .coalesce, .greatest, .least => if (n < 2) return error.InvalidSyntax,
        .substr => if (n < 2 or n > 3) return error.InvalidSyntax,
        .concat => if (n < 2) return error.InvalidSyntax,
        .date_trunc, .extract, .left, .right, .starts_with, .ends_with, .contains, .position => if (n != 2) return error.InvalidSyntax,
        .replace => if (n != 3) return error.InvalidSyntax,
    }
}

fn simpleCallToScalar(c: Call) ?Scalar {
    if (c.args.len == 0) return null;
    const a0 = asColumn(c.args[0]) orelse return null;
    switch (c.kind) {
        .abs, .round => {
            if (c.args.len != 1) return null;
            return .{ .kind = c.kind, .arg = a0, .alias = null };
        },
        .cast => {
            if (c.args.len != 1) return null;
            return .{ .kind = .cast, .arg = a0, .cast_type = c.cast_type, .alias = null };
        },
        .coalesce => {
            if (c.args.len != 2) return null;
            if (asColumn(c.args[1])) |c1|
                return .{ .kind = .coalesce, .arg = a0, .coalesce_col = c1, .alias = null };
            if (asLiteral(c.args[1])) |lit|
                return .{ .kind = .coalesce, .arg = a0, .coalesce_lit = lit, .alias = null };
            return null;
        },
        .lower, .upper, .length, .trim, .ltrim, .rtrim, .replace, .substr, .concat, .date_trunc, .extract, .year, .month, .day, .hour, .minute, .second, .left, .right, .starts_with, .ends_with, .contains, .position, .ceil, .floor, .sign, .greatest, .least => return null,
    }
}

fn itemFromValue(expr: *Expr, alias: ?[]const u8, sql_text: []const u8) SelectItem {
    const text = trimSql(sql_text);
    switch (expr.*) {
        .literal => |v| {
            const name = alias orelse switch (v) {
                .string => |s| s,
                .null => "null",
                else => text,
            };
            return .{ .literal = .{ .value = v, .name = name } };
        },
        .column => |n| {
            if (std.mem.indexOfScalar(u8, n, '.')) |dot| {
                return .{ .column = .{ .qualifier = n[0..dot], .name = n[dot + 1 ..], .alias = alias } };
            }
            return .{ .column = .{ .name = n, .alias = alias } };
        },
        .agg => |a| return .{ .agg = .{ .kind = a.kind, .arg = a.arg, .alias = alias, .distinct = a.distinct } },
        .call => |c| {
            if (simpleCallToScalar(c)) |s| {
                var sc = s;
                sc.alias = alias;
                return .{ .scalar = sc };
            }
            return .{ .expr = .{ .expr = expr, .alias = alias, .sql_text = text } };
        },
        else => return .{ .expr = .{ .expr = expr, .alias = alias, .sql_text = text } },
    }
}

fn isSelectAggOrWindow(lexer: *Lexer, sql: []const u8) bool {
    const t = lexer.peek() catch return false;
    if (t.kind != .ident or !identFollowedByLparen(lexer, sql)) return false;
    const name = t.slice(sql);
    return parseRankingKind(name) != null or parseLagLeadKind(name) != null or parseAggKind(name) != null;
}

fn parseSelectItem(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator) ParseError!SelectItem {
    const t = try lexer.peek();
    if (t.kind == .star) {
        _ = try lexer.next();
        return .star;
    }
    if (t.kind == .case) {
        _ = try lexer.next();
        return parseCaseItem(lexer, sql, allocator);
    }
    if (t.kind == .ident and identFollowedByLparen(lexer, sql)) {
        const name = t.slice(sql);
        if (parseRankingKind(name) != null or parseLagLeadKind(name) != null or parseAggKind(name) != null) {
            return parseSelectAggOrWindow(lexer, sql, allocator);
        }
    }
    const start = (try lexer.peek()).start;
    const expr = try parseValueExpr(allocator, lexer, sql, false);
    const text = sql[start..lexerCursor(lexer)];
    const alias = try optionalAlias(lexer, sql);
    return itemFromValue(expr, alias, text);
}

fn parseSelectAggOrWindow(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator) ParseError!SelectItem {
    const t = try lexer.next();
    _ = try expect(lexer, .lparen);
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
        const parsed = try parseAggArg(lexer, sql, kind);
        if ((try lexer.peek()).kind == .over) {
            if (parsed.distinct) return error.UnsupportedSql;
            const spec = try parseWindowSpec(allocator, lexer, sql);
            const alias = try optionalAlias(lexer, sql);
            return .{ .window = .{
                .kind = windowKindFromAgg(kind),
                .arg = parsed.arg,
                .alias = alias,
                .spec = spec,
            } };
        }
        const alias = try optionalAlias(lexer, sql);
        return .{ .agg = .{ .kind = kind, .arg = parsed.arg, .alias = alias, .distinct = parsed.distinct } };
    }
    return error.InvalidSyntax;
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
        .lower, .upper, .length, .trim, .ltrim, .rtrim, .replace, .substr, .concat, .date_trunc, .extract, .year, .month, .day, .hour, .minute, .second, .left, .right, .starts_with, .ends_with, .contains, .position, .ceil, .floor, .sign, .greatest, .least => return error.UnsupportedSql,
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

fn parseExists(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, negated: bool) ParseError!*BoolExpr {
    _ = try expect(lexer, .exists);
    _ = try expect(lexer, .lparen);
    const sub = try allocator.create(Query);
    sub.* = try parseQuery(allocator, lexer, sql, .rparen);
    _ = try expect(lexer, .rparen);
    return allocExpr(allocator, .{ .exists = .{ .query = sub, .negated = negated } });
}

fn parsePrimary(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*BoolExpr {
    if ((try lexer.peek()).kind == .not) {
        const saved = lexer.*;
        _ = try lexer.next();
        if ((try lexer.peek()).kind == .exists) return parseExists(allocator, lexer, sql, true);
        lexer.* = saved;
    }
    if ((try lexer.peek()).kind == .exists) return parseExists(allocator, lexer, sql, false);
    if ((try lexer.peek()).kind == .lparen) {
        const saved = lexer.*;
        _ = try lexer.next();
        if (parseValueExpr(allocator, lexer, sql, allow_agg)) |val| {
            if ((try lexer.peek()).kind == .rparen) {
                _ = try lexer.next();
                if (isPredOp((try lexer.peek()).kind, sql, lexer)) {
                    return parsePredFromLeft(allocator, lexer, sql, allow_agg, val);
                }
            }
        } else |_| {}
        lexer.* = saved;
        _ = try lexer.next();
        const inner = try parseOr(allocator, lexer, sql, allow_agg);
        _ = try expect(lexer, .rparen);
        return inner;
    }
    return parsePred(allocator, lexer, sql, allow_agg);
}

fn isPredOp(kind: Kind, sql: []const u8, lexer: *Lexer) bool {
    return switch (kind) {
        .eq, .ne, .lt, .le, .gt, .ge, .like, .in, .between, .not => true,
        .ident => eqlKw((lexer.peek() catch return false).slice(sql), "is"),
        else => false,
    };
}

fn exprToCmpLeft(e: *const Expr) ParseError!CmpLeft {
    return asAgg(e) orelse error.UnsupportedSql;
}

fn parsePred(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, allow_agg: bool) ParseError!*BoolExpr {
    const left_e = try parseValueExpr(allocator, lexer, sql, allow_agg);
    return parsePredFromLeft(allocator, lexer, sql, allow_agg, left_e);
}

fn parsePredFromLeft(
    allocator: std.mem.Allocator,
    lexer: *Lexer,
    sql: []const u8,
    allow_agg: bool,
    left_e: *Expr,
) ParseError!*BoolExpr {
    const op_tok = try lexer.next();
    if (op_tok.kind == .ident and eqlKw(op_tok.slice(sql), "is")) {
        var negated = false;
        var t = try lexer.next();
        if (t.kind == .not or (t.kind == .ident and eqlKw(t.slice(sql), "not"))) {
            negated = true;
            t = try lexer.next();
        }
        if (t.kind != .null and !(t.kind == .ident and eqlKw(t.slice(sql), "null"))) return error.InvalidSyntax;
        return allocExpr(allocator, .{ .isnull = .{ .left = try exprToCmpLeft(left_e), .negated = negated } });
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
        return allocExpr(allocator, .{ .like = .{
            .left = try exprToCmpLeft(left_e),
            .pattern = pattern,
            .negated = negated,
        } });
    }
    if (kind == .in) {
        _ = try expect(lexer, .lparen);
        const left = try exprToCmpLeft(left_e);
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
        return allocExpr(allocator, .{ .between = .{
            .left = try exprToCmpLeft(left_e),
            .lo = lo,
            .hi = hi,
            .negated = negated,
        } });
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
            return allocExpr(allocator, .{ .cmp_query = .{ .op = op, .left = left_e, .query = sub } });
        }
        const inner = try parseValueExpr(allocator, lexer, sql, allow_agg);
        _ = try expect(lexer, .rparen);
        return allocExpr(allocator, .{ .cmp = .{ .op = op, .left = left_e, .right = inner } });
    }
    const right = try parseValueExpr(allocator, lexer, sql, allow_agg);
    return allocExpr(allocator, .{ .cmp = .{ .op = op, .left = left_e, .right = right } });
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
        .copy, .show, .attach, .describe, .use, .detach,
        .create_namespace, .create_table, .insert, .delete, .update, .merge, .alter_table, .drop_table => return error.InvalidSyntax,
    }
}

pub fn parseStmt(allocator: std.mem.Allocator, sql: []const u8) !Stmt {
    var lexer: Lexer = .{ .sql = sql };
    if ((try lexer.peek()).kind == .copy) {
        return .{ .copy = try parseCopy(allocator, &lexer, sql) };
    }
    if ((try lexer.peek()).kind == .show) {
        return .{ .show = try parseShow(&lexer, sql) };
    }
    if ((try lexer.peek()).kind == .attach) {
        return .{ .attach = try parseAttach(&lexer, sql) };
    }
    if ((try lexer.peek()).kind == .describe) {
        return .{ .describe = try parseDescribe(&lexer, sql) };
    }
    if ((try lexer.peek()).kind == .use) {
        return .{ .use = try parseUse(&lexer, sql) };
    }
    if ((try lexer.peek()).kind == .detach) {
        return .{ .detach = try parseDetach(&lexer, sql) };
    }
    if ((try lexer.peek()).kind == .create) {
        return parseCreate(allocator, &lexer, sql);
    }
    if ((try lexer.peek()).kind == .insert) {
        return .{ .insert = try parseInsert(allocator, &lexer, sql) };
    }
    if ((try lexer.peek()).kind == .drop) {
        return .{ .drop_table = try parseDropTable(&lexer, sql) };
    }
    if ((try lexer.peek()).kind == .delete) {
        return .{ .delete = try parseDelete(allocator, &lexer, sql) };
    }
    if ((try lexer.peek()).kind == .update) {
        return .{ .update = try parseUpdate(allocator, &lexer, sql) };
    }
    if ((try lexer.peek()).kind == .merge) {
        return .{ .merge = try parseMerge(allocator, &lexer, sql) };
    }
    if ((try lexer.peek()).kind == .alter) {
        return .{ .alter_table = try parseAlterTable(&lexer, sql) };
    }
    return .{ .query = try parseQuery(allocator, &lexer, sql, .eof) };
}

fn parseShow(lexer: *Lexer, sql: []const u8) !Show {
    _ = try expect(lexer, .show);
    const what = try expect(lexer, .ident);
    const word = what.slice(sql);
    if (eqlKw(word, "catalogs")) {
        if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
        return .catalogs;
    }
    if (eqlKw(word, "namespaces")) {
        var catalog: ?[]const u8 = null;
        if ((try lexer.peek()).kind == .from) {
            _ = try lexer.next();
            catalog = try parseDottedIdent(lexer, sql);
        }
        if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
        return .{ .namespaces = .{ .catalog = catalog } };
    }
    if (eqlKw(word, "tables")) {
        var catalog: ?[]const u8 = null;
        var namespace: ?[]const u8 = null;
        if ((try lexer.peek()).kind == .from) {
            _ = try lexer.next();
            const dotted = try parseDottedIdent(lexer, sql);
            if (std.mem.indexOfScalar(u8, dotted, '.')) |dot| {
                catalog = dotted[0..dot];
                namespace = dotted[dot + 1 ..];
            } else {
                catalog = dotted;
            }
        }
        if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
        return .{ .tables = .{ .catalog = catalog, .namespace = namespace } };
    }
    return error.InvalidSyntax;
}

fn parseAttach(lexer: *Lexer, sql: []const u8) !Attach {
    _ = try expect(lexer, .attach);
    if ((try lexer.peek()).kind == .ident) {
        const word = (try lexer.peek()).slice(sql);
        if (eqlKw(word, "catalog")) _ = try lexer.next();
    }
    const src_tok = try lexer.next();
    const source = switch (src_tok.kind) {
        .string => unquote(sql, src_tok),
        .ident => src_tok.slice(sql),
        else => return error.InvalidSyntax,
    };
    _ = try expect(lexer, .as);
    const name = try expect(lexer, .ident);
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .source = source, .name = name.slice(sql) };
}

fn parseDescribe(lexer: *Lexer, sql: []const u8) !Describe {
    _ = try expect(lexer, .describe);
    if ((try lexer.peek()).kind == .ident) {
        const word = (try lexer.peek()).slice(sql);
        if (eqlKw(word, "table")) _ = try lexer.next();
    }
    const ident = try parseDottedIdent(lexer, sql);
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .ident = ident };
}

fn parseUse(lexer: *Lexer, sql: []const u8) !Use {
    _ = try expect(lexer, .use);
    if ((try lexer.peek()).kind == .ident) {
        const word = (try lexer.peek()).slice(sql);
        if (eqlKw(word, "catalog")) _ = try lexer.next();
    }
    const dotted = try parseDottedIdent(lexer, sql);
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    if (std.mem.indexOfScalar(u8, dotted, '.')) |dot| {
        if (std.mem.indexOfScalar(u8, dotted[dot + 1 ..], '.') != null) return error.InvalidSyntax;
        return .{ .catalog = dotted[0..dot], .namespace = dotted[dot + 1 ..] };
    }
    return .{ .catalog = dotted, .namespace = null };
}

fn parseDetach(lexer: *Lexer, sql: []const u8) !Detach {
    _ = try expect(lexer, .detach);
    if ((try lexer.peek()).kind == .ident) {
        const word = (try lexer.peek()).slice(sql);
        if (eqlKw(word, "catalog")) _ = try lexer.next();
    }
    const name = try expect(lexer, .ident);
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .name = name.slice(sql) };
}

fn parseCreate(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !Stmt {
    _ = try expect(lexer, .create);
    const kind_tok = try lexer.next();
    const word = kind_tok.slice(sql);
    if (eqlKw(word, "namespace") or eqlKw(word, "schema")) {
        const ident = try parseDottedIdent(lexer, sql);
        if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
        return .{ .create_namespace = .{ .ident = ident } };
    }
    if (eqlKw(word, "table")) {
        return .{ .create_table = try parseCreateTable(allocator, lexer, sql) };
    }
    return error.InvalidSyntax;
}

fn parseCreateTable(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !CreateTable {
    const ident = try parseDottedIdent(lexer, sql);
    var partition_by: std.ArrayList([]const u8) = .empty;
    defer partition_by.deinit(allocator);
    if (try peekPartitioned(lexer, sql)) {
        try parsePartitionedBy(allocator, lexer, sql, &partition_by);
    }
    if ((try lexer.peek()).kind == .as) {
        _ = try lexer.next();
        const q = try parseQuery(allocator, lexer, sql, .eof);
        if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
        return .{
            .ident = ident,
            .as_select = q,
            .partition_by = try partition_by.toOwnedSlice(allocator),
        };
    }
    _ = try expect(lexer, .lparen);
    var cols: std.ArrayList(ColDef) = .empty;
    defer cols.deinit(allocator);
    while (true) {
        const name = try expect(lexer, .ident);
        const ty = try lexer.next();
        if (ty.kind != .ident) return error.InvalidSyntax;
        const type_name = icebergTypeName(ty.slice(sql)) orelse return error.UnsupportedSql;
        try cols.append(allocator, .{ .name = name.slice(sql), .type_name = type_name });
        if ((try lexer.peek()).kind != .comma) break;
        _ = try lexer.next();
    }
    _ = try expect(lexer, .rparen);
    if (try peekPartitioned(lexer, sql)) {
        if (partition_by.items.len != 0) return error.InvalidSyntax;
        try parsePartitionedBy(allocator, lexer, sql, &partition_by);
    }
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    if (cols.items.len == 0) return error.InvalidSyntax;
    return .{
        .ident = ident,
        .columns = try cols.toOwnedSlice(allocator),
        .partition_by = try partition_by.toOwnedSlice(allocator),
    };
}

fn peekPartitioned(lexer: *Lexer, sql: []const u8) !bool {
    const tok = try lexer.peek();
    if (tok.kind == .partition) return true;
    return tok.kind == .ident and eqlKw(tok.slice(sql), "partitioned");
}

fn parsePartitionedBy(
    allocator: std.mem.Allocator,
    lexer: *Lexer,
    sql: []const u8,
    out: *std.ArrayList([]const u8),
) !void {
    const tok = try lexer.next();
    if (tok.kind == .ident) {
        if (!eqlKw(tok.slice(sql), "partitioned")) return error.InvalidSyntax;
    } else if (tok.kind != .partition) return error.InvalidSyntax;
    _ = try expect(lexer, .by);
    _ = try expect(lexer, .lparen);
    while (true) {
        try out.append(allocator, try parsePartitionCol(lexer, sql));
        if ((try lexer.peek()).kind != .comma) break;
        _ = try lexer.next();
    }
    _ = try expect(lexer, .rparen);
    if (out.items.len == 0) return error.InvalidSyntax;
    for (out.items, 0..) |name, i| {
        for (out.items[0..i]) |prev| {
            if (std.ascii.eqlIgnoreCase(prev, name)) return error.InvalidSyntax;
        }
    }
}

fn parsePartitionCol(lexer: *Lexer, sql: []const u8) ![]const u8 {
    const name = try parseColumnName(lexer, sql);
    if ((try lexer.peek()).kind != .lparen) return name;
    if (!eqlKw(name, "identity")) return error.UnsupportedPartitionSpec;
    _ = try lexer.next();
    const inner = try parseColumnName(lexer, sql);
    _ = try expect(lexer, .rparen);
    return inner;
}

fn icebergTypeName(name: []const u8) ?[]const u8 {
    if (eqlKw(name, "int") or eqlKw(name, "integer")) return "int";
    if (eqlKw(name, "bigint") or eqlKw(name, "long") or eqlKw(name, "int64")) return "long";
    if (eqlKw(name, "float") or eqlKw(name, "real")) return "float";
    if (eqlKw(name, "double") or eqlKw(name, "float64")) return "double";
    if (eqlKw(name, "bool") or eqlKw(name, "boolean")) return "boolean";
    if (eqlKw(name, "string") or eqlKw(name, "varchar") or eqlKw(name, "text") or eqlKw(name, "utf8"))
        return "string";
    return null;
}

fn parseInsert(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !Insert {
    _ = try expect(lexer, .insert);
    _ = try expect(lexer, .into);
    const ident = try parseDottedIdent(lexer, sql);
    const q = if ((try lexer.peek()).kind == .values)
        try parseValues(allocator, lexer, sql, .eof)
    else
        try parseQuery(allocator, lexer, sql, .eof);
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .ident = ident, .query = q };
}

fn parseDropTable(lexer: *Lexer, sql: []const u8) !DropTable {
    _ = try expect(lexer, .drop);
    const kind_tok = try lexer.next();
    const word = kind_tok.slice(sql);
    if (!eqlKw(word, "table")) return error.InvalidSyntax;
    const ident = try parseDottedIdent(lexer, sql);
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .ident = ident };
}

fn parseDelete(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !Delete {
    _ = try expect(lexer, .delete);
    _ = try expect(lexer, .from);
    const ident = try parseDottedIdent(lexer, sql);
    var where_expr: ?*BoolExpr = null;
    if ((try lexer.peek()).kind == .where) {
        _ = try lexer.next();
        where_expr = try parseBool(allocator, lexer, sql, false);
    }
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .ident = ident, .where = where_expr };
}

fn parseAssignValue(lexer: *Lexer, sql: []const u8, allocator: std.mem.Allocator) !AssignValue {
    const tok = try lexer.peek();
    switch (tok.kind) {
        .number, .string, .null => return .{ .literal = try parseLiteralToken(lexer, sql, allocator) },
        .ident => return .{ .column = try parseQualName(lexer, sql) },
        else => return error.InvalidSyntax,
    }
}

fn parseAssignments(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) ![]Assignment {
    var out: std.ArrayList(Assignment) = .empty;
    defer out.deinit(allocator);
    while (true) {
        const column = try parseColumnName(lexer, sql);
        _ = try expect(lexer, .eq);
        const value = try parseAssignValue(lexer, sql, allocator);
        try out.append(allocator, .{ .column = column, .value = value });
        if ((try lexer.peek()).kind != .comma) break;
        _ = try lexer.next();
    }
    if (out.items.len == 0) return error.InvalidSyntax;
    return out.toOwnedSlice(allocator);
}

fn parseUpdate(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !Update {
    _ = try expect(lexer, .update);
    const ident = try parseDottedIdent(lexer, sql);
    _ = try expect(lexer, .set);
    const assignments = try parseAssignments(allocator, lexer, sql);
    var where_expr: ?*BoolExpr = null;
    if ((try lexer.peek()).kind == .where) {
        _ = try lexer.next();
        where_expr = try parseBool(allocator, lexer, sql, false);
    }
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .ident = ident, .assignments = assignments, .where = where_expr };
}

fn parseJoinEqs(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) ![]JoinEq {
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
    if (eqs.items.len == 0) return error.InvalidSyntax;
    return eqs.toOwnedSlice(allocator);
}

fn parseMergeInsert(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !MergeInsert {
    _ = try expect(lexer, .insert);
    var columns: []const []const u8 = &.{};
    var values: []const AssignValue = &.{};
    if ((try lexer.peek()).kind == .lparen) {
        _ = try lexer.next();
        var cols: std.ArrayList([]const u8) = .empty;
        defer cols.deinit(allocator);
        try cols.append(allocator, try parseColumnName(lexer, sql));
        while ((try lexer.peek()).kind == .comma) {
            _ = try lexer.next();
            try cols.append(allocator, try parseColumnName(lexer, sql));
        }
        _ = try expect(lexer, .rparen);
        columns = try cols.toOwnedSlice(allocator);
    }
    if ((try lexer.peek()).kind == .values) {
        _ = try lexer.next();
        _ = try expect(lexer, .lparen);
        var vals: std.ArrayList(AssignValue) = .empty;
        defer vals.deinit(allocator);
        try vals.append(allocator, try parseAssignValue(lexer, sql, allocator));
        while ((try lexer.peek()).kind == .comma) {
            _ = try lexer.next();
            try vals.append(allocator, try parseAssignValue(lexer, sql, allocator));
        }
        _ = try expect(lexer, .rparen);
        values = try vals.toOwnedSlice(allocator);
        if (columns.len != 0 and columns.len != values.len) return error.InvalidSyntax;
    } else if (columns.len != 0) return error.InvalidSyntax;
    return .{ .columns = columns, .values = values };
}

fn parseMerge(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !Merge {
    _ = try expect(lexer, .merge);
    _ = try expect(lexer, .into);
    const ident = try parseDottedIdent(lexer, sql);
    var alias: ?[]const u8 = null;
    if ((try lexer.peek()).kind == .as) {
        _ = try lexer.next();
        alias = (try expect(lexer, .ident)).slice(sql);
    } else if ((try lexer.peek()).kind == .ident) {
        alias = (try lexer.next()).slice(sql);
    }
    _ = try expect(lexer, .using);
    var source: []const u8 = undefined;
    var source_alias: ?[]const u8 = null;
    var source_sub: ?*Query = null;
    if ((try lexer.peek()).kind == .lparen) {
        source_sub = try parseSubquery(allocator, lexer, sql);
        source_alias = try parseTableAlias(lexer, sql);
        if (source_alias == null) return error.InvalidSyntax;
        source = source_alias.?;
    } else {
        source = try parseDottedIdent(lexer, sql);
        if ((try lexer.peek()).kind == .as) {
            _ = try lexer.next();
            source_alias = (try expect(lexer, .ident)).slice(sql);
        } else if ((try lexer.peek()).kind == .ident) {
            source_alias = (try lexer.next()).slice(sql);
        }
    }
    _ = try expect(lexer, .on);
    const eqs = try parseJoinEqs(allocator, lexer, sql);
    var matched: ?MergeMatched = null;
    var not_matched: ?MergeInsert = null;
    while ((try lexer.peek()).kind == .when) {
        _ = try lexer.next();
        var is_not = false;
        if ((try lexer.peek()).kind == .not) {
            _ = try lexer.next();
            is_not = true;
        }
        const matched_tok = try expect(lexer, .ident);
        if (!eqlKw(matched_tok.slice(sql), "matched")) return error.InvalidSyntax;
        _ = try expect(lexer, .then);
        if (is_not) {
            if (not_matched != null) return error.InvalidSyntax;
            not_matched = try parseMergeInsert(allocator, lexer, sql);
        } else if ((try lexer.peek()).kind == .delete) {
            if (matched != null) return error.InvalidSyntax;
            _ = try lexer.next();
            matched = .delete;
        } else if ((try lexer.peek()).kind == .update) {
            if (matched != null) return error.InvalidSyntax;
            _ = try lexer.next();
            _ = try expect(lexer, .set);
            matched = .{ .update = try parseAssignments(allocator, lexer, sql) };
        } else return error.InvalidSyntax;
    }
    if (matched == null and not_matched == null) return error.InvalidSyntax;
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{
        .ident = ident,
        .alias = alias,
        .source = source,
        .source_alias = source_alias,
        .source_sub = source_sub,
        .eqs = eqs,
        .matched = matched,
        .not_matched = not_matched,
    };
}

fn parseAlterTable(lexer: *Lexer, sql: []const u8) !AlterTable {
    _ = try expect(lexer, .alter);
    const table_tok = try lexer.next();
    if (!eqlKw(table_tok.slice(sql), "table")) return error.InvalidSyntax;
    const ident = try parseDottedIdent(lexer, sql);
    const add_tok = try expect(lexer, .ident);
    if (!eqlKw(add_tok.slice(sql), "add")) return error.InvalidSyntax;
    const col_tok = try expect(lexer, .ident);
    if (!eqlKw(col_tok.slice(sql), "column")) return error.InvalidSyntax;
    const name = try parseColumnName(lexer, sql);
    const type_tok = try expect(lexer, .ident);
    const type_name = icebergTypeName(type_tok.slice(sql)) orelse return error.InvalidSyntax;
    if ((try lexer.peek()).kind != .eof) return error.InvalidSyntax;
    return .{ .ident = ident, .column = .{ .name = name, .type_name = type_name } };
}

fn parseQuery(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, stop: Kind) !Query {
    if ((try lexer.peek()).kind == .with) return parseWith(allocator, lexer, sql, stop);
    if ((try lexer.peek()).kind == .values) return parseValues(allocator, lexer, sql, stop);
    return parseSelect(allocator, lexer, sql, stop);
}

fn parseValues(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, stop: Kind) !Query {
    _ = try expect(lexer, .values);
    var rows: std.ArrayList([]const Literal) = .empty;
    defer rows.deinit(allocator);
    while (true) {
        _ = try expect(lexer, .lparen);
        var cols: std.ArrayList(Literal) = .empty;
        defer cols.deinit(allocator);
        try cols.append(allocator, try parseLiteralToken(lexer, sql, allocator));
        while ((try lexer.peek()).kind == .comma) {
            _ = try lexer.next();
            try cols.append(allocator, try parseLiteralToken(lexer, sql, allocator));
        }
        _ = try expect(lexer, .rparen);
        if (rows.items.len > 0 and cols.items.len != rows.items[0].len) return error.InvalidSyntax;
        try rows.append(allocator, try cols.toOwnedSlice(allocator));
        if ((try lexer.peek()).kind != .comma) break;
        _ = try lexer.next();
    }
    if (rows.items.len == 0) return error.InvalidSyntax;
    var q = try valuesRowQuery(allocator, rows.items[0]);
    var tail = &q;
    for (rows.items[1..]) |row| {
        const right = try allocator.create(Query);
        right.* = try valuesRowQuery(allocator, row);
        tail.union_all = true;
        tail.union_right = right;
        tail = right;
    }
    _ = stop;
    return q;
}

fn valuesRowQuery(allocator: std.mem.Allocator, row: []const Literal) !Query {
    const items = try allocator.alloc(SelectItem, row.len);
    for (row, 0..) |lit, i| {
        const name = try std.fmt.allocPrint(allocator, "column{d}", .{i + 1});
        items[i] = .{ .literal = .{ .value = lit, .name = name } };
    }
    return .{
        .items = items,
        .from = "",
        .where = null,
        .group_by = &.{},
        .having = null,
        .order_by = &.{},
        .distinct = false,
        .limit = null,
        .offset = null,
    };
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
        _ = try expect(lexer, .to);
        const path_tok = try expect(lexer, .string);
        const end = try lexer.next();
        if (end.kind != .eof) return error.InvalidSyntax;
        return .{ .query = query, .path = unquote(sql, path_tok) };
    }
    if (next.kind == .to) {
        _ = try lexer.next();
        const path_tok = try expect(lexer, .string);
        const end = try lexer.next();
        if (end.kind != .eof) return error.InvalidSyntax;
        return .{ .path = unquote(sql, path_tok) };
    }
    if (next.kind == .ident) {
        const ident = try parseDottedIdent(lexer, sql);
        if ((try lexer.peek()).kind == .from) {
            _ = try lexer.next();
            const path_tok = try expect(lexer, .string);
            const end = try lexer.next();
            if (end.kind != .eof) return error.InvalidSyntax;
            return .{ .path = unquote(sql, path_tok), .from_table = ident };
        }
        const from = ident;
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
        _ = try expect(lexer, .to);
        const path_tok = try expect(lexer, .string);
        const end = try lexer.next();
        if (end.kind != .eof) return error.InvalidSyntax;
        return .{ .query = query, .path = unquote(sql, path_tok) };
    }
    return error.InvalidSyntax;
}

fn parseSelect(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8, stop: Kind) !Query {
    var q = try parseSelectArm(allocator, lexer, sql);

    var tail: *Query = &q;
    while ((try lexer.peek()).kind == .@"union") {
        _ = try lexer.next();
        var union_all = false;
        if ((try lexer.peek()).kind == .all) {
            _ = try lexer.next();
            union_all = true;
        }
        const right = try allocator.create(Query);
        right.* = try parseSelectArm(allocator, lexer, sql);
        tail.union_all = union_all;
        tail.union_right = right;
        tail = right;
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
    q.order_by = try order_by.toOwnedSlice(allocator);

    if ((try lexer.peek()).kind == .limit) {
        _ = try lexer.next();
        q.limit = try parseU64(try expect(lexer, .number), sql);
        if ((try lexer.peek()).kind == .offset) {
            _ = try lexer.next();
            q.offset = try parseU64(try expect(lexer, .number), sql);
        }
    } else if ((try lexer.peek()).kind == .offset) {
        _ = try lexer.next();
        q.offset = try parseU64(try expect(lexer, .number), sql);
        if ((try lexer.peek()).kind == .limit) {
            _ = try lexer.next();
            q.limit = try parseU64(try expect(lexer, .number), sql);
        }
    }

    const end = try lexer.peek();
    if (end.kind != stop) {
        if (end.kind == .ident or end.kind == .select) return error.InvalidSyntax;
        return error.UnsupportedSql;
    }
    if (stop == .eof) _ = try lexer.next();
    return q;
}

fn parseSelectArm(allocator: std.mem.Allocator, lexer: *Lexer, sql: []const u8) !Query {
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
    var as_of: ?AsOf = null;
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
        as_of = try parseAsOf(lexer, sql);
        if (as_of != null and from_sub != null) return error.InvalidSyntax;
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

    if (having_expr != null and group_by.items.len == 0 and !hasAggItems(items.items)) {
        return error.InvalidSyntax;
    }

    return .{
        .items = try items.toOwnedSlice(allocator),
        .from = from,
        .from_alias = from_alias,
        .from_sub = from_sub,
        .as_of = as_of,
        .join = join,
        .where = where_expr,
        .group_by = try group_by.toOwnedSlice(allocator),
        .having = having_expr,
        .order_by = &.{},
        .distinct = distinct,
        .limit = null,
        .offset = null,
    };
}

fn hasAggItems(items: []const SelectItem) bool {
    for (items) |item| {
        if (item == .agg) return true;
    }
    return false;
}

fn parseAsOf(lexer: *Lexer, sql: []const u8) Error!?AsOf {
    if ((try lexer.peek()).kind != .@"for") return null;
    _ = try lexer.next();
    const what = try expect(lexer, .ident);
    const word = what.slice(sql);
    if (eqlKw(word, "snapshot")) {
        if ((try lexer.peek()).kind == .as) {
            _ = try lexer.next();
            const of = try expect(lexer, .ident);
            if (!eqlKw(of.slice(sql), "of")) return error.InvalidSyntax;
        }
        const n = try parseI64(try expect(lexer, .number), sql);
        return .{ .snapshot = n };
    }
    if (eqlKw(word, "timestamp")) {
        _ = try expect(lexer, .as);
        const of = try expect(lexer, .ident);
        if (!eqlKw(of.slice(sql), "of")) return error.InvalidSyntax;
        const tok = try lexer.next();
        const n = switch (tok.kind) {
            .number => try parseI64(tok, sql),
            .string => std.fmt.parseInt(i64, unquote(sql, tok), 10) catch return error.InvalidNumber,
            else => return error.InvalidSyntax,
        };
        return .{ .timestamp_ms = n };
    }
    return error.InvalidSyntax;
}

fn parseI64(tok: Token, sql: []const u8) Error!i64 {
    return std.fmt.parseInt(i64, tok.slice(sql), 10) catch error.InvalidNumber;
}

fn parseDottedIdent(lexer: *Lexer, sql: []const u8) Error![]const u8 {
    const first = try expect(lexer, .ident);
    var end = first.start + first.len;
    while ((try lexer.peek()).kind == .dot) {
        _ = try lexer.next();
        const part = try expect(lexer, .ident);
        end = part.start + part.len;
    }
    return sql[first.start..end];
}

fn parseFrom(lexer: *Lexer, sql: []const u8) Error![]const u8 {
    const t = try lexer.next();
    if (t.kind == .string) return unquote(sql, t);
    if (t.kind != .ident) return error.InvalidSyntax;
    var end = t.start + t.len;
    while ((try lexer.peek()).kind == .dot) {
        _ = try lexer.next();
        const part = try expect(lexer, .ident);
        end = part.start + part.len;
    }
    return sql[t.start..end];
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
    if ((try lexer.peek()).kind == .using) return error.UnsupportedJoin;
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
    const eqs = try parseJoinEqs(allocator, lexer, sql);
    return .{
        .kind = kind,
        .table = table,
        .alias = alias,
        .sub = sub,
        .eqs = eqs,
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

fn valueRefsOuter(e: *const Expr, inner: []const []const u8, outer: []const []const u8) bool {
    return switch (e.*) {
        .column => |n| columnRefersOuter(n, inner, outer),
        .binary => |b| valueRefsOuter(b.left, inner, outer) or valueRefsOuter(b.right, inner, outer),
        .unary_minus => |a| valueRefsOuter(a, inner, outer),
        .call => |c| blk: {
            for (c.args) |arg| {
                if (valueRefsOuter(arg, inner, outer)) break :blk true;
            }
            break :blk false;
        },
        .subquery => |q| isCorrelatedTo(q, inner, outer),
        .literal, .agg => false,
    };
}

fn exprRefsOuter(expr: *const BoolExpr, inner: []const []const u8, outer: []const []const u8) bool {
    return switch (expr.*) {
        .cmp => |c| valueRefsOuter(c.left, inner, outer) or valueRefsOuter(c.right, inner, outer),
        .isnull => |p| leftRefsOuter(p.left, inner, outer),
        .like => |p| leftRefsOuter(p.left, inner, outer),
        .in_list => |p| leftRefsOuter(p.left, inner, outer),
        .between => |p| leftRefsOuter(p.left, inner, outer),
        .in_query => |p| leftRefsOuter(p.left, inner, outer) or isCorrelatedTo(p.query, inner, outer),
        .cmp_query => |p| valueRefsOuter(p.left, inner, outer) or isCorrelatedTo(p.query, inner, outer),
        .exists => |p| isCorrelatedTo(p.query, inner, outer),
        .@"and", .@"or" => |b| exprRefsOuter(b.left, inner, outer) or exprRefsOuter(b.right, inner, outer),
    };
}

fn itemRefsOuter(item: SelectItem, inner: []const []const u8, outer: []const []const u8) bool {
    return switch (item) {
        .column => |c| if (c.qualifier) |q| qualRefersOuter(q, inner, outer) else false,
        .expr => |ex| valueRefsOuter(ex.expr, inner, outer),
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
    for (q.items) |item| {
        if (item == .expr and valueUsesTable(item.expr.expr, name)) return true;
    }
    if (q.union_right) |r| {
        if (usesTableName(r, name)) return true;
    }
    return false;
}

fn valueUsesTable(e: *const Expr, name: []const u8) bool {
    return switch (e.*) {
        .subquery => |q| usesTableName(q, name),
        .binary => |b| valueUsesTable(b.left, name) or valueUsesTable(b.right, name),
        .unary_minus => |a| valueUsesTable(a, name),
        .call => |c| blk: {
            for (c.args) |arg| {
                if (valueUsesTable(arg, name)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn exprUsesTable(expr: *const BoolExpr, name: []const u8) bool {
    return switch (expr.*) {
        .in_query => |p| usesTableName(p.query, name),
        .cmp_query => |p| usesTableName(p.query, name),
        .exists => |p| usesTableName(p.query, name),
        .cmp => |c| valueUsesTable(c.left, name) or valueUsesTable(c.right, name),
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
    try std.testing.expectEqualStrings("price", asColumn(where.cmp.left).?);
    try std.testing.expectEqual(@as(i64, 100), asLiteral(where.cmp.right).?.int);
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

    const uo = try parse(a, "SELECT 1 AS x UNION SELECT 2 UNION ALL SELECT 3 ORDER BY x LIMIT 2 OFFSET 1");
    try std.testing.expectEqual(@as(usize, 1), uo.order_by.len);
    try std.testing.expectEqualStrings("x", uo.order_by[0].column);
    try std.testing.expectEqual(@as(u64, 2), uo.limit.?);
    try std.testing.expectEqual(@as(u64, 1), uo.offset.?);
    try std.testing.expectEqual(@as(usize, 0), uo.union_right.?.order_by.len);
    try std.testing.expect(uo.union_right.?.limit == null);
    try std.testing.expectEqual(@as(usize, 0), uo.union_right.?.union_right.?.order_by.len);
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

    const scalar_sub = try parse(a, "SELECT (SELECT 1) FROM sales");
    try std.testing.expect(scalar_sub.items[0] == .expr);
    try std.testing.expect(scalar_sub.items[0].expr.expr.* == .subquery);

    const exists_q = try parse(a, "SELECT * FROM sales WHERE EXISTS (SELECT 1)");
    try std.testing.expect(exists_q.where.?.* == .exists);
    try std.testing.expect(!exists_q.where.?.exists.negated);

    const not_exists_q = try parse(a, "SELECT * FROM sales WHERE NOT EXISTS (SELECT 1)");
    try std.testing.expect(not_exists_q.where.?.* == .exists);
    try std.testing.expect(not_exists_q.where.?.exists.negated);

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
        try std.testing.expect(s.copy.from_table == null);
    }
    {
        const s = try parseStmt(a, "COPY lake.sales.orders FROM '/tmp/sales.parquet'");
        try std.testing.expectEqualStrings("lake.sales.orders", s.copy.from_table.?);
        try std.testing.expectEqualStrings("/tmp/sales.parquet", s.copy.path);
        try std.testing.expect(s.copy.query == null);
    }
    {
        const s = try parseStmt(a, "COPY t FROM 's3://bucket/data.parquet'");
        try std.testing.expectEqualStrings("t", s.copy.from_table.?);
        try std.testing.expectEqualStrings("s3://bucket/data.parquet", s.copy.path);
    }
    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "COPY FROM '/tmp/sales.parquet'"));
}

test "parse SHOW CATALOGS and SHOW TABLES" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const catalogs = try parseStmt(a, "SHOW CATALOGS");
    try std.testing.expect(catalogs == .show);
    try std.testing.expect(catalogs.show == .catalogs);

    const tables = try parseStmt(a, "show tables");
    try std.testing.expect(tables.show == .tables);
    try std.testing.expect(tables.show.tables.catalog == null);

    const from_cat = try parseStmt(a, "SHOW TABLES FROM files");
    try std.testing.expectEqualStrings("files", from_cat.show.tables.catalog.?);
    try std.testing.expect(from_cat.show.tables.namespace == null);

    const from_ns = try parseStmt(a, "SHOW TABLES FROM lake.default");
    try std.testing.expectEqualStrings("lake", from_ns.show.tables.catalog.?);
    try std.testing.expectEqualStrings("default", from_ns.show.tables.namespace.?);

    const nss = try parseStmt(a, "SHOW NAMESPACES FROM rest");
    try std.testing.expect(nss.show == .namespaces);
    try std.testing.expectEqualStrings("rest", nss.show.namespaces.catalog.?);

    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "SHOW VIEWS"));
    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "SHOW CATALOGS extra"));
}

test "parse FROM catalog.namespace.table" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const q = try parse(a, "SELECT * FROM lake.default.prune");
    try std.testing.expectEqualStrings("lake.default.prune", q.from);
    const j = try parse(a, "SELECT * FROM a.ns.t x JOIN b.ns.u y ON x.id = y.id");
    try std.testing.expectEqualStrings("a.ns.t", j.from);
    try std.testing.expectEqualStrings("b.ns.u", j.join.?.table);
}

test "parse ATTACH catalog as name" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const simple = try parseStmt(a, "ATTACH 'other.parquet' AS extra");
    try std.testing.expectEqualStrings("other.parquet", simple.attach.source);
    try std.testing.expectEqualStrings("extra", simple.attach.name);

    const with_kw = try parseStmt(a, "ATTACH CATALOG '/tmp/x.parquet' AS lake");
    try std.testing.expectEqualStrings("/tmp/x.parquet", with_kw.attach.source);
    try std.testing.expectEqualStrings("lake", with_kw.attach.name);

    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "ATTACH 'x.parquet'"));
}

test "parse DESCRIBE USE DETACH" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const d1 = try parseStmt(a, "DESCRIBE extra.attached");
    try std.testing.expectEqualStrings("extra.attached", d1.describe.ident);

    const d2 = try parseStmt(a, "DESCRIBE TABLE lake.default.prune");
    try std.testing.expectEqualStrings("lake.default.prune", d2.describe.ident);

    const use_one = try parseStmt(a, "USE extra");
    try std.testing.expectEqualStrings("extra", use_one.use.catalog);
    try std.testing.expect(use_one.use.namespace == null);

    const use_two = try parseStmt(a, "USE CATALOG lake.default");
    try std.testing.expectEqualStrings("lake", use_two.use.catalog);
    try std.testing.expectEqualStrings("default", use_two.use.namespace.?);

    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "USE lake.default.prune"));

    const det = try parseStmt(a, "DETACH CATALOG extra");
    try std.testing.expectEqualStrings("extra", det.detach.name);

    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "DETACH"));
}

test "parse CREATE INSERT DROP" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ns = try parseStmt(a, "CREATE NAMESPACE lake.sales");
    try std.testing.expectEqualStrings("lake.sales", ns.create_namespace.ident);

    const schema = try parseStmt(a, "CREATE SCHEMA sales");
    try std.testing.expectEqualStrings("sales", schema.create_namespace.ident);

    const ct = try parseStmt(a, "CREATE TABLE lake.sales.orders (id BIGINT, category STRING)");
    try std.testing.expectEqualStrings("lake.sales.orders", ct.create_table.ident);
    try std.testing.expectEqual(@as(usize, 2), ct.create_table.columns.len);
    try std.testing.expectEqualStrings("id", ct.create_table.columns[0].name);
    try std.testing.expectEqualStrings("long", ct.create_table.columns[0].type_name);
    try std.testing.expectEqualStrings("string", ct.create_table.columns[1].type_name);
    try std.testing.expect(ct.create_table.as_select == null);
    try std.testing.expectEqual(@as(usize, 0), ct.create_table.partition_by.len);

    const part = try parseStmt(a, "CREATE TABLE lake.sales.orders (id BIGINT, category STRING) PARTITIONED BY (category)");
    try std.testing.expectEqual(@as(usize, 1), part.create_table.partition_by.len);
    try std.testing.expectEqualStrings("category", part.create_table.partition_by[0]);

    const ident_tf = try parseStmt(a, "CREATE TABLE t (id BIGINT, category STRING) PARTITION BY (identity(category))");
    try std.testing.expectEqualStrings("category", ident_tf.create_table.partition_by[0]);

    const ctas_part = try parseStmt(a, "CREATE TABLE lake.sales.cats PARTITIONED BY (category) AS SELECT category, COUNT(*) AS n FROM orders GROUP BY category");
    try std.testing.expectEqualStrings("category", ctas_part.create_table.partition_by[0]);
    try std.testing.expect(ctas_part.create_table.as_select != null);

    try std.testing.expectError(error.UnsupportedPartitionSpec, parseStmt(a, "CREATE TABLE t (id BIGINT) PARTITIONED BY (bucket(4, id))"));
    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "CREATE TABLE t (id BIGINT) PARTITIONED BY ()"));
    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "CREATE TABLE t (id BIGINT, category STRING) PARTITIONED BY (category, category)"));

    const ctas = try parseStmt(a, "CREATE TABLE lake.sales.cats AS SELECT category, COUNT(*) AS n FROM orders GROUP BY category");
    try std.testing.expectEqualStrings("lake.sales.cats", ctas.create_table.ident);
    try std.testing.expect(ctas.create_table.as_select != null);
    try std.testing.expectEqualStrings("orders", ctas.create_table.as_select.?.from);

    const ins_v = try parseStmt(a, "INSERT INTO lake.sales.orders VALUES (1, 'fruit'), (2, 'veg')");
    try std.testing.expectEqualStrings("lake.sales.orders", ins_v.insert.ident);
    try std.testing.expectEqualStrings("", ins_v.insert.query.from);

    const ins_s = try parseStmt(a, "INSERT INTO dest SELECT id FROM src");
    try std.testing.expectEqualStrings("dest", ins_s.insert.ident);
    try std.testing.expectEqualStrings("src", ins_s.insert.query.from);

    const drop = try parseStmt(a, "DROP TABLE lake.sales.orders");
    try std.testing.expectEqualStrings("lake.sales.orders", drop.drop_table.ident);

    const del = try parseStmt(a, "DELETE FROM lake.sales.orders WHERE category = 'fruit'");
    try std.testing.expectEqualStrings("lake.sales.orders", del.delete.ident);
    try std.testing.expect(del.delete.where != null);

    const del_all = try parseStmt(a, "DELETE FROM t");
    try std.testing.expectEqualStrings("t", del_all.delete.ident);
    try std.testing.expect(del_all.delete.where == null);

    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "DELETE lake.sales.orders WHERE id = 1"));
    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "CREATE VIEW t AS SELECT 1"));
    try std.testing.expectError(error.InvalidSyntax, parseStmt(a, "INSERT dest VALUES (1)"));

    const upd = try parseStmt(a, "UPDATE lake.sales.orders SET category = 'x' WHERE id = 1");
    try std.testing.expectEqualStrings("lake.sales.orders", upd.update.ident);
    try std.testing.expectEqual(@as(usize, 1), upd.update.assignments.len);
    try std.testing.expectEqualStrings("category", upd.update.assignments[0].column);
    try std.testing.expect(upd.update.where != null);

    const mer = try parseStmt(a, "MERGE INTO dest d USING src s ON d.id = s.id WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT");
    try std.testing.expectEqualStrings("dest", mer.merge.ident);
    try std.testing.expectEqualStrings("d", mer.merge.alias.?);
    try std.testing.expectEqualStrings("src", mer.merge.source);
    try std.testing.expectEqualStrings("s", mer.merge.source_alias.?);
    try std.testing.expect(mer.merge.matched.? == .delete);
    try std.testing.expect(mer.merge.not_matched != null);

    const mer_up = try parseStmt(a, "MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET category = u.category");
    try std.testing.expect(mer_up.merge.matched.? == .update);

    const alter = try parseStmt(a, "ALTER TABLE lake.sales.orders ADD COLUMN note STRING");
    try std.testing.expectEqualStrings("lake.sales.orders", alter.alter_table.ident);
    try std.testing.expectEqualStrings("note", alter.alter_table.column.name);
    try std.testing.expectEqualStrings("string", alter.alter_table.column.type_name);
}

test "parse FOR SNAPSHOT and FOR TIMESTAMP AS OF" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const snap = try parse(a, "SELECT * FROM prune FOR SNAPSHOT 1");
    try std.testing.expectEqualStrings("prune", snap.from);
    try std.testing.expectEqual(@as(i64, 1), snap.as_of.?.snapshot);

    const snap_as = try parse(a, "SELECT COUNT(*) FROM lake.sales.prune FOR SNAPSHOT AS OF 2");
    try std.testing.expectEqualStrings("lake.sales.prune", snap_as.from);
    try std.testing.expectEqual(@as(i64, 2), snap_as.as_of.?.snapshot);

    const ts = try parse(a, "SELECT * FROM prune FOR TIMESTAMP AS OF 1700000000000");
    try std.testing.expectEqual(@as(i64, 1700000000000), ts.as_of.?.timestamp_ms);

    const ts_str = try parse(a, "SELECT * FROM prune FOR TIMESTAMP AS OF '1700000001000'");
    try std.testing.expectEqual(@as(i64, 1700000001000), ts_str.as_of.?.timestamp_ms);

    try std.testing.expectError(error.InvalidSyntax, parse(a, "SELECT * FROM (SELECT 1) FOR SNAPSHOT 1"));
}

test "parse arithmetic and col vs col" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const q = try parse(a, "SELECT price * 1.1 AS p FROM sales WHERE price > qty");
    try std.testing.expect(q.items[0] == .expr);
    try std.testing.expectEqualStrings("p", q.items[0].expr.alias.?);
    try std.testing.expect(q.items[0].expr.expr.* == .binary);
    try std.testing.expectEqual(ArithOp.mul, q.items[0].expr.expr.binary.op);
    const where = q.where orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("price", asColumn(where.cmp.left).?);
    try std.testing.expectEqualStrings("qty", asColumn(where.cmp.right).?);

    const nested = try parse(a, "SELECT abs(price * 2) FROM sales");
    try std.testing.expect(nested.items[0] == .expr);
    try std.testing.expect(nested.items[0].expr.expr.* == .call);

    const no_from = try parse(a, "SELECT 1 + 2");
    try std.testing.expect(no_from.isNoFrom());
    try std.testing.expect(!no_from.isLiteralOnly());
}

test "parse lower substr concat date_trunc count distinct values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const low = try parse(a, "SELECT lower(category) FROM sales");
    try std.testing.expect(low.items[0] == .expr);
    try std.testing.expectEqual(ScalarKind.lower, low.items[0].expr.expr.call.kind);

    const distinct = try parse(a, "SELECT COUNT(DISTINCT category) FROM sales");
    try std.testing.expect(distinct.items[0] == .agg);
    try std.testing.expect(distinct.items[0].agg.distinct);
    try std.testing.expectEqualStrings("category", distinct.items[0].agg.arg.?);

    const sub = try parse(a, "SELECT substr(category, 1, 2) FROM sales");
    try std.testing.expectEqual(ScalarKind.substr, sub.items[0].expr.expr.call.kind);

    const vals = try parse(a, "SELECT * FROM (VALUES (1), (2))");
    try std.testing.expect(vals.from_sub != null);
    try std.testing.expectEqualStrings("", vals.from_sub.?.from);
    try std.testing.expect(vals.from_sub.?.union_all);
    try std.testing.expectEqual(@as(i64, 1), vals.from_sub.?.items[0].literal.value.int);
    try std.testing.expectEqual(@as(i64, 2), vals.from_sub.?.union_right.?.items[0].literal.value.int);

    const up = try parse(a, "SELECT upper(category), length(category), trim(category), replace(category, 'a', 'b') FROM sales");
    try std.testing.expectEqual(ScalarKind.upper, up.items[0].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.length, up.items[1].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.trim, up.items[2].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.replace, up.items[3].expr.expr.call.kind);

    const ymd = try parse(a, "SELECT year(id), month(id), day(id), EXTRACT(YEAR FROM id) FROM sales");
    try std.testing.expectEqual(ScalarKind.year, ymd.items[0].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.month, ymd.items[1].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.day, ymd.items[2].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.extract, ymd.items[3].expr.expr.call.kind);

    const extra = try parse(a, "SELECT left(category, 1), right(category, 2), starts_with(category, 'f'), ceil(price), greatest(price, 0), hour(id) FROM sales");
    try std.testing.expectEqual(ScalarKind.left, extra.items[0].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.right, extra.items[1].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.starts_with, extra.items[2].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.ceil, extra.items[3].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.greatest, extra.items[4].expr.expr.call.kind);
    try std.testing.expectEqual(ScalarKind.hour, extra.items[5].expr.expr.call.kind);
}
