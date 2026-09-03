//! Structured engine error. Session/C ABI print `message`, not `@errorName`.

const std = @import("std");

pub const Code = enum {
    out_of_memory,
    io,
    invalid_syntax,
    unexpected_eof,
    invalid_number,
    unsupported_sql,
    unsupported_join,
    table_not_found,
    column_not_found,
    type_mismatch,
    schema_mismatch,
    unsupported_type,
    parquet,
    avro,
    iceberg,
};

pub const GlacierError = struct {
    code: Code,
    message: []const u8,

    pub fn fromZig(err: anyerror) GlacierError {
        return .{ .code = codeOf(err), .message = staticMessage(err) };
    }
};

pub fn codeOf(err: anyerror) Code {
    return switch (err) {
        error.OutOfMemory => .out_of_memory,
        error.InvalidSyntax => .invalid_syntax,
        error.UnexpectedEof => .unexpected_eof,
        error.InvalidNumber => .invalid_number,
        error.UnsupportedSql => .unsupported_sql,
        error.UnsupportedJoin => .unsupported_join,
        error.TableNotFound => .table_not_found,
        error.ColumnNotFound => .column_not_found,
        error.AmbiguousColumn => .column_not_found,
        error.TypeMismatch => .type_mismatch,
        error.SubqueryCardinality => .invalid_syntax,
        error.SchemaMismatch => .schema_mismatch,
        error.UnsupportedType, error.UnsupportedNested => .unsupported_type,
        error.UnsupportedDeletes, error.UnsupportedSchemaEvolution, error.UnsupportedPartitionSpec => .iceberg,
        error.ParquetOpenFailed, error.ParquetWriteFailed, error.CarquetInitFailed => .parquet,
        error.AvroOpenFailed, error.AvroWriteFailed => .avro,
        error.InvalidNativeFile => .io,
        error.InvalidMetadata, error.SnapshotNotFound, error.SchemaNotFound, error.ManifestsNeedAvro => .iceberg,
        error.AwsCredentialsMissing => .io,
        else => .io,
    };
}

pub fn staticMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.OutOfMemory => "out of memory",
        error.InvalidSyntax => "invalid SQL syntax",
        error.UnexpectedEof => "unexpected end of SQL",
        error.InvalidNumber => "invalid number",
        error.UnsupportedSql => "this SQL feature is not supported",
        error.UnsupportedJoin => "JOIN is not supported",
        error.TableNotFound => "table not found",
        error.ColumnNotFound => "column not found",
        error.AmbiguousColumn => "column name is ambiguous",
        error.TypeMismatch => "type mismatch",
        error.SubqueryCardinality => "subquery returned more than one row",
        error.SchemaMismatch => "schema mismatch across files",
        error.UnsupportedType => "unsupported column type",
        error.UnsupportedNested => "nested Parquet types are not supported",
        error.UnsupportedDeletes => "Iceberg equality/position deletes are not supported",
        error.UnsupportedSchemaEvolution => "Iceberg schema evolution is not supported",
        error.UnsupportedPartitionSpec => "Iceberg partition transform is not supported",
        error.ParquetOpenFailed => "failed to open parquet file",
        error.ParquetWriteFailed => "failed to write parquet file",
        error.InvalidNativeFile => "invalid .glacier file",
        error.CarquetInitFailed => "failed to initialize parquet reader",
        error.AvroOpenFailed => "failed to open avro file",
        error.AvroWriteFailed => "failed to write avro file",
        error.InvalidMetadata => "invalid Iceberg metadata.json",
        error.SnapshotNotFound => "Iceberg snapshot not found",
        error.SchemaNotFound => "Iceberg schema not found",
        error.ManifestsNeedAvro => "Iceberg manifests are missing or not valid Avro",
        error.AwsCredentialsMissing => "AWS credentials not found",
        error.HttpRedirectLocationOversize => "HTTP redirect URL is too long",
        error.HttpRangeUnsupported => "server does not support HTTP Range",
        error.HttpRequestFailed => "HTTP request failed",
        error.TooManyHttpRedirects => "too many HTTP redirects",
        error.FileNotFound => "file not found",
        error.AccessDenied, error.PermissionDenied => "permission denied",
        error.IsDir => "path is a directory",
        error.NotDir => "path is not a directory",
        error.UnexpectedEndOfFile => "unexpected end of file",
        else => "I/O error",
    };
}

test "JOIN maps to a message, not the error name" {
    const ge = GlacierError.fromZig(error.UnsupportedJoin);
    try std.testing.expectEqual(Code.unsupported_join, ge.code);
    try std.testing.expectEqualStrings("JOIN is not supported", ge.message);
    try std.testing.expect(std.mem.indexOf(u8, ge.message, "UnsupportedJoin") == null);
}

test "Iceberg unsupported features map to messages" {
    try std.testing.expectEqualStrings(
        "Iceberg equality/position deletes are not supported",
        GlacierError.fromZig(error.UnsupportedDeletes).message,
    );
    try std.testing.expectEqualStrings(
        "Iceberg schema evolution is not supported",
        GlacierError.fromZig(error.UnsupportedSchemaEvolution).message,
    );
    try std.testing.expectEqualStrings(
        "Iceberg partition transform is not supported",
        GlacierError.fromZig(error.UnsupportedPartitionSpec).message,
    );
    try std.testing.expectEqualStrings(
        "nested Parquet types are not supported",
        GlacierError.fromZig(error.UnsupportedNested).message,
    );
}
