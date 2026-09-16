//! Glacier 0.2 — Zig query engine. Format codecs are C (carquet, libavro).

pub const FileSource = @import("vfs/source.zig").FileSource;
pub const Transport = @import("vfs/source.zig").Transport;
pub const cache = @import("vfs/cache.zig");
pub const spill = @import("vfs/spill.zig");
pub const aws = @import("kernel/aws.zig");
pub const parquet = @import("formats/parquet_wrap.zig");
pub const avro = @import("formats/avro_wrap.zig");
pub const arrow = @import("formats/arrow_wrap.zig");
pub const native = @import("formats/native.zig");
pub const sql = @import("sql/parser.zig");
pub const batch = @import("execution/batch.zig");
pub const physical = @import("execution/physical.zig");
pub const session = @import("session.zig");
pub const iceberg = @import("table/iceberg.zig");
pub const rest_catalog = @import("table/rest_catalog.zig");
pub const err = @import("error.zig");
pub const c_api = @import("c_api.zig");

pub const Session = session.Session;
pub const Result = session.Result;
pub const Batch = batch.Batch;
pub const Column = batch.Column;
pub const GlacierError = err.GlacierError;

pub const version = "0.2.1";

comptime {
    _ = &c_api.glacier_open;
    _ = &c_api.glacier_open_buffer;
    _ = &c_api.glacier_malloc;
    _ = &c_api.glacier_malloc_free;
    _ = &c_api.glacier_close;
    _ = &c_api.glacier_connect;
    _ = &c_api.glacier_disconnect;
    _ = &c_api.glacier_query;
    _ = &c_api.glacier_result_destroy;
    _ = &c_api.glacier_result_error;
    _ = &c_api.glacier_result_arrow;
    _ = &c_api.glacier_free;
    _ = &c_api.glacier_version;
    _ = &c_api.glacier_api_version;
}

test {
    _ = FileSource;
    _ = aws;
    _ = parquet;
    _ = avro;
    _ = arrow;
    _ = sql;
    _ = session;
    _ = iceberg;
    _ = rest_catalog;
    _ = err;
    _ = c_api;
    _ = cache;
    _ = spill;
    _ = native;
}
