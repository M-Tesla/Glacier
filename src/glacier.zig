// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Pure Zig OLAP Query Engine for Apache Iceberg
// ═══════════════════════════════════════════════════════════════════════════
//
// Architecture: 4-Layer Standalone System
//
// Layer 1: Kernel Interface  → Networking, Async I/O, Threading
// Layer 2: File Formats       → Parquet Decoder, Compression Codecs
// Layer 3: Table Formats      → Iceberg Spec V2, Avro/JSON Parsing
// Layer 4: Execution Engine   → Expression Eval, Filtering, Aggregation
//
// Principles:
//   - Zero External Dependencies (no libc, no openssl)
//   - Arena Allocation Strategy (no malloc/free in hot path)
//   - Vectorized Processing (SIMD-optimized columnar operations)
//   - Comptime Monomorphization (zero virtual calls)
//
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════
// LAYER 1: KERNEL INTERFACE
// ═══════════════════════════════════════════════════════════════════════════

/// Pure Zig HTTP/HTTPS client with AWS S3 support
pub const http = @import("kernel/http.zig");

/// TLS 1.2/1.3 implementation (Pure Zig)
pub const tls = @import("kernel/tls.zig");

/// AWS Signature V4 authentication
pub const aws = @import("kernel/aws.zig");

/// Zero-copy buffer management (RingBuffer, etc)
pub const buffers = @import("kernel/buffers.zig");

/// Async I/O primitives (epoll/io_uring abstraction)
pub const io = @import("kernel/io.zig");

// ═══════════════════════════════════════════════════════════════════════════
// LAYER 2: FILE FORMATS
// ═══════════════════════════════════════════════════════════════════════════

/// Apache Parquet file format reader
pub const parquet = @import("formats/parquet.zig");

/// SNAPPY decompression
pub const snappy = @import("formats/snappy.zig");

/// Compression codecs (Snappy, Zstd)
pub const compression = @import("formats/compression.zig");

/// Encoding schemes (RLE, Bit-Packing, Delta)
pub const encoding = @import("formats/encoding.zig");

/// Column reader with Dremel algorithm (Definition/Repetition Levels)
pub const column_reader = @import("formats/column_reader.zig");

/// Value decoders for all Parquet types and encodings
pub const value_decoder = @import("formats/value_decoder.zig");

/// Thrift Compact Protocol parser
pub const thrift = @import("formats/thrift.zig");

// ═══════════════════════════════════════════════════════════════════════════
// LAYER 3: TABLE FORMATS
// ═══════════════════════════════════════════════════════════════════════════

/// Apache Iceberg table format (V2 spec)
pub const iceberg = @import("table/iceberg.zig");

/// Apache Avro binary format parser (complete implementation from GlacierAvro)
pub const avro = @import("table/avro.zig");

/// Apache Avro reader for data files
pub const avro_reader = @import("table/avro_reader.zig");

// ═══════════════════════════════════════════════════════════════════════════
// LAYER 4: EXECUTION ENGINE
// ═══════════════════════════════════════════════════════════════════════════

/// Vectorized batch processing
pub const batch = @import("execution/batch.zig");

/// Expression evaluator (comptime-specialized)
pub const expr = @import("execution/expr.zig");

/// Projection operator (column selection)
pub const projection = @import("execution/projection.zig");

/// Query execution planner
pub const planner = @import("execution/planner.zig");

/// Physical execution engine
pub const physical = @import("execution/physical.zig");

// ═══════════════════════════════════════════════════════════════════════════
// SQL PARSER
// ═══════════════════════════════════════════════════════════════════════════

/// SQL query parser
pub const sql = @import("sql/parser.zig");

// ═══════════════════════════════════════════════════════════════════════════
// CORE UTILITIES
// ═══════════════════════════════════════════════════════════════════════════

/// Memory management utilities
pub const memory = @import("core/memory.zig");

/// Error types used throughout the engine
pub const errors = @import("core/errors.zig");

// ═══════════════════════════════════════════════════════════════════════════
// VERSION INFORMATION
// ═══════════════════════════════════════════════════════════════════════════

pub const version = "0.8.1-alpha";
pub const version_major = 0;
pub const version_minor = 8;
pub const version_patch = 1;

// ═══════════════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════════════

test {
    // Import all submodules to run their tests
    std.testing.refAllDecls(@This());
}
