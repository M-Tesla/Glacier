const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const glacier_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    addCarquet(b, glacier_mod);
    addNanoarrow(b, glacier_mod);
    addAvro(b, glacier_mod);

    const lib_only = b.option(bool, "lib_only", "Install only libglacier (Python wheel)") orelse false;

    const lib = b.addLibrary(.{
        .name = "glacier",
        .linkage = .dynamic,
        .root_module = glacier_mod,
    });
    lib.installHeader(b.path("include/glacier.h"), "glacier.h");
    lib.setVersionScript(b.path("include/glacier.map"));
    if (optimize != .Debug) lib.root_module.strip = true;
    b.installArtifact(lib);

    const flight_mod = b.createModule(.{
        .root_source_file = b.path("src/table/flight_server.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "glacier", .module = glacier_mod },
        },
    });

    const repl = b.addExecutable(.{
        .name = "glacier",
        .root_module = b.createModule(.{
            .root_source_file = b.path("glacier_repl.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "glacier", .module = glacier_mod },
                .{ .name = "flight_server", .module = flight_mod },
            },
        }),
    });
    if (!lib_only) b.installArtifact(repl);

    const with_ui = b.option(bool, "ui", "Build glacier-ui (dvui + SDL3 Linux prototype)") orelse false;
    if (with_ui and !lib_only and target.result.cpu.arch != .wasm32) {
        if (b.lazyDependency("dvui", .{
            .target = target,
            .optimize = optimize,
            .backend = .sdl3,
            .@"tree-sitter" = false,
        })) |dvui_dep| {
            const ui = b.addExecutable(.{
                .name = "glacier-ui",
                .root_module = b.createModule(.{
                    .root_source_file = b.path("glacier_ui.zig"),
                    .target = target,
                    .optimize = optimize,
                    .link_libc = true,
                    .imports = &.{
                        .{ .name = "glacier", .module = glacier_mod },
                        .{ .name = "dvui", .module = dvui_dep.module("dvui_sdl3") },
                    },
                }),
            });
            if (optimize != .Debug) ui.root_module.strip = true;
            b.installArtifact(ui);
            const run_ui = b.addRunArtifact(ui);
            if (b.args) |args| run_ui.addArgs(args);
            b.step("run-ui", "Run glacier-ui (needs -Dui)").dependOn(&run_ui.step);
        }
    }

    const info = b.addExecutable(.{
        .name = "parquet-info",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/parquet_info.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "glacier", .module = glacier_mod },
            },
        }),
    });
    if (!lib_only) b.installArtifact(info);

    const gen = b.addExecutable(.{
        .name = "gen-fixtures",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/gen_fixtures.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "glacier", .module = glacier_mod },
            },
        }),
    });
    const run_gen = b.addRunArtifact(gen);
    run_gen.setCwd(b.path("."));
    run_gen.addArg("tests/formats/sales.parquet");
    run_gen.addArg("tests/iceberg_prune");
    b.step("fixtures", "Write sales.parquet and tests/iceberg_prune").dependOn(&run_gen.step);

    const gen_volume = b.addExecutable(.{
        .name = "gen-volume",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/gen_volume.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "glacier", .module = glacier_mod },
            },
        }),
    });
    if (!lib_only) b.installArtifact(gen_volume);

    const run_info = b.addRunArtifact(info);
    if (b.args) |args| run_info.addArgs(args);
    const run_step = b.step("run", "Run parquet-info (pass -- path.parquet)");
    run_step.dependOn(&run_info.step);

    const lib_tests = b.addTest(.{
        .root_module = glacier_mod,
    });
    const run_tests = b.addRunArtifact(lib_tests);

    const cli_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("glacier_repl.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "glacier", .module = glacier_mod },
                .{ .name = "flight_server", .module = flight_mod },
            },
        }),
    });
    const run_cli_tests = b.addRunArtifact(cli_tests);

    const flight_tests = b.addTest(.{
        .root_module = flight_mod,
    });
    const run_flight_tests = b.addRunArtifact(flight_tests);

    const cli_smoke = b.addRunArtifact(repl);
    cli_smoke.addArgs(&.{ "-c", "SELECT 1" });
    cli_smoke.expectStdOutMatch("1");

    const cli_fail = b.addRunArtifact(repl);
    cli_fail.addArgs(&.{ "-c", "SELECT * FROM nope" });
    cli_fail.expectExitCode(1);
    cli_fail.expectStdOutMatch("table not found");

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
    test_step.dependOn(&run_cli_tests.step);
    test_step.dependOn(&run_flight_tests.step);
    test_step.dependOn(&cli_smoke.step);
    test_step.dependOn(&cli_fail.step);

    const smoke = b.addExecutable(.{
        .name = "glacier-c-smoke",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    smoke.root_module.addCSourceFile(.{
        .file = b.path("bindings/c/example.c"),
        .flags = &.{"-std=c11"},
        .language = .c,
    });
    smoke.root_module.addIncludePath(b.path("include"));
    smoke.root_module.linkLibrary(lib);
    if (!lib_only) b.installArtifact(smoke);

    const run_smoke = b.addRunArtifact(smoke);
    run_smoke.setCwd(b.path("."));
    run_smoke.addArg("tests/formats/sales.parquet");
    run_smoke.step.dependOn(&run_gen.step);
    b.step("example-c", "C ABI smoke: SELECT * on a tests/ parquet, print one Arrow int64 column").dependOn(&run_smoke.step);

    const example = b.addExecutable(.{
        .name = "glacier-c-example",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    example.root_module.addCSourceFile(.{
        .file = b.path("examples/c/example.c"),
        .flags = &.{"-std=c11"},
        .language = .c,
    });
    example.root_module.addIncludePath(b.path("include"));
    example.root_module.linkLibrary(lib);
    if (!lib_only) b.installArtifact(example);

    const wasm_step = b.step("wasm", "Build glacier.wasm (wasm32-wasi, ReleaseSmall)");
    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .wasi,
    });
    const wasm_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSmall,
        .link_libc = false,
        .single_threaded = true,
    });
    addWasiHeaders(b, wasm_mod);
    addCarquet(b, wasm_mod);
    addNanoarrow(b, wasm_mod);
    addAvro(b, wasm_mod);
    wasm_mod.addCSourceFile(.{
        .file = b.path("src/formats/wasm/minilibc.c"),
        .flags = &.{ "-std=c11", "-fno-builtin", "-fno-sanitize=undefined" },
        .language = .c,
    });
    const wasm_lib = b.addExecutable(.{
        .name = "glacier",
        .root_module = wasm_mod,
    });
    wasm_lib.entry = .disabled;
    wasm_lib.rdynamic = true;
    const wasm_install = b.addInstallArtifact(wasm_lib, .{});
    wasm_step.dependOn(&wasm_install.step);
}

fn addWasiHeaders(b: *std.Build, mod: *std.Build.Module) void {
    const zig_lib = b.graph.zig_lib_directory.path orelse return;
    const dirs = [_][]const u8{
        "include",
        "libc/include/wasm-wasi-musl",
        "libc/include/generic-musl",
        "libc/include/wasm32-wasi-any",
        "libc/include/any-wasi-any",
    };
    for (dirs) |rel| {
        const p = std.fs.path.join(b.allocator, &.{ zig_lib, rel }) catch @panic("oom");
        mod.addSystemIncludePath(.{ .cwd_relative = p });
    }
}

fn moduleIsWasm(mod: *std.Build.Module) bool {
    const resolved = mod.resolved_target orelse return false;
    return resolved.result.cpu.arch == .wasm32;
}

fn moduleCpuArch(mod: *std.Build.Module) std.Target.Cpu.Arch {
    const resolved = mod.resolved_target orelse return .x86_64;
    return resolved.result.cpu.arch;
}

fn moduleOsTag(mod: *std.Build.Module) std.Target.Os.Tag {
    const resolved = mod.resolved_target orelse return .linux;
    return resolved.result.os.tag;
}

fn addCarquet(b: *std.Build, mod: *std.Build.Module) void {
    const wasm = moduleIsWasm(mod);
    const carquet_root = b.path("vendor/carquet");
    if (wasm) {
        mod.addIncludePath(b.path("src/formats/wasm"));
    }
    mod.addIncludePath(carquet_root.path(b, "include"));
    mod.addIncludePath(carquet_root.path(b, "src"));
    mod.addIncludePath(b.path("vendor/lz4"));
    if (wasm) {
        mod.addIncludePath(b.path("vendor/zlib"));
    } else {
        const os = moduleOsTag(mod);
        mod.addIncludePath(b.path("vendor/zstd"));
        mod.addIncludePath(b.path("vendor/zstd/lib"));
        addZlib(b, mod);
        addZstd(b, mod);
        if (os != .windows) {
            mod.linkSystemLibrary("m", .{});
            mod.linkSystemLibrary("pthread", .{});
        }
    }

    const flags = [_][]const u8{
        "-std=c11",
        "-fno-sanitize=undefined",
        "-fvisibility=hidden",
        "-D_GNU_SOURCE",
        "-Wno-unused-parameter",
        "-Wno-unused-function",
    };

    if (wasm) {
        mod.addCSourceFiles(.{
            .root = carquet_root,
            .files = &carquet_sources_wasm,
            .flags = &flags,
        });
        mod.addCSourceFile(.{
            .file = carquet_root.path(b, "src/compression/snappy.c"),
            .flags = &flags,
            .language = .c,
        });
    } else {
        const arch = moduleCpuArch(mod);
        const x86 = arch == .x86_64 or arch == .x86;
        const arm = arch == .aarch64 or arch == .arm;
        const x86_flags = flags ++ [_][]const u8{"-DCARQUET_ARCH_X86"};
        const arm_flags = flags ++ [_][]const u8{ "-DCARQUET_ARCH_ARM", "-DCARQUET_ENABLE_NEON" };
        const native_flags: []const []const u8 = if (x86) &x86_flags else if (arm) &arm_flags else &flags;
        const snappy_x86 = x86_flags ++ [_][]const u8{"-mssse3"};
        const snappy_flags: []const []const u8 = if (x86) &snappy_x86 else native_flags;
        mod.addCSourceFiles(.{
            .root = carquet_root,
            .files = &carquet_sources,
            .flags = native_flags,
        });
        mod.addCSourceFile(.{
            .file = carquet_root.path(b, "src/compression/snappy.c"),
            .flags = snappy_flags,
            .language = .c,
        });
    }
    mod.addCSourceFile(.{
        .file = b.path("vendor/lz4/lz4.c"),
        .flags = &flags,
        .language = .c,
    });
    if (wasm) {
        mod.addCSourceFile(.{
            .file = b.path("src/formats/wasm/zstd_stub.c"),
            .flags = &flags,
            .language = .c,
        });
        mod.addCSourceFile(.{
            .file = b.path("src/formats/wasm/worker_pool_serial.c"),
            .flags = &flags,
            .language = .c,
        });
        addZlib(b, mod);
    } else {
        mod.addCSourceFile(.{
            .file = carquet_root.path(b, "src/compression/zstd.c"),
            .flags = &flags,
            .language = .c,
        });
    }
    mod.addCSourceFile(.{
        .file = carquet_root.path(b, "src/compression/lz4.c"),
        .flags = &flags,
        .language = .c,
    });
    mod.addCSourceFile(.{
        .file = b.path("src/formats/parquet_bridge.c"),
        .flags = &flags,
        .language = .c,
    });
}

fn addZlib(b: *std.Build, mod: *std.Build.Module) void {
    const zflags = [_][]const u8{
        "-std=c11",
        "-fno-sanitize=undefined",
        "-fvisibility=hidden",
        "-D_GNU_SOURCE",
        "-Wno-unused-parameter",
        "-Wno-unused-function",
        "-DHAVE_UNISTD_H",
        "-DHAVE_STDARG_H",
    };
    mod.addIncludePath(b.path("vendor/zlib"));
    mod.addCSourceFiles(.{
        .root = b.path("vendor/zlib"),
        .files = &.{
            "adler32.c",
            "compress.c",
            "crc32.c",
            "deflate.c",
            "infback.c",
            "inffast.c",
            "inflate.c",
            "inftrees.c",
            "trees.c",
            "uncompr.c",
            "zutil.c",
        },
        .flags = &zflags,
    });
}

fn addZstd(b: *std.Build, mod: *std.Build.Module) void {
    const flags = [_][]const u8{
        "-std=c11",
        "-fno-sanitize=undefined",
        "-fvisibility=hidden",
        "-D_GNU_SOURCE",
        "-DZSTD_DISABLE_ASM=1",
        "-DZSTD_LEGACY_SUPPORT=0",
        "-Wno-unused-parameter",
        "-Wno-unused-function",
        "-Wno-unused-variable",
    };
    mod.addCSourceFiles(.{
        .root = b.path("vendor/zstd/lib"),
        .files = &.{
            "common/debug.c",
            "common/entropy_common.c",
            "common/error_private.c",
            "common/fse_decompress.c",
            "common/pool.c",
            "common/threading.c",
            "common/xxhash.c",
            "common/zstd_common.c",
            "compress/fse_compress.c",
            "compress/hist.c",
            "compress/huf_compress.c",
            "compress/zstd_compress.c",
            "compress/zstd_compress_literals.c",
            "compress/zstd_compress_sequences.c",
            "compress/zstd_compress_superblock.c",
            "compress/zstd_double_fast.c",
            "compress/zstd_fast.c",
            "compress/zstd_lazy.c",
            "compress/zstd_ldm.c",
            "compress/zstdmt_compress.c",
            "compress/zstd_opt.c",
            "compress/zstd_preSplit.c",
            "decompress/huf_decompress.c",
            "decompress/zstd_ddict.c",
            "decompress/zstd_decompress.c",
            "decompress/zstd_decompress_block.c",
        },
        .flags = &flags,
    });
}

fn addAvro(b: *std.Build, mod: *std.Build.Module) void {
    if (moduleIsWasm(mod)) {
        mod.addIncludePath(b.path("src/formats/wasm"));
        mod.addIncludePath(b.path("vendor/zlib"));
    }
    mod.addIncludePath(b.path("vendor/avro-c/src"));
    mod.addIncludePath(b.path("vendor/jansson/src"));
    const common = [_][]const u8{
        "-std=c11",
        "-fno-sanitize=undefined",
        "-fvisibility=hidden",
        "-D_GNU_SOURCE",
        "-Wno-unused-parameter",
        "-Wno-unused-function",
        "-Wno-unused-variable",
        "-Wno-sign-compare",
        "-Wno-unused-but-set-variable",
    };
    const jansson_flags = common ++ [_][]const u8{"-DHAVE_CONFIG_H"};
    const avro_flags = common ++ [_][]const u8{ "-DDEFLATE_CODEC", "-DSNAPPY_CODEC" };
    mod.addIncludePath(b.path("src/formats"));
    mod.addCSourceFiles(.{
        .root = b.path("vendor/jansson/src"),
        .files = &.{
            "dump.c",
            "error.c",
            "hashtable.c",
            "hashtable_seed.c",
            "load.c",
            "memory.c",
            "pack_unpack.c",
            "strbuffer.c",
            "strconv.c",
            "utf.c",
            "value.c",
            "version.c",
        },
        .flags = &jansson_flags,
    });
    mod.addCSourceFiles(.{
        .root = b.path("vendor/avro-c/src"),
        .files = &.{
            "allocation.c",
            "array.c",
            "codec.c",
            "consumer.c",
            "consume-binary.c",
            "datafile.c",
            "datum.c",
            "datum_equal.c",
            "datum_read.c",
            "datum_size.c",
            "datum_skip.c",
            "datum_validate.c",
            "datum_value.c",
            "datum_write.c",
            "dump.c",
            "encoding_binary.c",
            "errors.c",
            "generic.c",
            "io.c",
            "map.c",
            "memoize.c",
            "resolved-reader.c",
            "resolved-writer.c",
            "resolver.c",
            "schema.c",
            "schema_equal.c",
            "st.c",
            "string.c",
            "value.c",
            "value-hash.c",
            "value-json.c",
            "value-read.c",
            "value-sizeof.c",
            "value-write.c",
            "wrapped-buffer.c",
        },
        .flags = &avro_flags,
    });
    mod.addCSourceFile(.{
        .file = b.path("src/formats/avro_bridge.c"),
        .flags = &avro_flags,
        .language = .c,
    });
    mod.addCSourceFile(.{
        .file = b.path("src/formats/snappy_c_shim.c"),
        .flags = &avro_flags,
        .language = .c,
    });
}

fn addNanoarrow(b: *std.Build, mod: *std.Build.Module) void {
    mod.addIncludePath(b.path("vendor/nanoarrow"));
    const flags = [_][]const u8{
        "-std=c11",
        "-fno-sanitize=undefined",
        "-fvisibility=hidden",
        "-Wno-unused-parameter",
        "-Wno-unused-function",
    };
    mod.addCSourceFile(.{
        .file = b.path("vendor/nanoarrow/nanoarrow.c"),
        .flags = &flags,
        .language = .c,
    });
    mod.addCSourceFile(.{
        .file = b.path("src/formats/arrow_bridge.c"),
        .flags = &flags,
        .language = .c,
    });
}

const carquet_sources = [_][]const u8{
    "src/core/arena.c",
    "src/core/allocator.c",
    "src/core/buffer.c",
    "src/core/bitpack.c",
    "src/core/endian.c",
    "src/core/error.c",
    "src/core/geo_wkb.c",
    "src/thrift/thrift_decode.c",
    "src/thrift/thrift_encode.c",
    "src/thrift/parquet_types.c",
    "src/encoding/plain.c",
    "src/encoding/rle.c",
    "src/encoding/delta.c",
    "src/encoding/delta_length.c",
    "src/encoding/delta_strings.c",
    "src/encoding/dictionary.c",
    "src/encoding/byte_stream_split.c",
    "src/compression/gzip.c",
    "src/compression/custom.c",
    "src/simd/detect.c",
    "src/simd/dispatch.c",
    "src/reader/file_reader.c",
    "src/reader/arrow_schema_read.c",
    "src/reader/arrow_c_export.c",
    "src/reader/arrow_c_read.c",
    "src/reader/row_group_reader.c",
    "src/reader/column_reader.c",
    "src/reader/page_reader.c",
    "src/reader/batch_reader.c",
    "src/reader/page_filter.c",
    "src/reader/statistics.c",
    "src/reader/mmap_reader.c",
    "src/reader/worker_pool.c",
    "src/writer/file_writer.c",
    "src/writer/row_group_writer.c",
    "src/writer/column_writer.c",
    "src/writer/page_writer.c",
    "src/writer/arrow_schema.c",
    "src/writer/arrow_c_import.c",
    "src/metadata/schema.c",
    "src/metadata/bloom_filter.c",
    "src/metadata/page_index.c",
    "src/util/crc32.c",
    "src/util/xxhash.c",
};

const carquet_sources_wasm = [_][]const u8{
    "src/core/arena.c",
    "src/core/allocator.c",
    "src/core/buffer.c",
    "src/core/bitpack.c",
    "src/core/endian.c",
    "src/core/error.c",
    "src/core/geo_wkb.c",
    "src/thrift/thrift_decode.c",
    "src/thrift/thrift_encode.c",
    "src/thrift/parquet_types.c",
    "src/encoding/plain.c",
    "src/encoding/rle.c",
    "src/encoding/delta.c",
    "src/encoding/delta_length.c",
    "src/encoding/delta_strings.c",
    "src/encoding/dictionary.c",
    "src/encoding/byte_stream_split.c",
    "src/compression/gzip.c",
    "src/compression/custom.c",
    "src/simd/detect.c",
    "src/simd/dispatch.c",
    "src/reader/file_reader.c",
    "src/reader/arrow_schema_read.c",
    "src/reader/arrow_c_export.c",
    "src/reader/arrow_c_read.c",
    "src/reader/row_group_reader.c",
    "src/reader/column_reader.c",
    "src/reader/page_reader.c",
    "src/reader/batch_reader.c",
    "src/reader/page_filter.c",
    "src/reader/statistics.c",
    "src/reader/mmap_reader.c",
    "src/writer/file_writer.c",
    "src/writer/row_group_writer.c",
    "src/writer/column_writer.c",
    "src/writer/page_writer.c",
    "src/writer/arrow_schema.c",
    "src/writer/arrow_c_import.c",
    "src/metadata/schema.c",
    "src/metadata/bloom_filter.c",
    "src/metadata/page_index.c",
    "src/util/crc32.c",
    "src/util/xxhash.c",
};
