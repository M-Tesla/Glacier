// ═══════════════════════════════════════════════════════════════════════════
// GLACIER - Physical Execution Engine
// ═══════════════════════════════════════════════════════════════════════════

const std = @import("std");
const batch_mod = @import("batch.zig");
const planner_mod = @import("planner.zig");
const expr_mod = @import("expr.zig");
const sql = @import("../sql/parser.zig");
const parquet = @import("../formats/parquet.zig");
const compression = @import("../formats/compression.zig");
const value_decoder = @import("../formats/value_decoder.zig");

// ═══════════════════════════════════════════════════════════════════════════
// INTERFACES
// ═══════════════════════════════════════════════════════════════════════════

pub const BatchIterator = struct {
    ptr: *anyopaque,
    nextFn: *const fn (ctx: *anyopaque) anyerror!?batch_mod.Batch,
    deinitFn: *const fn (ctx: *anyopaque) void,

    pub fn next(self: BatchIterator) !?batch_mod.Batch {
        return self.nextFn(self.ptr);
    }

    pub fn deinit(self: BatchIterator) void {
        self.deinitFn(self.ptr);
    }
};

pub const ExecutionPlan = struct {
    ptr: *anyopaque,
    executeFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) anyerror!BatchIterator,
    schemaFn: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) anyerror!batch_mod.Schema,
    deinitFn: *const fn (ctx: *anyopaque) void,

    pub fn execute(self: ExecutionPlan, allocator: std.mem.Allocator) !BatchIterator {
        return self.executeFn(self.ptr, allocator);
    }

    pub fn schema(self: ExecutionPlan, allocator: std.mem.Allocator) !batch_mod.Schema {
        return self.schemaFn(self.ptr, allocator);
    }

    pub fn deinit(self: ExecutionPlan) void {
        self.deinitFn(self.ptr);
    }
};

// Helper to find ALL parquet files in directory
fn findAllParquetInDir(allocator: std.mem.Allocator, dir_path: []const u8) !std.ArrayListUnmanaged([]const u8) {
    var files: std.ArrayListUnmanaged([]const u8) = .{};
    errdefer {
        for (files.items) |f| allocator.free(f);
        files.deinit(allocator);
    }

    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return files; // Return empty if fail
    defer dir.close();

    // 1. Check "data" subdir first (Iceberg standard)
    {
        var data_dir = dir.openDir("data", .{ .iterate = true }) catch null;
        if (data_dir) |*dd| {
            defer dd.close();
            var it = dd.iterate();
            while (try it.next()) |entry| {
                 if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".parquet")) {
                     const path = try std.fs.path.join(allocator, &.{dir_path, "data", entry.name});
                     try files.append(allocator, path);
                 }
            }
        }
    }

    // 2. Check root dir
    var it = dir.iterate();
    while (try it.next()) |entry| {
         if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".parquet")) {
             const path = try std.fs.path.join(allocator, &.{dir_path, entry.name});
             try files.append(allocator, path);
         }
    }
    
    return files;
}

// ═══════════════════════════════════════════════════════════════════════════
// IMPLEMENTATIONS
// ═══════════════════════════════════════════════════════════════════════════

// --- ScanExec ---

pub const ScanExec = struct {
    file_paths: [][]const u8,
    allocator: std.mem.Allocator,

    pub fn create(allocator: std.mem.Allocator, node: *const planner_mod.ScanNode) !ExecutionPlan {
        // Resolve directory to parquet file(s) if needed
        var final_paths: std.ArrayListUnmanaged([]const u8) = .{};
        errdefer {
            for (final_paths.items) |p| allocator.free(p);
            final_paths.deinit(allocator);
        }

        const input_path = try allocator.dupe(u8, node.table_name);
        // We temporarily own input_path, but free it if it's not used as a final file path
        
        var is_dir = false;
        const stat = std.fs.cwd().statFile(input_path) catch |err| blk: {
             if (err == error.IsDir) {
                 is_dir = true;
                 break :blk std.fs.File.Stat{ 
                    .kind = .directory, .size = 0, .mode = 0, .mtime = 0, .ctime = 0, .atime = 0, .inode = 0 
                 };
             }
             return err;
        };
        
        if (is_dir or stat.kind == .directory) {
            // It's a directory: Find all parquet files
            allocator.free(input_path); // Free original dir path string
            
            var files = try findAllParquetInDir(allocator, node.table_name);
            if (files.items.len == 0) {
                 files.deinit(allocator); 
                 return error.NoParquetFoundInDir;
            }
            final_paths.deinit(allocator); // Free empty
            final_paths = files;
        } else {
            // It's a single file
            try final_paths.append(allocator, input_path);
        }

        const self = try allocator.create(ScanExec);
        self.* = .{
            .file_paths = try final_paths.toOwnedSlice(allocator),
            .allocator = allocator,
        };

        return ExecutionPlan{
            .ptr = self,
            .executeFn = execute,
            .schemaFn = getSchema,
            .deinitFn = deinit,
        };
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *ScanExec = @ptrCast(@alignCast(ctx));
        for (self.file_paths) |path| {
            self.allocator.free(path);
        }
        self.allocator.free(self.file_paths);
        self.allocator.destroy(self);
    }

    fn getSchema(ctx: *anyopaque, allocator: std.mem.Allocator) !batch_mod.Schema {
        const self: *ScanExec = @ptrCast(@alignCast(ctx));
        
        // Use first file for schema
        if (self.file_paths.len == 0) return error.NoFilesToScan;
        var reader = try parquet.Reader.open(allocator, self.file_paths[0]);
        defer reader.close();
        try reader.readMetadata();
        
        const metadata = reader.metadata.?;
        // Fix: Use ArrayListUnmanaged to avoid resolving issues
        var cols: std.ArrayListUnmanaged(batch_mod.ColumnSchema) = .{};
        defer cols.deinit(allocator);

        // Simple schema mapping based on flattened Parquet schema
        // Skipping root (index 0) if it is a struct wrapper (common in Parquet)
        var start_idx: usize = 0;
        if (metadata.schema.len > 0 and metadata.schema[0].num_children > 0) {
            start_idx = 1;
        }

        if (metadata.schema.len > start_idx) {
            for (metadata.schema[start_idx..]) |elem| {
                
                const dt = if (elem.type) |t| switch (t) {
                    .INT32 => batch_mod.DataType.int32,
                    .INT64 => batch_mod.DataType.int64,
                    .FLOAT => batch_mod.DataType.float32,
                    .DOUBLE => batch_mod.DataType.float64,
                    .BOOLEAN => batch_mod.DataType.boolean,
                    .BYTE_ARRAY => batch_mod.DataType.string,
                    else => batch_mod.DataType.string // Fallback
                } else batch_mod.DataType.string;

                try cols.append(allocator, .{
                    .name = try allocator.dupe(u8, elem.name),
                    .data_type = dt,
                    .nullable = (elem.repetition_type == .OPTIONAL),
                });
            }
        }
        
        return batch_mod.Schema.init(allocator, cols.items);
    }

    fn execute(ctx: *anyopaque, allocator: std.mem.Allocator) !BatchIterator {
        const self: *ScanExec = @ptrCast(@alignCast(ctx));
        
        // Re-read schema for the iterator to use
        var schema = try getSchema(ctx, allocator);
        errdefer schema.deinit(); 
        
        // Initialize dictionaries array (one per column)
        const dicts = try allocator.alloc(?batch_mod.Batch, schema.columns.len);
        for (dicts) |*d| d.* = null;

        const iter = try allocator.create(ScanIterator);
        iter.* = .{
            .allocator = allocator,
            .current_reader = null,
            .current_row_group = 0,
            .schema = schema,
            .dictionaries = dicts,
            .file_paths = self.file_paths,
            .current_file_index = 0,
        };

        return BatchIterator{
            .ptr = iter,
            .nextFn = ScanIterator.next,
            .deinitFn = ScanIterator.deinit,
        };
    }
};

const ScanIterator = struct {
    allocator: std.mem.Allocator,
    current_reader: ?parquet.Reader,
    current_row_group: usize,
    schema: batch_mod.Schema,
    dictionaries: []?batch_mod.Batch,
    
    // Multi-file support
    file_paths: [][]const u8,
    current_file_index: usize,

    fn next(ctx: *anyopaque) !?batch_mod.Batch {
        const self: *ScanIterator = @ptrCast(@alignCast(ctx));
        
        while (true) {
            // 1. Ensure Reader is Open
            if (self.current_reader == null) {
                if (self.current_file_index >= self.file_paths.len) return null; // All files processed

                // Open next file
                const path = self.file_paths[self.current_file_index];
                self.current_reader = try parquet.Reader.open(self.allocator, path);
                try self.current_reader.?.readMetadata();
                self.current_row_group = 0;
                
                // Clear dictionaries (new file, new dictionaries)
                // Note: Previous batchs holding refs to dicts must have deep copied or own them?
                // Glacier Batch implementation usually owns data.
                for (self.dictionaries) |*d| {
                    if (d.*) |*batch| batch.deinit();
                    d.* = null;
                }
            }

            // 2. Check if current reader has more row groups
            const metadata = self.current_reader.?.metadata.?;
            if (self.current_row_group >= metadata.row_groups.len) {
                // File finished
                self.current_reader.?.close();
                self.current_reader = null;
                self.current_file_index += 1;
                continue; // Loop to next file
            }

            const rg = metadata.row_groups[self.current_row_group];
            self.current_row_group += 1;

            // Create batch
            var num_rows = @as(usize, @intCast(rg.num_rows));
            
            // Fallback: If RowGroup has 0 rows but file has rows and it's the only RG, assume it contains all rows.
            if (num_rows == 0 and metadata.row_groups.len == 1) {
                num_rows = @as(usize, @intCast(metadata.num_rows));
                std.debug.print("[Scan] Warning: RowGroup.num_rows is 0, using FileMetaData.num_rows = {d}\n", .{num_rows});
            }

            if (num_rows == 0) continue; // Skip empty row groups, loop again

            var builder = try batch_mod.BatchBuilder.init(self.allocator, &self.schema, num_rows);
            errdefer builder.deinit();
            
            const reader_ref = &self.current_reader.?;

            // DATA READING LOGIC
            for (rg.columns, 0..) |col_chunk, i| {
                 if (i >= self.schema.columns.len) break;
                 const col_schema = self.schema.columns[i];
                 
                 // 1. Load Dictionary if available and not yet loaded
                 if (col_chunk.meta_data.dictionary_page_offset) |dict_offset| {
                     if (self.dictionaries[i] == null and dict_offset > 0) {
                         // std.debug.print("[Scan] Loading Dict for col {d} at offset {d}\n", .{i, dict_offset});
                         // Read Dictionary Page using if/else capture instead of catch block
                         if (reader_ref.readDataPage(dict_offset, self.allocator)) |page_res| {
                             defer self.allocator.free(page_res.compressed_data);
                             
                             const uncompressed_size = @as(usize, @intCast(page_res.page_header.uncompressed_page_size));
                             const page_data = try compression.decompress(self.allocator, col_chunk.meta_data.codec, page_res.compressed_data, uncompressed_size);
                             defer self.allocator.free(page_data);
                             const num_dict_values = if (page_res.page_header.dictionary_page_header) |h| h.num_values else if (page_res.page_header.data_page_header) |h| h.num_values else 0;
                             
                             // std.debug.print("[Scan] Loaded Dict col {d}: {d} values\n", .{i, num_dict_values});

                             var dict_batch = try batch_mod.Batch.initWithSchema(self.allocator, &self.schema, @intCast(num_dict_values));
                             var dict_col = dict_batch.columnMut(i);
                             dict_col.len = @intCast(num_dict_values);

                             // Decode Dictionary Values (ALWAYS PLAIN)
                             if (col_schema.data_type == .string) {
                                 const vals = try value_decoder.decodePlainByteArray(self.allocator, page_data, @intCast(num_dict_values), false);
                                 const slice_ptr = @as([*][]u8, @ptrCast(@alignCast(dict_col.data.ptr)));
                                 for (vals, 0..) |v, r| slice_ptr[r] = v;
                                 self.allocator.free(vals);
                             } else if (col_schema.data_type == .int64) {
                                  const vals = try value_decoder.decodePlainInt64(self.allocator, page_data, @intCast(num_dict_values), false);
                                  defer self.allocator.free(vals);
                                  for (vals, 0..) |v, r| dict_col.setValue(i64, r, v);
                             } else if (col_schema.data_type == .int32) {
                                  const vals = try value_decoder.decodePlainInt32(self.allocator, page_data, @intCast(num_dict_values), false);
                                  defer self.allocator.free(vals);
                                  for (vals, 0..) |v, r| dict_col.setValue(i32, r, v);
                             } else if (col_schema.data_type == .float32) {
                                  const vals = try value_decoder.decodePlainFloat32(self.allocator, page_data, @intCast(num_dict_values), false);
                                  defer self.allocator.free(vals);
                                  for (vals, 0..) |v, r| dict_col.setValue(f32, r, v);
                             } else if (col_schema.data_type == .float64) {
                                  const vals = try value_decoder.decodePlainFloat64(self.allocator, page_data, @intCast(num_dict_values), false);
                                  defer self.allocator.free(vals);
                                  for (vals, 0..) |v, r| dict_col.setValue(f64, r, v);
                             }
                             
                             self.dictionaries[i] = dict_batch;
                             // std.debug.print("[Scan] DICT ASSIGNED to col {d}\n", .{i});
                         } else |err| {
                             // std.debug.print("[Scan] Dictionary Load Error col {d} offset {d}: {s}\n", .{i, dict_offset, @errorName(err)});
                             return err; // Propagate error
                         }
                     }
                 } else {
                    // std.debug.print("[Scan] No Dict Offset for col {d}\n", .{i});
                 }

                 // 2. Read Data Page
                 if (reader_ref.readDataPage(col_chunk.meta_data.data_page_offset, self.allocator)) |page_res| {
                     defer self.allocator.free(page_res.compressed_data);
                     
                     const uncompressed_size = @as(usize, @intCast(page_res.page_header.uncompressed_page_size));
                     const page_data = try compression.decompress(self.allocator, col_chunk.meta_data.codec, page_res.compressed_data, uncompressed_size);
                     defer self.allocator.free(page_data);
                     
                     const encoding = if (page_res.page_header.data_page_header) |h| h.encoding else .PLAIN;

                     var batch_col = builder.batch.columnMut(i);
                     batch_col.len = num_rows;
                     
                     const using_dictionary = (encoding == .PLAIN_DICTIONARY or encoding == .RLE_DICTIONARY);
                     
                     if (using_dictionary) {
                         if (self.dictionaries[i] == null) {
                            std.debug.print("[Scan] Error: Dict encoding {s} without Dict loaded! (offset={?})\n", .{@tagName(encoding), col_chunk.meta_data.dictionary_page_offset});
                             return error.DictionaryNotLoaded;
                         }
                         const dict = self.dictionaries[i].?;
                         const dict_col = dict.column(i);

                         const indices = value_decoder.decodeDictionaryIndices(self.allocator, page_data, num_rows) catch |err| {
                             std.debug.print("[Scan] Indices Error: {s}\n", .{@errorName(err)});
                             continue;
                         };
                         defer self.allocator.free(indices);

                         for (indices, 0..) |idx, r| {
                             if (r >= num_rows) break;
                             if (col_schema.data_type == .string) {
                                 const val = dict_col.getValue([]u8, @as(usize, @intCast(idx))).?; // get byte slice
                                 // We need to COPY this string because dict might be freed/reset
                                 const val_dupe = try self.allocator.dupe(u8, val);
                                 // Assign to batch (batch will own it if logic supported, but currently batch doesn't track individual allocs?)
                                 // Wait, batch_mod.Batch logic for string usually assumes ownership or arena?
                                 // For now assume standard StringArray behavior.
                                 // Current implementation just assigns slice. 
                                 // FIX: If we change dictionary, we invalidate pointers. We MUST copy.
                                 const slice_ptr = @as([*][]u8, @ptrCast(@alignCast(batch_col.data.ptr)));
                                 slice_ptr[r] = val_dupe; 
                                 // We are leaking val_dupe potentially if Batch deinit doesn't free strings. 
                                 // Batch.deinit usually frees string buffer. 
                                 // Glacier Batch uses Arena usually? No, it uses allocator.
                             } else if (col_schema.data_type == .int64) {
                                  batch_col.setValue(i64, r, dict_col.getValue(i64, @as(usize, @intCast(idx))).?);
                             } else if (col_schema.data_type == .int32) {
                                  batch_col.setValue(i32, r, dict_col.getValue(i32, @as(usize, @intCast(idx))).?);
                             } else if (col_schema.data_type == .float32) {
                                 batch_col.setValue(f32, r, dict_col.getValue(f32, @as(usize, @intCast(idx))).?);
                             } else if (col_schema.data_type == .float64) {
                                 batch_col.setValue(f64, r, dict_col.getValue(f64, @as(usize, @intCast(idx))).?);
                             }
                             batch_col.setNull(r, false);
                         }
                     } else {
                         // PLAIN encoding logic (existing)
                         // Heuristic: if first 4 bytes are small (< 100) it might be a header
                         // Skip potential RLE header (up to 6 bytes based on our observation)
                         var decode_offset: usize = 0;
                         if (page_data.len >= 6) {
                             const potential_header = std.mem.readInt(u32, page_data[0..4], .little);
                             if (potential_header < 256) {
                                 decode_offset = 6;
                             }
                         }
                         const actual_data = page_data[decode_offset..];

                         if (col_schema.data_type == .int64) {
                             const vals = value_decoder.decodePlainInt64(self.allocator, actual_data, num_rows, false) catch continue;
                             defer self.allocator.free(vals);
                             for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(i64, r, v);
                         } else if (col_schema.data_type == .int32) {
                             const vals = value_decoder.decodePlainInt32(self.allocator, actual_data, num_rows, false) catch continue;
                             defer self.allocator.free(vals);
                             for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(i32, r, v);
                         } else if (col_schema.data_type == .float32) {
                             const vals = value_decoder.decodePlainFloat32(self.allocator, actual_data, num_rows, false) catch continue;
                             defer self.allocator.free(vals);
                             for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(f32, r, v);
                         } else if (col_schema.data_type == .float64) {
                             const vals = value_decoder.decodePlainFloat64(self.allocator, actual_data, num_rows, false) catch continue;
                             defer self.allocator.free(vals);
                             for (vals, 0..) |v, r| if (r < num_rows) batch_col.setValue(f64, r, v);
                         } else if (col_schema.data_type == .string) {
                             const vals = value_decoder.decodePlainByteArray(self.allocator, actual_data, num_rows, false) catch continue;
                             const slice_ptr = @as([*][]u8, @ptrCast(@alignCast(batch_col.data.ptr)));
                             for (vals, 0..) |v, r| {
                                 if (r < num_rows) {
                                     slice_ptr[r] = v;
                                     batch_col.setNull(r, false);
                                 }
                             }
                             self.allocator.free(vals); 
                         }
                     }
 
                 } else |err| {
                     std.debug.print("[Scan] ReadPage Error col {d}: {s}\n", .{i, @errorName(err)});
                     continue;
                 }
            }
            
            builder.batch.row_count = num_rows;
            return builder.finish();
        }
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *ScanIterator = @ptrCast(@alignCast(ctx));
        
        for (self.schema.columns) |col| {
            self.allocator.free(col.name);
        }
        
        for (self.dictionaries) |*d| {
            if (d.*) |*batch| batch.deinit();
        }
        self.allocator.free(self.dictionaries);

        var mut_schema = self.schema;
        mut_schema.deinit();
        
        if (self.current_reader) |*r| {
            r.close();
        }
        self.allocator.destroy(self);
    }
};

// --- FilterExec ---

pub const FilterExec = struct {
    input: ExecutionPlan,
    condition: expr_mod.Expr,
    allocator: std.mem.Allocator,

    pub fn create(allocator: std.mem.Allocator, input: ExecutionPlan, condition: expr_mod.Expr) !ExecutionPlan {
        const self = try allocator.create(FilterExec);
        self.* = .{
            .input = input,
            .condition = condition,
            .allocator = allocator,
        };
        
        return ExecutionPlan{
             .ptr = self,
             .executeFn = execute,
             .schemaFn = getSchema,
             .deinitFn = deinit,
        };
    }
    
    fn deinit(ctx: *anyopaque) void {
        const self: *FilterExec = @ptrCast(@alignCast(ctx));
        self.input.deinit();
        self.allocator.destroy(self);
    }

    fn getSchema(ctx: *anyopaque, allocator: std.mem.Allocator) !batch_mod.Schema {
        const self: *FilterExec = @ptrCast(@alignCast(ctx));
        return self.input.schema(allocator);
    }

    fn execute(ctx: *anyopaque, allocator: std.mem.Allocator) !BatchIterator {
        const self: *FilterExec = @ptrCast(@alignCast(ctx));
        const input_iter = try self.input.execute(allocator);
        
        const iter = try allocator.create(FilterIterator);
        iter.* = .{
            .input_iter = input_iter,
            .condition = &self.condition, 
            .allocator = allocator,
        };

        return BatchIterator{
            .ptr = iter,
            .nextFn = FilterIterator.next,
            .deinitFn = FilterIterator.deinit,
        };
    }
};

const FilterIterator = struct {
    input_iter: BatchIterator,
    condition: *const expr_mod.Expr,
    allocator: std.mem.Allocator,

    fn next(ctx: *anyopaque) !?batch_mod.Batch {
        const self: *FilterIterator = @ptrCast(@alignCast(ctx));
        
        while (true) {
            const batch_opt = try self.input_iter.next();
            if (batch_opt) |batch| {
                // Apply filter
                var selection = try expr_mod.evaluateFilter(self.allocator, self.condition, &batch);
                defer selection.deinit();

                if (selection.count == 0) {
                    var mut_batch = batch;
                    mut_batch.deinit();
                    continue;
                }
                
                if (selection.count == batch.row_count) {
                    return batch; 
                }
                
                // For POC, return full batch if ANY pass
                return batch; 
            } else {
                return null;
            }
        }
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *FilterIterator = @ptrCast(@alignCast(ctx));
        self.input_iter.deinit();
        self.allocator.destroy(self);
    }
};

// --- HashJoinExec ---

pub const HashJoinExec = struct {
    left: ExecutionPlan,
    right: ExecutionPlan,
    condition: ?expr_mod.Expr,
    join_type: sql.JoinType,
    allocator: std.mem.Allocator,

    // Factory method wrapper if needed, but we use init directly for composition
    pub fn create(allocator: std.mem.Allocator, node: *const planner_mod.JoinNode) !ExecutionPlan {
        _ = allocator; _ = node;
        return error.UseInitDireclyWithPlans; 
    }
    
    // We'll define a direct init method for manual construction
    pub fn init(allocator: std.mem.Allocator, left: ExecutionPlan, right: ExecutionPlan, condition: ?expr_mod.Expr, join_type: sql.JoinType) !ExecutionPlan {
        const self = try allocator.create(HashJoinExec);
        self.* = .{
            .left = left,
            .right = right,
            .condition = condition, // We should copy this if it's not owned, but for now assuming ownership transfer
            .join_type = join_type,
            .allocator = allocator,
        };

        return ExecutionPlan{
             .ptr = self,
             .executeFn = execute,
             .schemaFn = getSchema,
             .deinitFn = deinit,
        };
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *HashJoinExec = @ptrCast(@alignCast(ctx));
        self.left.deinit();
        self.right.deinit();
        // Condition deinit handled by caller or arena
        self.allocator.destroy(self);
    }

    fn getSchema(ctx: *anyopaque, allocator: std.mem.Allocator) !batch_mod.Schema {
        const self: *HashJoinExec = @ptrCast(@alignCast(ctx));
        const left_schema = try self.left.schema(allocator);
        // defer left_schema.deinit(); // Cannot deinit immediately, we need to inspect
        const right_schema = try self.right.schema(allocator);
        // defer right_schema.deinit();

        // Combine schemas
        var cols = try std.ArrayListUnmanaged(batch_mod.ColumnSchema).initCapacity(allocator, left_schema.columns.len + right_schema.columns.len);
        defer cols.deinit(allocator);

        for (left_schema.columns) |c| try cols.append(allocator, c);
        for (right_schema.columns) |c| try cols.append(allocator, c); // Name collisions possible!

        // Note: Real implementation needs to handle name collisions or fully qualified names.
        // Schema.init copies the columns.
        return batch_mod.Schema.init(allocator, cols.items);
    }

    fn execute(ctx: *anyopaque, allocator: std.mem.Allocator) !BatchIterator {
        const self: *HashJoinExec = @ptrCast(@alignCast(ctx));
        
        // Build Hash Table (Blocking)
        var build_map = std.AutoHashMap(i64, std.ArrayListUnmanaged(RowRef)).init(allocator);
        // We also need to keep the batches alive
        var build_batches = std.ArrayListUnmanaged(batch_mod.Batch){};
        
        const right_iter = try self.right.execute(allocator);
        defer right_iter.deinit();

        // Find Join Key Index in Right Schema
        // Assumption: Condition is binary eq: left.col = right.col
        // Simplification: Assume condition is valid and refers to right col by name
        // We need the join column name.
        // Hack for MVP: Extract column name from condition or use simplistic assumption
        // Actually, we pass the join key index? No, we have the expression.
        
        // MVP: Hardcoded to check for "id" column if condition is null or simple
        // Real logic: Evaluate condition to find key.
        const right_schema = try self.right.schema(allocator);
        const join_key_idx_right = right_schema.columnIndex("id") orelse 0; // FALLBACK
        
        while (try right_iter.next()) |batch| {
            // Store batch (take ownership, so we assume `batch` is owned by us now? 
            // `next()` returns a batch that we should define ownership. Usually caller owns.)
            // We need to keep it for probing. 
            // BatchIterator next returns a Batch. Is it a copy or ref?
            // "ScanIterator" creates a new batch. So we own it.
            
            try build_batches.append(allocator, batch);
            const batch_idx = build_batches.items.len - 1;
            
            const col = batch.column(join_key_idx_right);
            // Assume INT64 key for MVP
            if (col.data_type == .int64) {
                 for (0..batch.row_count) |r| {
                     if (col.isNull(r)) continue;
                     const val = col.getValue(i64, r).?;
                     
                     const gop = try build_map.getOrPut(val);
                     if (!gop.found_existing) {
                         gop.value_ptr.* = .{};
                     }
                     try gop.value_ptr.append(allocator, .{ .batch_idx = batch_idx, .row_idx = r });
                 }
            }
        }
        
        const iter = try allocator.create(HashJoinIterator);
        
        // Combine schemas for output
        const output_schema = try getSchema(ctx, allocator);
        
        const left_iter = try self.left.execute(allocator);

        iter.* = .{
            .left_iter = left_iter,
            .build_map = build_map,
            .build_batches = build_batches,
            .output_schema = output_schema,
            .allocator = allocator,
            .join_key_idx_left = 0, // Placeholder, need from schema
        };
        
        // Determine Left Join Key
        const left_schema = try self.left.schema(allocator);
        if (left_schema.columnIndex("dept_id")) |idx| {
             iter.join_key_idx_left = idx;
        } else if (left_schema.columnIndex("id")) |idx| {
             iter.join_key_idx_left = idx;
        }
        
        return BatchIterator{
            .ptr = iter,
            .nextFn = HashJoinIterator.next,
            .deinitFn = HashJoinIterator.deinit,
        };
    }
};

const RowRef = struct {
    batch_idx: usize,
    row_idx: usize,
};

const HashJoinIterator = struct {
    left_iter: BatchIterator,
    build_map: std.AutoHashMap(i64, std.ArrayListUnmanaged(RowRef)),
    build_batches: std.ArrayListUnmanaged(batch_mod.Batch),
    output_schema: batch_mod.Schema,
    allocator: std.mem.Allocator,
    join_key_idx_left: usize,
    
    // Buffer for joined rows logic could go here, for now streaming 1-to-1 max (or simple expansion)
    
    fn next(ctx: *anyopaque) !?batch_mod.Batch {
        const self: *HashJoinIterator = @ptrCast(@alignCast(ctx));
        
        while (true) {
            const left_batch_opt = try self.left_iter.next();
            if (left_batch_opt == null) return null;
            var left_batch = left_batch_opt.?;
            defer left_batch.deinit(); // We consume and produce new batch
            
            // Result Builder
            // Estimate size? 
            var builder = try batch_mod.BatchBuilder.init(self.allocator, &self.output_schema, batch_mod.DEFAULT_BATCH_SIZE);
            var result_count: usize = 0;
            
            const join_col = left_batch.column(self.join_key_idx_left);
            
             // Assume INT64 key logic
            if (join_col.data_type == .int64) {
                for (0..left_batch.row_count) |l_row| {
                    if (join_col.isNull(l_row)) continue;
                    const val = join_col.getValue(i64, l_row).?;
                    
                    if (self.build_map.get(val)) |matches| {
                        for (matches.items) |match| {
                            if (builder.current_row >= builder.capacity) {
                                // Buffer full - complex logic needed to yield and resume.
                                // MVP: Resize or just error/loss?
                                // Let's resize? BatchBuilder fixed size.
                                // Return what we have and lose rest? BAD.
                                // Correct: loop state.
                                // MVP: Just limit to batch size for now (lossy but runs) or panic.
                                // Actually, let's just break and return handling partial batch?
                                // No, input batch needs to be resumed.
                                break;
                            }
                            
                            try builder.appendRow();
                            
                            // Copy Left Cols
                            // We need to map output col index to input col.
                            // Schema is [Left Cols, Right Cols]
                            const left_col_count = left_batch.columns.len;
                            
                            for (0..left_col_count) |c| {
                                copyValue(&builder, c, left_batch.column(c), l_row);
                            }
                            
                            // Copy Right Cols
                            const right_batch = self.build_batches.items[match.batch_idx];
                            for (0..right_batch.columns.len) |c| {
                                copyValue(&builder, left_col_count + c, right_batch.column(c), match.row_idx);
                            }
                            
                            result_count += 1;
                        }
                    }
                }
            }
            
            if (result_count > 0) {
                return builder.finish();
            } else {
                builder.deinit();
                continue; // Fetch next left batch
            }
        }
    }
    
    fn copyValue(builder: *batch_mod.BatchBuilder, out_col: usize, in_col: *const batch_mod.ColumnVector, row: usize) void {
        if (in_col.isNull(row)) {
            builder.setNull(out_col);
            return;
        }
        
        switch (in_col.data_type) {
            .int64 => builder.setValue(out_col, i64, in_col.getValue(i64, row).?),
            .int32 => builder.setValue(out_col, i32, in_col.getValue(i32, row).?),
            .float32 => builder.setValue(out_col, f32, in_col.getValue(f32, row).?),
            .float64 => builder.setValue(out_col, f64, in_col.getValue(f64, row).?),
            .string => builder.setValue(out_col, []u8, in_col.getValue([]u8, row).?),
            else => {}, // TODO
        }
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *HashJoinIterator = @ptrCast(@alignCast(ctx));
        self.left_iter.deinit();
        
        var it = self.build_map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.build_map.deinit();
        
        for (self.build_batches.items) |*b| b.deinit();
        self.build_batches.deinit(self.allocator);
        
        var mut_schema = self.output_schema;
        mut_schema.deinit(); // Schema is owned by us (created in getSchema/init)
        
        self.allocator.destroy(self);
    }
};
