/**
 * Glacier wasm32-wasi wrapper. Same glacier.h exports.
 * Browser: instantiate with the bundled WASI preview1 stub (no filesystem).
 * Data in: Uint8Array via glacier_open_buffer (copied into linear memory).
 */
export function wasiPreview1(memoryRef) {
  const clock = () => BigInt(Date.now()) * 1000000n;
  const encoder = new TextEncoder();
  const u8 = () => new Uint8Array(memoryRef.memory.buffer);
  const u32 = (ptr) => new DataView(memoryRef.memory.buffer).getUint32(ptr, true);
  const setU32 = (ptr, v) => new DataView(memoryRef.memory.buffer).setUint32(ptr, v, true);
  const setU64 = (ptr, v) => new DataView(memoryRef.memory.buffer).setBigUint64(ptr, v, true);

  return {
    args_get: () => 0,
    args_sizes_get: (argc, buf_sz) => {
      setU32(argc, 0);
      setU32(buf_sz, 0);
      return 0;
    },
    environ_get: () => 0,
    environ_sizes_get: (count, buf_sz) => {
      setU32(count, 0);
      setU32(buf_sz, 0);
      return 0;
    },
    clock_res_get: (id, out) => {
      setU64(out, 1000000n);
      return 0;
    },
    clock_time_get: (id, precision, out) => {
      setU64(out, clock());
      return 0;
    },
    fd_close: () => 0,
    fd_fdstat_get: () => 8, // EBADF — no preopens
    fd_fdstat_set_flags: () => 8,
    fd_prestat_get: () => 8,
    fd_prestat_dir_name: () => 8,
    fd_read: () => 8,
    fd_seek: () => 8,
    fd_write: (fd, iovs, iovs_len, nwritten) => {
      let n = 0;
      const view = new DataView(memoryRef.memory.buffer);
      const bytes = u8();
      for (let i = 0; i < iovs_len; i++) {
        const ptr = view.getUint32(iovs + i * 8, true);
        const len = view.getUint32(iovs + i * 8 + 4, true);
        const chunk = bytes.subarray(ptr, ptr + len);
        const text = new TextDecoder().decode(chunk);
        if (fd === 2) console.error(text);
        else if (fd === 1) console.log(text);
        n += len;
      }
      setU32(nwritten, n);
      return 0;
    },
    poll_oneoff: () => 52, // ENOSYS
    proc_exit: (code) => {
      throw new Error(`WASI proc_exit ${code}`);
    },
    random_get: (ptr, len) => {
      const buf = u8().subarray(ptr, ptr + len);
      if (globalThis.crypto && crypto.getRandomValues) crypto.getRandomValues(buf);
      else for (let i = 0; i < len; i++) buf[i] = Math.floor(Math.random() * 256);
      return 0;
    },
    sched_yield: () => 0,
    fd_advise: () => 0,
    fd_allocate: () => 8,
    fd_datasync: () => 0,
    fd_filestat_get: () => 8,
    fd_filestat_set_size: () => 8,
    fd_filestat_set_times: () => 8,
    fd_pread: () => 8,
    fd_pwrite: () => 8,
    fd_readdir: () => 8,
    fd_renumber: () => 8,
    fd_sync: () => 0,
    fd_tell: () => 8,
    path_create_directory: () => 8,
    path_filestat_get: () => 8,
    path_filestat_set_times: () => 8,
    path_link: () => 8,
    path_open: () => 8,
    path_readlink: () => 8,
    path_remove_directory: () => 8,
    path_rename: () => 8,
    path_symlink: () => 8,
    path_unlink_file: () => 8,
    sock_accept: () => 52,
    sock_recv: () => 52,
    sock_send: () => 52,
    sock_shutdown: () => 52,
    "thread-spawn": () => 52,
  };
}

export class Glacier {
  constructor(instance) {
    this.e = instance.exports;
    this.memory = this.e.memory;
  }

  static async instantiate(wasmBytes) {
    const memoryRef = { memory: null };
    const imports = { wasi_snapshot_preview1: wasiPreview1(memoryRef) };
    const { instance } = await WebAssembly.instantiate(wasmBytes, imports);
    memoryRef.memory = instance.exports.memory;
    return new Glacier(instance);
  }

  version() {
    return this.readCString(this.e.glacier_version());
  }

  malloc(n) {
    const p = this.e.glacier_malloc(n);
    if (!p) throw new Error("out of memory");
    return p;
  }

  writeBytes(bytes) {
    const p = this.malloc(bytes.length);
    new Uint8Array(this.memory.buffer, p, bytes.length).set(bytes);
    return p;
  }

  writeCString(s) {
    const encoded = new TextEncoder().encode(s);
    const p = this.malloc(encoded.length + 1);
    const buf = new Uint8Array(this.memory.buffer, p, encoded.length + 1);
    buf.set(encoded);
    buf[encoded.length] = 0;
    return { ptr: p, n: encoded.length + 1 };
  }

  readCString(ptr) {
    if (!ptr) return null;
    const mem = new Uint8Array(this.memory.buffer);
    let end = ptr;
    while (mem[end] !== 0) end++;
    return new TextDecoder().decode(mem.subarray(ptr, end));
  }

  openBuffer(uint8) {
    const p = this.writeBytes(uint8);
    const errSlot = this.malloc(4);
    new DataView(this.memory.buffer).setUint32(errSlot, 0, true);
    const db = this.e.glacier_open_buffer(p, uint8.length, errSlot);
    this.e.glacier_malloc_free(p, uint8.length);
    if (!db) {
      const errPtr = new DataView(this.memory.buffer).getUint32(errSlot, true);
      const msg = this.readCString(errPtr) || "open buffer failed";
      if (errPtr) this.e.glacier_free(errPtr);
      this.e.glacier_malloc_free(errSlot, 4);
      throw new Error(msg);
    }
    this.e.glacier_malloc_free(errSlot, 4);
    return db;
  }

  openEmpty() {
    const errSlot = this.malloc(4);
    new DataView(this.memory.buffer).setUint32(errSlot, 0, true);
    const db = this.e.glacier_open(0, errSlot);
    if (!db) {
      const errPtr = new DataView(this.memory.buffer).getUint32(errSlot, true);
      const msg = this.readCString(errPtr) || "open failed";
      if (errPtr) this.e.glacier_free(errPtr);
      this.e.glacier_malloc_free(errSlot, 4);
      throw new Error(msg);
    }
    this.e.glacier_malloc_free(errSlot, 4);
    return db;
  }

  close(db) {
    this.e.glacier_close(db);
  }

  query(db, sql) {
    const { ptr, n } = this.writeCString(sql);
    const errSlot = this.malloc(4);
    new DataView(this.memory.buffer).setUint32(errSlot, 0, true);
    const result = this.e.glacier_query(db, ptr, errSlot);
    this.e.glacier_malloc_free(ptr, n);
    if (!result) {
      const errPtr = new DataView(this.memory.buffer).getUint32(errSlot, true);
      const msg = this.readCString(errPtr) || "query failed";
      if (errPtr) this.e.glacier_free(errPtr);
      this.e.glacier_malloc_free(errSlot, 4);
      throw new Error(msg);
    }
    this.e.glacier_malloc_free(errSlot, 4);
    const err = this.e.glacier_result_error(result);
    if (err) {
      const msg = this.readCString(err);
      this.e.glacier_result_destroy(result);
      throw new Error(msg);
    }
    return result;
  }

  destroyResult(result) {
    this.e.glacier_result_destroy(result);
  }
}
