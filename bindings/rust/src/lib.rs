//! Thin `extern "C"` wrapper around `include/glacier.h`. Does not reimplement the engine.

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

#[repr(C)]
pub struct GlacierDatabase {
    _private: [u8; 0],
}
#[repr(C)]
pub struct GlacierConn {
    _private: [u8; 0],
}
#[repr(C)]
pub struct GlacierResult {
    _private: [u8; 0],
}

#[repr(C)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

#[repr(C)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

#[link(name = "glacier")]
extern "C" {
    fn glacier_version() -> *const c_char;
    fn glacier_api_version() -> c_int;
    fn glacier_open(path: *const c_char, err: *mut *mut c_char) -> *mut GlacierDatabase;
    fn glacier_open_buffer(
        buf: *const c_void,
        n: usize,
        err: *mut *mut c_char,
    ) -> *mut GlacierDatabase;
    fn glacier_close(db: *mut GlacierDatabase);
    fn glacier_connect(db: *mut GlacierDatabase, err: *mut *mut c_char) -> *mut GlacierConn;
    fn glacier_disconnect(conn: *mut GlacierConn);
    fn glacier_query(
        conn: *mut GlacierConn,
        sql: *const c_char,
        err: *mut *mut c_char,
    ) -> *mut GlacierResult;
    fn glacier_result_arrow(
        result: *mut GlacierResult,
        array: *mut ArrowArray,
        schema: *mut ArrowSchema,
    ) -> c_int;
    fn glacier_result_error(result: *const GlacierResult) -> *const c_char;
    fn glacier_result_destroy(result: *mut GlacierResult);
    fn glacier_free(ptr: *mut c_void);
}

fn take_c_err(err: *mut c_char) -> String {
    if err.is_null() {
        return "unknown error".into();
    }
    let msg = unsafe { CStr::from_ptr(err) }
        .to_string_lossy()
        .into_owned();
    unsafe { glacier_free(err as *mut c_void) };
    msg
}

pub fn version() -> String {
    unsafe { CStr::from_ptr(glacier_version()) }
        .to_string_lossy()
        .into_owned()
}

pub fn api_version() -> i32 {
    unsafe { glacier_api_version() }
}

pub struct Conn {
    db: *mut GlacierDatabase,
    conn: *mut GlacierConn,
}

unsafe impl Send for Conn {}

impl Drop for Conn {
    fn drop(&mut self) {
        unsafe {
            if !self.conn.is_null() {
                glacier_disconnect(self.conn);
                self.conn = ptr::null_mut();
            }
            if !self.db.is_null() {
                glacier_close(self.db);
                self.db = ptr::null_mut();
            }
        }
    }
}

impl Conn {
    pub fn connect(path: Option<&str>) -> Result<Self, String> {
        let mut err: *mut c_char = ptr::null_mut();
        let db = unsafe {
            match path {
                None | Some("") => glacier_open(ptr::null(), &mut err),
                Some(p) => {
                    let c = CString::new(p).map_err(|_| "path contains NUL")?;
                    glacier_open(c.as_ptr(), &mut err)
                }
            }
        };
        if db.is_null() {
            return Err(take_c_err(err));
        }
        let mut cerr: *mut c_char = ptr::null_mut();
        let conn = unsafe { glacier_connect(db, &mut cerr) };
        if conn.is_null() {
            unsafe { glacier_close(db) };
            return Err(take_c_err(cerr));
        }
        Ok(Conn { db, conn })
    }

    pub fn execute(&self, sql: &str) -> Result<Vec<Vec<Cell>>, String> {
        if self.conn.is_null() {
            return Err("connection is closed".into());
        }
        let csql = CString::new(sql).map_err(|_| "sql contains NUL")?;
        let mut err: *mut c_char = ptr::null_mut();
        let res = unsafe { glacier_query(self.conn, csql.as_ptr(), &mut err) };
        if res.is_null() {
            return Err(take_c_err(err));
        }
        let qerr = unsafe { glacier_result_error(res) };
        if !qerr.is_null() {
            let msg = unsafe { CStr::from_ptr(qerr) }
                .to_string_lossy()
                .into_owned();
            unsafe { glacier_result_destroy(res) };
            return Err(msg);
        }
        let rows = rows_from_result(res);
        unsafe { glacier_result_destroy(res) };
        rows
    }

    pub fn read_parquet(&mut self, buf: &[u8]) -> Result<Vec<Vec<Cell>>, String> {
        if buf.is_empty() {
            return Err("buffer is empty".into());
        }
        let mut err: *mut c_char = ptr::null_mut();
        let db = unsafe {
            glacier_open_buffer(buf.as_ptr() as *const c_void, buf.len(), &mut err)
        };
        if db.is_null() {
            return Err(take_c_err(err));
        }
        unsafe {
            glacier_disconnect(self.conn);
            glacier_close(self.db);
        }
        let mut cerr: *mut c_char = ptr::null_mut();
        let conn = unsafe { glacier_connect(db, &mut cerr) };
        if conn.is_null() {
            unsafe { glacier_close(db) };
            self.db = ptr::null_mut();
            self.conn = ptr::null_mut();
            return Err(take_c_err(cerr));
        }
        self.db = db;
        self.conn = conn;
        self.execute("SELECT *")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Utf8(String),
}

fn rows_from_result(res: *mut GlacierResult) -> Result<Vec<Vec<Cell>>, String> {
    let mut array = ArrowArray {
        length: 0,
        null_count: 0,
        offset: 0,
        n_buffers: 0,
        n_children: 0,
        buffers: ptr::null_mut(),
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    };
    let mut schema = ArrowSchema {
        format: ptr::null(),
        name: ptr::null(),
        metadata: ptr::null(),
        flags: 0,
        n_children: 0,
        children: ptr::null_mut(),
        dictionary: ptr::null_mut(),
        release: None,
        private_data: ptr::null_mut(),
    };
    let rc = unsafe { glacier_result_arrow(res, &mut array, &mut schema) };
    if rc != 0 {
        return Err("failed to export Arrow".into());
    }
    let out = unsafe { decode_struct(&schema, &array) };
    unsafe {
        if let Some(rel) = array.release {
            rel(&mut array);
        }
        if let Some(rel) = schema.release {
            rel(&mut schema);
        }
    }
    out
}

unsafe fn decode_struct(
    schema: &ArrowSchema,
    array: &ArrowArray,
) -> Result<Vec<Vec<Cell>>, String> {
    let fmt = CStr::from_ptr(schema.format).to_string_lossy();
    if fmt != "+s" {
        return Err(format!("expected struct Arrow batch, got {fmt}"));
    }
    let n_rows = array.length as usize;
    let n_cols = schema.n_children as usize;
    let children_s = std::slice::from_raw_parts(schema.children, n_cols);
    let children_a = std::slice::from_raw_parts(array.children, n_cols);
    let mut rows = Vec::with_capacity(n_rows);
    for r in 0..n_rows {
        let mut row = Vec::with_capacity(n_cols);
        for c in 0..n_cols {
            row.push(cell_at(&*children_s[c], &*children_a[c], r));
        }
        rows.push(row);
    }
    Ok(rows)
}

unsafe fn cell_at(s: &ArrowSchema, a: &ArrowArray, i: usize) -> Cell {
    if !valid_at(a, i) {
        return Cell::Null;
    }
    let fmt = CStr::from_ptr(s.format).to_bytes();
    let bufs = std::slice::from_raw_parts(a.buffers, a.n_buffers as usize);
    match fmt {
        b"b" => {
            let bits = bufs[1] as *const u8;
            let byte = *bits.add(i >> 3);
            Cell::Bool(byte & (1 << (i & 7)) != 0)
        }
        b"i" => Cell::I32(*(bufs[1] as *const i32).add(i)),
        b"l" => Cell::I64(*(bufs[1] as *const i64).add(i)),
        b"f" => Cell::F32(*(bufs[1] as *const f32).add(i)),
        b"g" => Cell::F64(*(bufs[1] as *const f64).add(i)),
        b"u" => {
            let off = bufs[1] as *const i32;
            let start = *off.add(i) as usize;
            let end = *off.add(i + 1) as usize;
            let bytes = std::slice::from_raw_parts(bufs[2] as *const u8, end);
            Cell::Utf8(String::from_utf8_lossy(&bytes[start..end]).into_owned())
        }
        _ => Cell::Null,
    }
}

unsafe fn valid_at(a: &ArrowArray, i: usize) -> bool {
    if a.null_count == 0 || a.n_buffers < 1 || a.buffers.is_null() {
        return true;
    }
    let bufs = std::slice::from_raw_parts(a.buffers, a.n_buffers as usize);
    if bufs[0].is_null() {
        return true;
    }
    let bits = bufs[0] as *const u8;
    let byte = *bits.add(i >> 3);
    byte & (1 << (i & 7)) != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sales() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/formats/sales.parquet")
    }

    #[test]
    fn select_1() {
        assert_eq!(version(), "0.1.0");
        assert_eq!(api_version(), 1);
        let con = Conn::connect(None).unwrap();
        let rows = con.execute("select 1").unwrap();
        assert_eq!(rows, vec![vec![Cell::I64(1)]]);
    }

    #[test]
    fn parquet_and_join() {
        let con = Conn::connect(Some(sales().to_str().unwrap())).unwrap();
        let rows = con.execute("SELECT COUNT(*)").unwrap();
        assert_eq!(rows, vec![vec![Cell::I64(10)]]);
        let buf = std::fs::read(sales()).unwrap();
        let mut con = con;
        let all = con.read_parquet(&buf).unwrap();
        assert_eq!(all.len(), 10);
        let empty = Conn::connect(None).unwrap();
        let err = empty.execute("SELECT * FROM a JOIN b").unwrap_err();
        assert_eq!(err, "JOIN is not supported");
    }
}
