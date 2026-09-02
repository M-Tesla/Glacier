// Package glacier is a thin cgo wrapper around include/glacier.h.
package glacier

/*
#cgo CFLAGS: -I"${SRCDIR}/../../include"
#cgo LDFLAGS: -L"${SRCDIR}/../../zig-out/lib" -lglacier -Wl,-rpath,"${SRCDIR}/../../zig-out/lib"
#include "glacier.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

static void call_arrow_array_release(struct ArrowArray *a) {
	if (a && a->release) a->release(a);
}
static void call_arrow_schema_release(struct ArrowSchema *s) {
	if (s && s->release) s->release(s);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type Error struct {
	msg string
}

func (e Error) Error() string { return e.msg }

func Version() string {
	return C.GoString(C.glacier_version())
}

func APIVersion() int {
	return int(C.glacier_api_version())
}

type Conn struct {
	db   *C.GlacierDatabase
	conn *C.GlacierConn
}

func Connect(path string) (*Conn, error) {
	var err *C.char
	var db *C.GlacierDatabase
	if path == "" {
		db = C.glacier_open(nil, &err)
	} else {
		cpath := C.CString(path)
		db = C.glacier_open(cpath, &err)
		C.free(unsafe.Pointer(cpath))
	}
	if db == nil {
		msg := "open failed"
		if err != nil {
			msg = C.GoString(err)
			C.glacier_free(unsafe.Pointer(err))
		}
		return nil, Error{msg}
	}
	var cerr *C.char
	cn := C.glacier_connect(db, &cerr)
	if cn == nil {
		msg := "connect failed"
		if cerr != nil {
			msg = C.GoString(cerr)
			C.glacier_free(unsafe.Pointer(cerr))
		}
		C.glacier_close(db)
		return nil, Error{msg}
	}
	return &Conn{db: db, conn: cn}, nil
}

func (c *Conn) Close() {
	if c == nil || c.db == nil {
		return
	}
	C.glacier_disconnect(c.conn)
	C.glacier_close(c.db)
	c.db = nil
	c.conn = nil
}

func (c *Conn) Execute(sql string) ([][]any, error) {
	if c == nil || c.db == nil {
		return nil, Error{"connection is closed"}
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var err *C.char
	res := C.glacier_query(c.conn, csql, &err)
	if res == nil {
		msg := "query failed"
		if err != nil {
			msg = C.GoString(err)
			C.glacier_free(unsafe.Pointer(err))
		}
		return nil, Error{msg}
	}
	defer C.glacier_result_destroy(res)
	if qerr := C.glacier_result_error(res); qerr != nil {
		return nil, Error{C.GoString(qerr)}
	}
	return rowsFromResult(res)
}

func (c *Conn) ReadParquet(buf []byte) ([][]any, error) {
	if c == nil || c.db == nil {
		return nil, Error{"connection is closed"}
	}
	if len(buf) == 0 {
		return nil, Error{"buffer is empty"}
	}
	var err *C.char
	db := C.glacier_open_buffer(unsafe.Pointer(&buf[0]), C.size_t(len(buf)), &err)
	if db == nil {
		msg := "open buffer failed"
		if err != nil {
			msg = C.GoString(err)
			C.glacier_free(unsafe.Pointer(err))
		}
		return nil, Error{msg}
	}
	C.glacier_disconnect(c.conn)
	C.glacier_close(c.db)
	var cerr *C.char
	cn := C.glacier_connect(db, &cerr)
	if cn == nil {
		msg := "connect failed"
		if cerr != nil {
			msg = C.GoString(cerr)
			C.glacier_free(unsafe.Pointer(cerr))
		}
		C.glacier_close(db)
		c.db = nil
		c.conn = nil
		return nil, Error{msg}
	}
	c.db = db
	c.conn = cn
	return c.Execute("SELECT *")
}

func rowsFromResult(res *C.GlacierResult) ([][]any, error) {
	var array C.struct_ArrowArray
	var schema C.struct_ArrowSchema
	if C.glacier_result_arrow(res, &array, &schema) != 0 {
		return nil, Error{"failed to export Arrow"}
	}
	defer func() {
		if array.release != nil {
			C.call_arrow_array_release(&array)
		}
		if schema.release != nil {
			C.call_arrow_schema_release(&schema)
		}
	}()
	fmtStr := C.GoString(schema.format)
	if fmtStr != "+s" {
		return nil, Error{fmt.Sprintf("expected struct Arrow batch, got %q", fmtStr)}
	}
	nRows := int(array.length)
	nCols := int(schema.n_children)
	rows := make([][]any, nRows)
	childrenS := unsafe.Slice(schema.children, nCols)
	childrenA := unsafe.Slice(array.children, nCols)
	for r := 0; r < nRows; r++ {
		row := make([]any, nCols)
		for c := 0; c < nCols; c++ {
			row[c] = cellAt(childrenS[c], childrenA[c], r)
		}
		rows[r] = row
	}
	return rows, nil
}

func cellAt(s *C.struct_ArrowSchema, a *C.struct_ArrowArray, i int) any {
	if a == nil || s == nil {
		return nil
	}
	if !validAt(a, i) {
		return nil
	}
	fmtStr := C.GoString(s.format)
	bufs := bufferSlice(a)
	switch fmtStr {
	case "b":
		bits := unsafe.Slice((*byte)(bufs[1]), int((a.length+7)/8)+1)
		return bits[i>>3]&(1<<uint(i&7)) != 0
	case "i":
		d := unsafe.Slice((*int32)(bufs[1]), int(a.length))
		return d[i]
	case "l":
		d := unsafe.Slice((*int64)(bufs[1]), int(a.length))
		return d[i]
	case "f":
		d := unsafe.Slice((*float32)(bufs[1]), int(a.length))
		return d[i]
	case "g":
		d := unsafe.Slice((*float64)(bufs[1]), int(a.length))
		return d[i]
	case "u":
		off := unsafe.Slice((*int32)(bufs[1]), int(a.length)+1)
		start, end := int(off[i]), int(off[i+1])
		bytes := unsafe.Slice((*byte)(bufs[2]), end)
		return string(bytes[start:end])
	default:
		return nil
	}
}

func validAt(a *C.struct_ArrowArray, i int) bool {
	if a.null_count == 0 || a.n_buffers < 1 || a.buffers == nil {
		return true
	}
	bufs := bufferSlice(a)
	if bufs[0] == nil {
		return true
	}
	bits := unsafe.Slice((*byte)(bufs[0]), int((a.length+7)/8)+1)
	return bits[i>>3]&(1<<uint(i&7)) != 0
}

func bufferSlice(a *C.struct_ArrowArray) []unsafe.Pointer {
	return unsafe.Slice((*unsafe.Pointer)(unsafe.Pointer(a.buffers)), int(a.n_buffers))
}
