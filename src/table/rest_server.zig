//! Iceberg REST Catalog server. Native `glacier serve` only.
//! WASM / `libglacier` must not listen: wasm32 imports a stub, not the HTTP loop.

const builtin = @import("builtin");

const impl = if (builtin.cpu.arch == .wasm32)
    @import("rest_server_stub.zig")
else
    @import("rest_server_impl.zig");

pub const Handle = impl.Handle;
pub const run = impl.run;
pub const bind = impl.bind;
pub const serveLoop = impl.serveLoop;
pub const parseListen = impl.parseListen;
pub const parseListenPort = impl.parseListenPort;
