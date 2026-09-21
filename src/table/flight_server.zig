//! Arrow Flight SQL on `glacier serve`. Native CLI only; WASM imports a stub.

const builtin = @import("builtin");

const impl = if (builtin.cpu.arch == .wasm32)
    @import("flight_server_stub.zig")
else
    @import("flight_server_impl.zig");

pub const Handle = impl.Handle;
pub const run = impl.run;
pub const bind = impl.bind;
pub const serveLoop = impl.serveLoop;
pub const runSql = impl.runSql;
pub const queryI64 = impl.queryI64;
pub const queryI64Stream = impl.queryI64Stream;

test {
    _ = impl;
    _ = @import("h2.zig");
    _ = @import("flight_server_impl.zig");
}

