const std = @import("std");
const testing = std.testing;
pub const store = @import("store/store.zig");

test {
    testing.refAllDecls(@This());
}
