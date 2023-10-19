const std = @import("std");
const Uuid = @import("uuid6");

const StatefulCommandHandler = @import("command_handler.zig").StatefulCommandHandler;

const Store = @import("store/store.zig").Store;

const helpers = @import("store/test_helpers.zig");
const StoreDb = helpers.StoreDb;

const bank_account = @import("bank_account.zig");
const Command = bank_account.Command;
const Decider = bank_account.Decider;
const Event = bank_account.Event;
const State = bank_account.State;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() == .leak) {
        std.debug.panic("leaks detected", .{});
    };
    var allocator = gpa.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();

    var prng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.os.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    const rand = prng.random();
    const source = Uuid.v7.Source.init(rand);

    var handler =
        try StatefulCommandHandler(Decider, Command, Event, State)
        .init(allocator, "ACCT-1", &store, source);

    var metadata = std.StringHashMap([]const u8).init(allocator);
    defer metadata.deinit();
    var command = .{ .open_account = 500 };
    var results = try handler.handleCommand(command, metadata);
    for (results) |result| {
        std.debug.print("ID: {s}\n", .{result.id});
        std.debug.print("Event type: {s}\n", .{result.event_type});
        result.deinit();
    }

    std.debug.print("State: {any}\n", .{handler.state});
}
