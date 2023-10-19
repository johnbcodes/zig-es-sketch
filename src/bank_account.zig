const std = @import("std");

pub const Event = union(enum) {
    account_opened: u32,
    funds_withdrawn: u32,
    funds_deposited: u32,
};

pub const Command = union(enum) {
    open_account: u32,
    withdraw_funds: u32,
    deposit_funds: u32,
};

pub const State = struct {
    balance: i64,
};

pub const Decider = struct {
    pub fn initialState() State {
        return State{ .balance = 0 };
    }

    // pub fn decide(allocator: std.mem.Allocator, cmd: Command) ![]Event {
    //     var result = switch (cmd) {
    //         .open_account => |val| [_]Event{.{ .account_opened = val }},
    //         .withdraw_funds => |val| [_]Event{.{ .funds_withdrawn = val }},
    //         .deposit_funds => |val| [_]Event{.{ .funds_deposited = val }},
    //     };
    //     return try allocator.dupe(Event, &result);
    // }

    // TODO: Figure out how to return a slice of events where the individual events
    //       outlive the slice in a way that the events can be managed later.
    //
    //       Must I return explicit pointers here?
    //       In the commented-out version above:
    //          Can we split the items of slice out individually?
    //          Can an item on the stack be moved to the heap?
    //       Etc.
    pub fn decide(allocator: std.mem.Allocator, cmd: Command) ![]*Event {
        var event = try allocator.create(Event);
        event.* = switch (cmd) {
            .open_account => |val| .{ .account_opened = val },
            .withdraw_funds => |val| .{ .funds_withdrawn = val },
            .deposit_funds => |val| .{ .funds_deposited = val },
        };
        var events = [_]*Event{event};
        return events[0..];
    }

    pub fn evolve_all(state: State, events: *const []*Event) State {
        var result: State = state;
        for (events.*) |event| {
            result = evolve(result, event);
        }
        return result;
    }

    // Current error:
    // thread 13180559 panic: switch on corrupt value
    // .../zig-es-sketch/src/bank_account.zig:61:30: 0x10b226d09 in evolve (command-handler-poc)
    // var result = switch (event.*) {
    fn evolve(state: State, event: *Event) State {
        var result = switch (event.*) {
            .account_opened => |val| State{ .balance = val },
            .funds_withdrawn => |val| State{ .balance = state.balance - val },
            .funds_deposited => |val| State{ .balance = state.balance + val },
        };
        return result;
    }

    pub fn isTerminal() bool {
        return false;
    }
};

const testing = std.testing;
test "open account" {
    const cmd = Command{ .open_account = 10 };
    var result = try Decider.decide(testing.allocator, cmd);
    defer testing.allocator.free(result);
    try testing.expectEqual(@as(u32, 10), result[0].account_opened);
}

test "deposit funds" {
    const state = State{ .balance = 10 };
    const evt = Event{ .funds_deposited = 20 };
    const result = Decider.evolve(state, evt);
    try testing.expectEqual(@as(i64, 30), result.balance);
}

test "evolve all" {
    const state = State{ .balance = 0 };
    var evts = [_]Event{ .{ .funds_deposited = 20 }, .{ .funds_deposited = 30 }, .{ .funds_withdrawn = 10 } };
    const result = Decider.evolve_all(state, evts[0..]);
    try testing.expectEqual(@as(i64, 40), result.balance);
}
