const std = @import("std");
const StringHashMap = std.StringArrayHashMap;
const EventEnvelope = @import("core.zig").EventEnvelope;
const Dispatcher = @import("core.zig").Dispatcher;

pub const Event = union(enum) {
    account_opened: u32,
    funds_withdrawn: u32,
    funds_deposited: u32,
};

const EventHandler = union(enum) {
    my_read_model: ReadModel,

    pub fn handleEvent(self: *EventHandler, event: EventEnvelope(Event)) !void {
        switch (self.*) {
            inline else => |*case| try case.handleEvent(event),
        }
    }
};

const ReadModel = struct {
    fn handleEvent(self: *ReadModel, event: EventEnvelope(Event)) !void {
        _ = self;
        switch (event.payload) {
            .account_opened => |val| {
                std.debug.print("Account opened for {d}\n", .{val});
            },
            .funds_withdrawn => |val| {
                std.debug.print("Funds withdrawn for {d}\n", .{val});
            },
            .funds_deposited => |val| {
                std.debug.print("Funds deposited for {d}\n", .{val});
            },
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();
    defer if (gpa.deinit() == .leak) {
        std.debug.panic("leaks detected", .{});
    };

    var dispatcher = Dispatcher(EventHandler, Event).init(allocator);
    defer dispatcher.deinit();
    try dispatcher.add_handler(.{ .my_read_model = ReadModel{} });

    var event = Event{ .account_opened = 100 };
    var metadata = StringHashMap([]const u8).init(allocator);
    var envelope = .{
        .id = "test-1",
        .event_type = "event_handler.Event.account_opened",
        .payload = event,
        .metadata = metadata,
    };

    try dispatcher.dispatch(envelope);
}
