const std = @import("std");
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;

pub fn EventEnvelope(comptime Event: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        id: []const u8,
        event_type: []const u8,
        payload: *Event,
        metadata: StringHashMap([]const u8),

        pub fn deinit(self: Self) void {
            self.allocator.free(self.id);
            self.allocator.free(self.event_type);
            self.allocator.destroy(self.payload);
        }
    };
}

pub fn Dispatcher(comptime EventHandler: type, comptime Event: type) type {
    return struct {
        const Self = @This();

        handlers: ArrayList(EventHandler),

        pub fn init(allocator: std.mem.Allocator) Self {
            var handlers = ArrayList(EventHandler).init(allocator);
            return Self{ .handlers = handlers };
        }

        pub fn deinit(self: *Self) void {
            self.handlers.deinit();
        }

        pub fn add_handler(self: *Self, handler: EventHandler) !void {
            try self.handlers.append(handler);
        }

        pub fn dispatch(self: *Self, event_data: EventEnvelope(Event)) !void {
            for (self.handlers.items) |*handler| {
                try handler.handleEvent(event_data);
            }
        }
    };
}
