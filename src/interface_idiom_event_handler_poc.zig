const std = @import("std");

pub fn EventHandler(comptime Event: type) type {
    return struct {
        const Self = @This();
        ptr: *anyopaque,
        handleEventFn: *const fn (ptr: *anyopaque, event: Event) void,

        fn init(ptr: anytype) Self {
            const T = @TypeOf(ptr);
            const ptr_info = @typeInfo(T);

            const gen = struct {
                pub fn handleEvent(pointer: *anyopaque, event: Event) void {
                    const self: T = @ptrCast(@alignCast(pointer));
                    return ptr_info.Pointer.child.handleEvent(self, event);
                }
            };

            return .{
                .ptr = ptr,
                .handleEventFn = gen.handleEvent,
            };
        }

        pub fn handleEvent(self: *EventHandler(Event), event: Event) void {
            return self.handleEventFn(self.ptr, event);
        }
    };
}

const DogHandler = struct {
    const Self = @This();

    fn eventHandler(self: *Self) EventHandler([]const u8) {
        return EventHandler([]const u8).init(self);
    }

    fn handleEvent(self: *Self, event: []const u8) void {
        _ = self;
        std.debug.print("Woof to {s}!\n", .{event});
    }
};

const CatHandler = struct {
    const Self = @This();

    fn eventHandler(self: *Self) EventHandler([]const u8) {
        return EventHandler([]const u8).init(self);
    }

    fn handleEvent(self: *Self, event: []const u8) void {
        _ = self;
        std.debug.print("Meow to {s}!\n", .{event});
    }
};

const MouseHandler = struct {
    const Self = @This();

    fn eventHandler(self: *Self) EventHandler([]const u8) {
        return EventHandler([]const u8).init(self);
    }

    fn handleEvent(self: *Self, event: []const u8) void {
        _ = self;
        std.debug.print("Squeak to {s}!\n", .{event});
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();
    defer if (gpa.deinit() == .leak) {
        std.debug.panic("leaks detected", .{});
    };

    var handlers = std.ArrayList(EventHandler([]const u8)).init(allocator);
    defer handlers.deinit();

    var dog = DogHandler{};
    var cat = CatHandler{};
    var mouse = MouseHandler{};
    try handlers.append(dog.eventHandler());
    try handlers.append(cat.eventHandler());
    try handlers.append(mouse.eventHandler());

    for (handlers.items) |*handler| {
        handler.handleEvent("World");
    }
}
