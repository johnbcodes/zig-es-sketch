const std = @import("std");
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;

const Uuid = @import("uuid6");

const EventEnvelope = @import("core.zig").EventEnvelope;

const store = @import("store/store.zig");
const EventData = store.EventData;
const ExpectedVersion = store.ExpectedVersion;
const RecordedEvent = store.RecordedEvent;
const Store = store.Store;

pub fn StatefulCommandHandler(comptime Decider: type, comptime Command: type, comptime Event: type, comptime State: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        stream_name: []const u8,
        store: *Store,
        state: State,
        current_version: i64,
        uuid_source: Uuid.v7.Source,

        pub fn init(allocator: std.mem.Allocator, stream_name: []const u8, event_store: *Store, uuid_source: Uuid.v7.Source) !Self {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            var arena_allocator = arena.allocator();

            var events = try load(arena_allocator, event_store, stream_name);
            var max_position = if (events.len > 0) events[events.len - 1].position else 0;

            var converted = ArrayList(*Event).init(arena_allocator);
            for (events) |event| {
                var parsed_event = try std.json.parseFromSliceLeaky(Event, arena_allocator, event.event_data, .{});
                try converted.append(&parsed_event);
            }

            const initial_state = Decider.initialState();
            const state = Decider.evolve_all(initial_state, &converted.items);

            return Self{
                .allocator = allocator,
                .stream_name = stream_name,
                .store = event_store,
                .state = state,
                .current_version = max_position,
                .uuid_source = uuid_source,
            };
        }

        pub fn handleCommand(self: *Self, command: Command, metadata: StringHashMap([]const u8)) ![]EventEnvelope(Event) {
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            var arena_allocator = arena.allocator();

            const events = try Decider.decide(self.allocator, command);
            //defer self.allocator.free(events);

            var result = self.append(arena_allocator, events, metadata) catch |err| {
                switch (err) {
                    store.StoreError.WrongExpectedVersion => {
                        const catchup_events = try self.store.read_stream(arena_allocator, self.stream_name, .{ .position = .{ .Position = self.current_version } });
                        var actual_version = if (catchup_events.len > 0) catchup_events[catchup_events.len - 1].position else 0;
                        self.current_version = actual_version;

                        var converted = ArrayList(*Event).init(arena_allocator);
                        for (catchup_events) |catchup_event| {
                            var parsed_event = try std.json.parseFromSliceLeaky(Event, arena_allocator, catchup_event.event_data, .{});
                            try converted.append(&parsed_event);
                        }

                        self.state = Decider.evolve_all(self.state, &converted.items);
                        return try self.handleCommand(command, metadata);
                    },
                    else => {},
                }
                return err;
            };

            // Current error begins here because the event in the slice is considered corrupt
            self.state = Decider.evolve_all(self.state, &events);
            return result;
        }

        fn load(arena_allocator: std.mem.Allocator, event_store: *Store, stream_name: []const u8) ![]RecordedEvent {
            return event_store.read_stream(arena_allocator, stream_name, .{}) catch |err| {
                switch (err) {
                    store.StoreError.StreamDoesNotExist => return arena_allocator.dupe(RecordedEvent, &[_]RecordedEvent{}),
                    else => return err,
                }
            };
        }

        fn append(self: *Self, arena_allocator: std.mem.Allocator, events: []*Event, metadata: StringHashMap([]const u8)) ![]EventEnvelope(Event) {
            var results = ArrayList(EventEnvelope(Event)).init(self.allocator);
            var converted = ArrayList(EventData).init(arena_allocator);

            for (events) |event| {
                var event_json = try std.json.stringifyAlloc(arena_allocator, event, .{});

                // Need to get past:
                // error: unable to stringify type '[*]hash_map.HashMapUnmanaged([]const u8,[]const u8,hash_map.StringContext,80).Metadata' without sentinel
                //var metadata_json = try std.json.stringifyAlloc(arena_allocator, metadata, .{});

                const id = try self.uuidString();
                const event_type = try self.eventType(event);
                var event_data = .{
                    .id = id,
                    .event_type = event_type,
                    .event_data = event_json,
                    .metadata = "metadata_fixme",
                };
                try converted.append(event_data);

                // Commenting this out results in a different error where the the event
                // is not freed correctly
                var envelope = .{
                    .allocator = self.allocator,
                    .id = id,
                    .event_type = event_type,
                    .payload = event,
                    .metadata = metadata,
                };
                try results.append(envelope);
            }

            const expected_version = if (self.current_version != 0) ExpectedVersion{ .Exact = self.current_version } else ExpectedVersion.NoStream;
            _ = try self.store.append(self.stream_name, converted.items, expected_version);

            return results.toOwnedSlice();
        }

        fn uuidString(self: *Self) ![]const u8 {
            var uuid_string = try self.allocator.alloc(u8, 36);
            var uuid_stream = std.io.fixedBufferStream(uuid_string);
            const uuid = self.uuid_source.create();
            try std.fmt.format(uuid_stream.writer(), "{}", .{uuid});
            return uuid_string;
        }

        fn eventType(self: *Self, event: *Event) ![]const u8 {
            const type_name = @typeName(@TypeOf(event.*)) ++ ".";
            const tag_name = @tagName(event.*);
            var event_type = try self.allocator.alloc(u8, type_name.len + tag_name.len);
            std.mem.copyForwards(u8, event_type[0..], type_name);
            std.mem.copyForwards(u8, event_type[type_name.len..], tag_name);
            return event_type;
        }
    };
}
