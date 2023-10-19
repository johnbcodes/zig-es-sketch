const std = @import("std");
const sqlite = @import("sqlite");

pub const Record = struct {
    id: []const u8,
    stream_name: []const u8,
    global_position: i64,
    position: i64,
};

pub const StoreDb = struct {
    const Self = @This();
    db: sqlite.Db,

    pub fn init() !Self {
        var db = try sqlite.Db.init(.{
            .open_flags = .{
                .write = true,
                .create = true,
            },
            .mode = .{ .Memory = {} },
            //.mode = .{ .File = "data.db" },
        });
        return Self{ .db = db };
    }

    pub fn deinit(self: *Self) void {
        self.db.deinit();
    }

    pub fn insert(self: *Self, record: Record) !void {
        const sql =
            \\INSERT INTO _stremes_events (
            \\  id,
            \\  stream_name,
            \\  event_type,
            \\  event_data,
            \\  metadata,
            \\  global_position,
            \\  position,
            \\  created_at
            \\) VALUES (
            \\  ?,
            \\  ?,
            \\  ?,
            \\  ?,
            \\  ?,
            \\  ?,
            \\  ?,
            \\  ?
            \\);
        ;

        try self.db.exec(sql, .{}, .{
            record.id,
            record.stream_name,
            "event_type",
            "event_data",
            "metadata",
            record.global_position,
            record.position,
            "timestamp",
        });
    }

    pub fn insert_all(self: *Self, records: []Record) !void {
        var tx = try self.db.transaction();
        for (records) |record| {
            try self.insert(record);
        }
        try tx.commit();
    }

    pub fn lastEvent(self: *Self, allocator: std.mem.Allocator, stream_name: []const u8) !?Record {
        const sql =
            \\SELECT
            \\  id,
            \\  stream_name,
            \\  global_position,
            \\  position
            \\FROM
            \\  _stremes_events
            \\WHERE
            \\  stream_name = ?
            \\ORDER BY
            \\  position DESC
            \\LIMIT
            \\  1;
        ;

        return try self.db.oneAlloc(Record, allocator, sql, .{}, .{stream_name});
    }

    pub fn insert_default_fixtures(self: *Self) !void {
        var records = [_]Record{
            .{ .id = "id-1", .stream_name = "ACCT-1", .global_position = 1, .position = 1 },
            .{ .id = "id-2", .stream_name = "ACCT-1", .global_position = 2, .position = 2 },
            .{ .id = "id-3", .stream_name = "ACCT-2", .global_position = 3, .position = 1 },
            .{ .id = "id-4", .stream_name = "ACCT-1", .global_position = 4, .position = 3 },
            .{ .id = "id-5", .stream_name = "ACCT-2", .global_position = 5, .position = 2 },
            .{ .id = "id-6", .stream_name = "ACCT-3", .global_position = 6, .position = 1 },
            .{ .id = "id-7", .stream_name = "ACCT-3", .global_position = 7, .position = 2 },
        };
        try self.insert_all(records[0..]);
    }
};
