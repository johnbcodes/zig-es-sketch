const std = @import("std");
const sqlite = @import("sqlite");

const logger = std.log.scoped(.ziges);

pub const StoreError = error{
    //    #[error("Wrong expected version: expected '{expected}' but received '{current}'")]
    WrongExpectedVersion,
    //    #[error("Must be a stream name but received category: {given}")]
    MustBeStream,
    //    #[error("Must be a category but received stream name: {given}")]
    MustBeCategory,
    //    #[error("Stream does not exist: {0}")]
    StreamDoesNotExist,
    //    #[error("Stream already exists: {0}")]
    StreamAlreadyExists,
    //    #[error("Category does not exist: {0}")]
    CategoryDoesNotExist,
    //    #[error("Invalid position: {0}, it must be greater than or equal to 1")]
    InvalidPosition,
    //    #[error("Invalid limit: {0}, it must be greater than or equal to 1")]
    InvalidLimit,
};

pub const EventData = struct {
    id: []const u8,
    event_type: []const u8,
    event_data: []const u8,
    metadata: []const u8,
};

pub const RecordedEvent = struct {
    id: []const u8,
    stream_name: []const u8,
    event_type: []const u8,
    event_data: []const u8,
    metadata: []const u8,
    global_position: i64,
    position: i64,
    created_at: []const u8,
};

pub const ExpectedVersion = union(enum) {
    /// This write should not conflict with anything and should always succeed.
    Any,

    /// The stream should exist. If it or a metadata stream does not exist,
    /// treats that as a concurrency problem.
    StreamExists,

    /// The stream being written to should not yet exist. If it does exist,
    /// treats that as a concurrency problem.
    NoStream,

    /// States that the last event written to the stream should have an event
    /// number matching your expected value.
    Exact: i64,
};

pub const ReadDirection = enum {
    Forward,
    Backward,
};

pub const Position = union(enum) {
    Start,
    End,
    Position: i64,
};

pub const Limit = union(enum) {
    All,
    Limit: i64,
};

pub const ReadOptions = struct {
    position: ?Position = Position.Start,
    direction: ?ReadDirection = ReadDirection.Forward,
    limit: ?Limit = Limit.All,
};

pub const Store = struct {
    const Self = @This();
    db: sqlite.Db,

    pub fn init(db: sqlite.Db) Self {
        return Self{ .db = db };
    }

    pub fn migrate(self: *Self) !void {
        var tx = try self.db.transactionWithBehavior(sqlite.TransactionBehavior.Immediate);
        inline for (@import("ddl.zig").DDL) |ddl| {
            self.db.exec(ddl, .{}, .{}) catch |err| {
                tx.rollback() catch |rerr| {
                    const rollback_error = self.db.getDetailedError();
                    logger.err("Unable to rollback migration transaction, error: {}, message: {s}", .{ rerr, rollback_error });
                    return rerr;
                };
                return err;
            };
        }
        try tx.commit();
    }

    pub fn append(self: *Self, stream_name: []const u8, events: []EventData, expected_version: ExpectedVersion) !i64 {
        var tx = self.db.transactionWithBehavior(sqlite.TransactionBehavior.Immediate) catch |err| {
            const rollback_error = self.db.getDetailedError();
            logger.err("Unable to begin append transaction, error: {}, message: {s}", .{ err, rollback_error });
            return err;
        };
        defer tx.deinit();

        const stream_version: i64 = self.verifyVersion(stream_name, expected_version) catch |err| {
            tx.rollback() catch |rerr| {
                const rollback_error = self.db.getDetailedError();
                logger.err("Unable to rollback append transaction, error: {}, message: {s}", .{ rerr, rollback_error });
                return rerr;
            };
            return err;
        };

        var next_version = stream_version;
        for (events) |event| {
            next_version += 1;
            const insert =
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
                \\  $id{[]const u8},
                \\  $stream_name{[]const u8},
                \\  $event_type{[]const u8},
                \\  $event_data{[]const u8},
                \\  $metadata{[]const u8},
                \\  (select
                \\      coalesce(max(global_position), 0) + 1
                \\    from
                \\      _stremes_events),
                \\  $position{i64},
                \\  $created_at
                \\)
            ;

            const timestamp = "timestamp";
            self.db.exec(
                insert,
                .{},
                .{
                    .id = event.id,
                    .stream_name = stream_name,
                    .event_type = event.event_type,
                    .event_data = event.event_data,
                    .metadata = event.metadata,
                    .position = next_version,
                    .created_at = timestamp,
                },
            ) catch |err| {
                const insert_error = self.db.getDetailedError();
                logger.err("Unable to INSERT recorded event, error: {}, message: {s}", .{ err, insert_error });
                tx.rollback() catch |rerr| {
                    const rollback_error = self.db.getDetailedError();
                    logger.err("Unable to ROLLBACK append transaction, error: {}, message: {s}", .{ rerr, rollback_error });
                    return rerr;
                };
                return err;
            };
        }

        tx.commit() catch |err| {
            const commit_error = self.db.getDetailedError();
            logger.err("Unable to COMMIT append transaction, error: {}, message: {s}", .{ err, commit_error });
            return err;
        };

        return next_version;
    }

    pub fn verifyVersion(self: *Self, stream_name: []const u8, expected_version: ExpectedVersion) !i64 {
        const stream_exists = try self.streamExists(stream_name);
        const stream_version: i64 = switch (expected_version) {
            .Any => {
                if (stream_exists.?) {
                    const stream_version = try self.streamVersion(stream_name);
                    return stream_version.?;
                } else {
                    return 0;
                }
            },
            .StreamExists => {
                if (stream_exists.?) {
                    const stream_version = try self.streamVersion(stream_name);
                    return stream_version.?;
                } else {
                    return StoreError.StreamDoesNotExist;
                }
            },
            .NoStream => {
                if (!stream_exists.?) {
                    return 0;
                } else {
                    return StoreError.StreamAlreadyExists;
                }
            },
            .Exact => |exp_version| {
                if (exp_version < 1) {
                    return StoreError.InvalidPosition;
                }

                if (stream_exists.?) {
                    const current_version = try self.streamVersion(stream_name);
                    if (exp_version != current_version.?) {
                        return StoreError.WrongExpectedVersion;
                    }
                    return current_version.?;
                } else {
                    return StoreError.StreamDoesNotExist;
                }
            },
        };
        return stream_version;
    }

    fn streamExists(self: *Self, stream_name: []const u8) !?bool {
        const query = "select exists(select id from _stremes_events where stream_name = $name)";
        return self.db.one(bool, query, .{}, .{ .name = stream_name }) catch |err| {
            const stmt_error = self.db.getDetailedError();
            logger.err("Unable to verify if stream ({s}) exists, error: {}, message: {s}", .{ stream_name, err, stmt_error });
            return err;
        };
    }

    pub fn streamVersion(self: *Self, stream_name: []const u8) !?i64 {
        const query = "select max(position) from _stremes_events where stream_name = $name";
        return self.db.one(i64, query, .{}, .{ .name = stream_name }) catch |err| {
            const query_error = self.db.getDetailedError();
            logger.err("Unable to determine version of stream ({s}), error: {}, message: {s}", .{ stream_name, err, query_error });
            return err;
        };
    }

    pub fn lastEvent(self: *Self, allocator: std.mem.Allocator, stream_name: []const u8) !?RecordedEvent {
        const sql =
            \\SELECT
            \\  id,
            \\  stream_name,
            \\  event_type,
            \\  event_data,
            \\  metadata,
            \\  global_position,
            \\  position,
            \\  created_at
            \\FROM
            \\  _stremes_events
            \\WHERE
            \\  stream_name = ?
            \\ORDER BY
            \\  position DESC
            \\LIMIT
            \\  1;
        ;
        var result = self.db.oneAlloc(RecordedEvent, allocator, sql, .{}, .{stream_name}) catch |err| {
            const query_error = self.db.getDetailedError();
            logger.err("Unable to retrieve last event of stream ({s}), error: {}, message: {s}", .{ stream_name, err, query_error });
            return err;
        };
        return result;
    }

    pub fn read_stream(self: *Self, allocator: std.mem.Allocator, stream_name: []const u8, options: ReadOptions) ![]RecordedEvent {
        const positionOption = options.position orelse Position.Start;
        const directionOption = options.direction orelse ReadDirection.Forward;
        const limitOption = options.limit orelse Limit.All;

        if (isCategory(stream_name)) return StoreError.MustBeStream;

        const stream_exists = try self.streamExists(stream_name);
        if (!stream_exists.?) {
            return StoreError.StreamDoesNotExist;
        }

        const direction = switch (directionOption) {
            .Forward => "ASC",
            .Backward => "DESC",
        };

        const direction_evaluator = switch (directionOption) {
            .Forward => ">=",
            .Backward => "<=",
        };

        const limit = switch (limitOption) {
            .All => -1,
            .Limit => |value| value,
        };

        if (limit <= 0 and limit != -1) {
            return StoreError.InvalidLimit;
        }

        switch (positionOption) {
            .Start => {
                const sql_format =
                    \\SELECT
                    \\    id,
                    \\    stream_name,
                    \\    event_type,
                    \\    event_data,
                    \\    metadata,
                    \\    global_position,
                    \\    position,
                    \\    created_at
                    \\FROM
                    \\    _stremes_events
                    \\WHERE
                    \\    stream_name = ?
                    \\AND
                    \\    position {s} (
                    \\    SELECT
                    \\        MIN(COALESCE(position, 0))
                    \\    FROM
                    \\        _stremes_events
                    \\    WHERE
                    \\        stream_name = ?
                    \\    )
                    \\ORDER BY
                    \\    position {s}
                    \\LIMIT
                    \\    ?;
                ;

                const sql = try std.fmt.allocPrint(allocator, sql_format, .{ direction_evaluator, direction });
                var stmt = self.db.prepareDynamic(sql) catch |serr| {
                    const stmt_error = self.db.getDetailedError();
                    logger.err("Unable to prepare statement for Start position in stream ({s}), error: {}, message: {s}", .{ stream_name, serr, stmt_error });
                    return serr;
                };
                defer stmt.deinit();

                return stmt.all(
                    RecordedEvent,
                    allocator,
                    .{},
                    .{ stream_name, stream_name, limit },
                ) catch |err| {
                    const query_error = self.db.getDetailedError();
                    logger.err("Unable to retrieve events for Start position in stream ({s}), error: {}, message: {s}", .{ stream_name, err, query_error });
                    return err;
                };
            },
            .End => {
                const sql_format =
                    \\SELECT
                    \\    id,
                    \\    stream_name,
                    \\    event_type,
                    \\    event_data,
                    \\    metadata,
                    \\    global_position,
                    \\    position,
                    \\    created_at
                    \\FROM
                    \\    _stremes_events
                    \\WHERE
                    \\    stream_name = ?
                    \\AND
                    \\    position {s} (
                    \\    SELECT
                    \\        MAX(COALESCE(position, 0))
                    \\    FROM
                    \\        _stremes_events
                    \\    WHERE
                    \\        stream_name = ?
                    \\    )
                    \\ORDER BY
                    \\    position {s}
                    \\LIMIT
                    \\    ?;
                ;

                const sql: []u8 = try std.fmt.allocPrint(allocator, sql_format, .{ direction_evaluator, direction });
                var stmt = self.db.prepareDynamic(sql) catch |serr| {
                    const stmt_error = self.db.getDetailedError();
                    logger.err("Unable to prepare statement for End position in stream ({s}), error: {}, message: {s}", .{ stream_name, serr, stmt_error });
                    return serr;
                };
                defer stmt.deinit();

                return stmt.all(
                    RecordedEvent,
                    allocator,
                    .{},
                    .{ stream_name, stream_name, limit },
                ) catch |err| {
                    const query_error = self.db.getDetailedError();
                    logger.err("Unable to retrieve events for End position of stream ({s}), error: {}, message: {s}", .{ stream_name, err, query_error });
                    return err;
                };
            },
            .Position => |position| {
                if (position < 1) {
                    return StoreError.InvalidPosition;
                }

                const sql_format =
                    \\SELECT
                    \\  id,
                    \\  stream_name,
                    \\  event_type,
                    \\  event_data,
                    \\  metadata,
                    \\  global_position,
                    \\  position,
                    \\  created_at
                    \\FROM
                    \\  _stremes_events
                    \\WHERE
                    \\  stream_name = ?
                    \\AND
                    \\  position {s} ?
                    \\ORDER BY
                    \\  position {s}
                    \\LIMIT
                    \\  ?;
                ;

                const sql: []u8 = try std.fmt.allocPrint(allocator, sql_format, .{ direction_evaluator, direction });
                var stmt = self.db.prepareDynamic(sql) catch |serr| {
                    const stmt_error = self.db.getDetailedError();
                    logger.err("Unable to prepare statement for Position position in stream ({s}), error: {}, message: {s}", .{ stream_name, serr, stmt_error });
                    return serr;
                };
                defer stmt.deinit();

                return stmt.all(
                    RecordedEvent,
                    allocator,
                    .{},
                    .{ stream_name, position, limit },
                ) catch |err| {
                    const query_error = self.db.getDetailedError();
                    logger.err("Unable to retrieve events for Position position of stream ({s}), error: {}, message: {s}", .{ stream_name, err, query_error });
                    return err;
                };
            },
        }
    }

    pub fn read_category(self: *Self, allocator: std.mem.Allocator, category_name: []const u8, options: ReadOptions) ![]RecordedEvent {
        const positionOption = options.position orelse Position.Start;
        const directionOption = options.direction orelse ReadDirection.Forward;
        const limitOption = options.limit orelse Limit.All;

        if (!isCategory(category_name)) return StoreError.MustBeCategory;

        const category_exists = try self.categoryExists(category_name);
        if (!category_exists.?) {
            return StoreError.CategoryDoesNotExist;
        }

        const direction = switch (directionOption) {
            .Forward => "ASC",
            .Backward => "DESC",
        };

        const direction_evaluator = switch (directionOption) {
            .Forward => ">=",
            .Backward => "<=",
        };

        const limit = switch (limitOption) {
            .All => -1,
            .Limit => |value| value,
        };

        if (limit <= 0 and limit != -1) {
            return StoreError.InvalidLimit;
        }

        switch (positionOption) {
            .Start => {
                const sql_format =
                    \\SELECT
                    \\  id,
                    \\  stream_name,
                    \\  event_type,
                    \\  event_data,
                    \\  metadata,
                    \\  global_position,
                    \\  position,
                    \\  created_at
                    \\FROM
                    \\  _stremes_events
                    \\WHERE
                    \\  substr(stream_name, 0, instr(stream_name, '-')) = ?
                    \\AND
                    \\  global_position {s} (
                    \\    SELECT
                    \\      MIN(COALESCE(global_position, 0))
                    \\    FROM
                    \\      _stremes_events
                    \\    WHERE
                    \\      substr(stream_name, 0, instr(stream_name, '-')) = ?
                    \\  )
                    \\ORDER BY
                    \\  global_position {s}
                    \\LIMIT
                    \\  ?;
                ;

                const sql = try std.fmt.allocPrint(allocator, sql_format, .{ direction_evaluator, direction });
                var stmt = self.db.prepareDynamic(sql) catch |serr| {
                    const stmt_error = self.db.getDetailedError();
                    logger.err("Unable to prepare statement for Start position in category ({s}), error: {}, message: {s}", .{ category_name, serr, stmt_error });
                    return serr;
                };
                defer stmt.deinit();

                return stmt.all(
                    RecordedEvent,
                    allocator,
                    .{},
                    .{ category_name, category_name, limit },
                ) catch |err| {
                    const query_error = self.db.getDetailedError();
                    logger.err("Unable to retrieve events for Start position of category ({s}), error: {}, message: {s}", .{ category_name, err, query_error });
                    return err;
                };
            },
            .End => {
                const sql_format =
                    \\SELECT
                    \\  id,
                    \\  stream_name,
                    \\  event_type,
                    \\  event_data,
                    \\  metadata,
                    \\  global_position,
                    \\  position,
                    \\  created_at
                    \\FROM
                    \\  _stremes_events
                    \\WHERE
                    \\  substr(stream_name, 0, instr(stream_name, '-')) = ?
                    \\AND
                    \\  global_position {s} (
                    \\    SELECT
                    \\      MAX(COALESCE(global_position, 0))
                    \\    FROM
                    \\      _stremes_events
                    \\    WHERE
                    \\      substr(stream_name, 0, instr(stream_name, '-')) = ?
                    \\  )
                    \\ORDER BY
                    \\  global_position {s}
                    \\LIMIT
                    \\  ?;
                ;

                const sql: []u8 = try std.fmt.allocPrint(allocator, sql_format, .{ direction_evaluator, direction });
                var stmt = self.db.prepareDynamic(sql) catch |serr| {
                    const stmt_error = self.db.getDetailedError();
                    logger.err("Unable to prepare statement for End position in category ({s}), error: {}, message: {s}", .{ category_name, serr, stmt_error });
                    return serr;
                };
                defer stmt.deinit();

                return stmt.all(
                    RecordedEvent,
                    allocator,
                    .{},
                    .{ category_name, category_name, limit },
                ) catch |err| {
                    const query_error = self.db.getDetailedError();
                    logger.err("Unable to retrieve events for End position of category ({s}), error: {}, message: {s}", .{ category_name, err, query_error });
                    return err;
                };
            },
            .Position => |position| {
                if (position < 1) {
                    return StoreError.InvalidPosition;
                }

                const sql_format =
                    \\SELECT
                    \\  id,
                    \\  stream_name,
                    \\  event_type,
                    \\  event_data,
                    \\  metadata,
                    \\  global_position,
                    \\  position,
                    \\  created_at
                    \\FROM
                    \\  _stremes_events
                    \\WHERE
                    \\  substr(stream_name, 0, instr(stream_name, '-')) = ?
                    \\AND
                    \\  global_position {s} ?
                    \\ORDER BY
                    \\  global_position {s}
                    \\LIMIT
                    \\  ?;
                ;

                const sql: []u8 = try std.fmt.allocPrint(allocator, sql_format, .{ direction_evaluator, direction });
                var stmt = self.db.prepareDynamic(sql) catch |serr| {
                    const stmt_error = self.db.getDetailedError();
                    logger.err("Unable to prepare statement for Position position in category ({s}), error: {}, message: {s}", .{ category_name, serr, stmt_error });
                    return serr;
                };
                defer stmt.deinit();

                return stmt.all(
                    RecordedEvent,
                    allocator,
                    .{},
                    .{ category_name, position, limit },
                ) catch |err| {
                    const query_error = self.db.getDetailedError();
                    logger.err("Unable to retrieve events for Position position of category ({s}), error: {}, message: {s}", .{ category_name, err, query_error });
                    return err;
                };
            },
        }
    }

    fn categoryExists(self: *Self, category: []const u8) !?bool {
        const query = "select exists(select id from _stremes_events where substr(stream_name, 0, instr(stream_name, '-')) = $name)";
        return self.db.one(bool, query, .{}, .{ .name = category }) catch |err| {
            const stmt_error = self.db.getDetailedError();
            logger.err("Unable to verify if category ({s}) exists, error: {}, message: {s}", .{ category, err, stmt_error });
            return err;
        };
    }
};

fn isCategory(name: []const u8) bool {
    return std.mem.indexOfPosLinear(u8, name, 0, "-") == null;
}

const testing = std.testing;
const expectEqual = testing.expectEqual;
const expectEqualSlices = testing.expectEqualSlices;
const expectError = testing.expectError;
const helpers = @import("test_helpers.zig");
const StoreDb = helpers.StoreDb;

test "appending first event in a new db sets positions correctly" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    _ = try store.append("ACCT-99", events[0..], ExpectedVersion.Any);

    const record = try store_db.lastEvent(allocator, "ACCT-99");
    try expectEqual(@as(i64, 1), record.?.position);
    try expectEqual(@as(i64, 1), record.?.global_position);
}

test "appending event to an existing stream sets correct stream position" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const current_version = try store.append("ACCT-2", events[0..], ExpectedVersion.Any);
    try expectEqual(@as(i64, 3), current_version);
}

test "appending event to a new stream of an existing db sets stream position correctly" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const current_version = try store.append("ACCT-4", events[0..], ExpectedVersion.Any);
    try expectEqual(@as(i64, 1), current_version);
}

test "appending event with the correct expected position returns the correct current stream position" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const current_version = try store.append("ACCT-1", events[0..], .{ .Exact = 3 });
    try expectEqual(@as(i64, 4), current_version);
}

test "appending event with an incorrect expected position fails" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const result = store.append("ACCT-1", events[0..], .{ .Exact = 2 });
    try expectError(StoreError.WrongExpectedVersion, result);
}

test "appending event to new stream with expected version of NoStream succeeds" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const current_version = try store.append("ACCT-99", events[0..], ExpectedVersion.NoStream);
    try expectEqual(@as(i64, 1), current_version);
}

test "appending event to existing stream with expected version of NoStream fails" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const result = store.append("ACCT-1", events[0..], ExpectedVersion.NoStream);
    try expectError(StoreError.StreamAlreadyExists, result);
}

test "appending event to existing stream with expected version of StreamExists succeeds" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const current_version = try store.append("ACCT-1", events[0..], ExpectedVersion.StreamExists);
    try expectEqual(@as(i64, 4), current_version);
}

test "appending event to new stream with expected version of StreamExists fails" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const result = store.append("ACCT-99", events[0..], ExpectedVersion.StreamExists);
    try expectError(StoreError.StreamDoesNotExist, result);
}

test "appending event to new stream with expected version of Exact fails" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const result = store.append("ACCT-99", events[0..], .{ .Exact = 1 });
    try expectError(StoreError.StreamDoesNotExist, result);
}

test "appending event with expected version of less than one fails" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{.{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" }};

    const result = store.append("ACCT-99", events[0..], .{ .Exact = 0 });
    try expectError(StoreError.InvalidPosition, result);
}

test "appending multiple events returns the correct current stream position" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = [_]EventData{
        .{ .id = "id-99", .event_type = "E1", .event_data = "", .metadata = "" },
        .{ .id = "id-100", .event_type = "E2", .event_data = "", .metadata = "" },
        .{ .id = "id-101", .event_type = "E3", .event_data = "", .metadata = "" },
    };

    const current_version = try store.append("ACCT-2", events[0..], ExpectedVersion.StreamExists);
    try expectEqual(@as(i64, 5), current_version);
}

test "reading the version of a stream" {
    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    const version = try store.streamVersion("ACCT-2");
    try expectEqual(@as(i64, 2), version.?);
}

test "reading the last event of a stream" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    const event = try store.lastEvent(allocator, "ACCT-1");
    try expectEqualSlices(u8, "id-4", event.?.id);
}

test "reading stream events at start forwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_stream(allocator, "ACCT-1", .{});
    try expectEqual(@as(usize, 3), events.len);
    try expectEqualSlices(u8, "id-1", events[0].id);
    try expectEqualSlices(u8, "id-2", events[1].id);
    try expectEqualSlices(u8, "id-4", events[2].id);
}

test "reading stream events at start backwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_stream(allocator, "ACCT-1", .{ .direction = ReadDirection.Backward });
    try expectEqual(@as(usize, 1), events.len);
    try expectEqualSlices(u8, "id-1", events[0].id);
}

test "reading stream events at end backwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_stream(allocator, "ACCT-1", .{ .position = Position.End, .direction = ReadDirection.Backward });
    try expectEqual(@as(usize, 3), events.len);
    try expectEqualSlices(u8, "id-4", events[0].id);
    try expectEqualSlices(u8, "id-2", events[1].id);
    try expectEqualSlices(u8, "id-1", events[2].id);
}

test "reading stream events at end forwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_stream(allocator, "ACCT-1", .{ .position = Position.End });
    try expectEqual(@as(usize, 1), events.len);
    try expectEqualSlices(u8, "id-4", events[0].id);
}

test "reading stream events at position forwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_stream(allocator, "ACCT-1", .{ .position = .{ .Position = 2 } });
    try expectEqual(@as(usize, 2), events.len);
    try expectEqualSlices(u8, "id-2", events[0].id);
    try expectEqualSlices(u8, "id-4", events[1].id);
}

test "reading stream events at position backwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_stream(allocator, "ACCT-1", .{ .position = .{ .Position = 2 }, .direction = ReadDirection.Backward });
    try expectEqual(@as(usize, 2), events.len);
    try expectEqualSlices(u8, "id-2", events[0].id);
    try expectEqualSlices(u8, "id-1", events[1].id);
}

test "reading stream events with invalid position fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_stream(allocator, "ACCT-1", .{ .position = .{ .Position = 0 } });
    try expectError(StoreError.InvalidPosition, result);
}

test "reading stream events with limit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_stream(allocator, "ACCT-1", .{ .limit = .{ .Limit = 1 } });
    try expectEqual(@as(usize, 1), events.len);
    try expectEqualSlices(u8, "id-1", events[0].id);
}

test "reading stream events with invalid limit fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_stream(allocator, "ACCT-1", .{ .limit = .{ .Limit = -99 } });
    try expectError(StoreError.InvalidLimit, result);
}

test "reading stream events with category instead of stream name fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_stream(allocator, "ACCT", .{});
    try expectError(StoreError.MustBeStream, result);
}

test "reading stream events from stream that does not exist fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_stream(allocator, "ACCT-99", .{});
    try expectError(StoreError.StreamDoesNotExist, result);
}

test "reading category events at start forwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_category(allocator, "ACCT", .{});
    try expectEqual(@as(usize, 7), events.len);
    try expectEqualSlices(u8, "id-1", events[0].id);
    try expectEqualSlices(u8, "id-2", events[1].id);
    try expectEqualSlices(u8, "id-3", events[2].id);
    try expectEqualSlices(u8, "id-4", events[3].id);
    try expectEqualSlices(u8, "id-5", events[4].id);
    try expectEqualSlices(u8, "id-6", events[5].id);
    try expectEqualSlices(u8, "id-7", events[6].id);
}

test "reading category events at start backwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_category(allocator, "ACCT", .{ .direction = ReadDirection.Backward });
    try expectEqual(@as(usize, 1), events.len);
    try expectEqualSlices(u8, "id-1", events[0].id);
}

test "reading category events at end backwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_category(allocator, "ACCT", .{ .position = Position.End, .direction = ReadDirection.Backward });
    try expectEqual(@as(usize, 7), events.len);
    try expectEqualSlices(u8, "id-7", events[0].id);
    try expectEqualSlices(u8, "id-6", events[1].id);
    try expectEqualSlices(u8, "id-5", events[2].id);
    try expectEqualSlices(u8, "id-4", events[3].id);
    try expectEqualSlices(u8, "id-3", events[4].id);
    try expectEqualSlices(u8, "id-2", events[5].id);
    try expectEqualSlices(u8, "id-1", events[6].id);
}

test "reading category events at end forwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_category(allocator, "ACCT", .{ .position = Position.End });
    try expectEqual(@as(usize, 1), events.len);
    try expectEqualSlices(u8, "id-7", events[0].id);
}

test "reading category events at position forwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_category(allocator, "ACCT", .{ .position = .{ .Position = 6 } });
    try expectEqual(@as(usize, 2), events.len);
    try expectEqualSlices(u8, "id-6", events[0].id);
    try expectEqualSlices(u8, "id-7", events[1].id);
}

test "reading category events at position backwards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_category(allocator, "ACCT", .{ .position = .{ .Position = 5 }, .direction = ReadDirection.Backward });
    try expectEqual(@as(usize, 5), events.len);
    try expectEqualSlices(u8, "id-5", events[0].id);
    try expectEqualSlices(u8, "id-4", events[1].id);
    try expectEqualSlices(u8, "id-3", events[2].id);
    try expectEqualSlices(u8, "id-2", events[3].id);
    try expectEqualSlices(u8, "id-1", events[4].id);
}

test "reading category events with invalid position fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_category(allocator, "ACCT", .{ .position = .{ .Position = 0 } });
    try expectError(StoreError.InvalidPosition, result);
}

test "reading category events with limit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var events = try store.read_category(allocator, "ACCT", .{ .limit = .{ .Limit = 1 } });
    try expectEqual(@as(usize, 1), events.len);
    try expectEqualSlices(u8, "id-1", events[0].id);
}

test "reading category events with invalid limit fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_category(allocator, "ACCT", .{ .limit = .{ .Limit = -99 } });
    try expectError(StoreError.InvalidLimit, result);
}

test "reading category events with stream instead of category name fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_category(allocator, "ACCT-1", .{});
    try expectError(StoreError.MustBeCategory, result);
}

test "reading category events from category that does not exist fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    var store_db = try StoreDb.init();
    var store = Store.init(store_db.db);
    try store.migrate();
    try store_db.insert_default_fixtures();

    var result = store.read_category(allocator, "BANK", .{});
    try expectError(StoreError.CategoryDoesNotExist, result);
}
