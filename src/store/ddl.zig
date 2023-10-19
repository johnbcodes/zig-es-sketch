pub const DDL = &[_][]const u8{
    \\CREATE TABLE IF NOT EXISTS _stremes_events (
    \\    id              TEXT    NOT NULL,
    \\    stream_name     TEXT    NOT NULL,
    \\    event_type      TEXT    NOT NULL,
    \\    event_data      TEXT    NOT NULL,
    \\    metadata        TEXT    NOT NULL,
    \\    global_position INTEGER NOT NULL,
    \\    position        INTEGER NOT NULL,
    \\    created_at      TEXT    NOT NULL
    \\) strict;
    \\
    \\CREATE UNIQUE INDEX _stremes_events_global_position_idx ON _stremes_events (global_position);
    \\CREATE UNIQUE INDEX _stremes_events_id_idx ON _stremes_events (id);
    \\CREATE UNIQUE INDEX _stremes_events_stream_name_idx ON _stremes_events (stream_name, position);
    \\CREATE INDEX _stremes_events_category ON _stremes_events (
    \\   substr(stream_name, 0, instr(stream_name, '-')),
    \\   global_position,
    \\   metadata->>'correlationStreamName'
    \\);
    ,
    \\CREATE VIEW IF NOT EXISTS stremes_type_summary AS
    \\  WITH
    \\    type_count AS (
    \\      SELECT
    \\        type,
    \\        COUNT(id) AS message_count
    \\      FROM
    \\        _stremes_events
    \\      GROUP BY
    \\        type
    \\    ),
    \\
    \\    total_count AS (
    \\      SELECT
    \\        COUNT(id) AS total_count
    \\      FROM
    \\        _stremes_events
    \\    )
    \\
    \\SELECT
    \\    type,
    \\    message_count,
    \\    ROUND(CAST((message_count / total_count) as REAL) * 100, 2) AS percent
    \\FROM
    \\    type_count,
    \\    total_count
    \\ORDER BY
    \\    type;
    ,
    \\CREATE VIEW IF NOT EXISTS stremes_category_type_summary AS
    \\  WITH
    \\    type_count AS (
    \\      SELECT
    \\        substr(stream_name, 0, instr(stream_name, '-')) AS category,
    \\        type,
    \\        COUNT(id) AS message_count
    \\      FROM
    \\        _stremes_events
    \\      GROUP BY
    \\        category,
    \\        type
    \\    ),
    \\
    \\    total_count AS (
    \\      SELECT
    \\        COUNT(id) AS total_count
    \\      FROM
    \\        _stremes_events
    \\    )
    \\
    \\SELECT
    \\  category,
    \\  type,
    \\  message_count,
    \\  ROUND(CAST((message_count / total_count) AS REAL) * 100, 2) AS percent
    \\FROM
    \\  type_count,
    \\  total_count
    \\ORDER BY
    \\  category,
    \\  type;
    ,
    \\CREATE VIEW IF NOT EXISTS stremes_stream_summary AS
    \\  WITH
    \\    stream_count AS (
    \\      SELECT
    \\        stream_name,
    \\        COUNT(id) AS message_count
    \\      FROM
    \\        _stremes_events
    \\      GROUP BY
    \\        stream_name
    \\    ),
    \\
    \\    total_count AS (
    \\      SELECT
    \\        COUNT(id) AS total_count
    \\      FROM
    \\        _stremes_events
    \\    )
    \\
    \\SELECT
    \\  stream_name,
    \\  message_count,
    \\  ROUND(CAST((message_count / total_count) AS REAL) * 100, 2) AS percent
    \\FROM
    \\  stream_count,
    \\  total_count
    \\ORDER BY
    \\  stream_name;
    ,
    \\CREATE VIEW IF NOT EXISTS stremes_stream_type_summary AS
    \\  WITH
    \\    type_count AS (
    \\      SELECT
    \\        stream_name,
    \\        type,
    \\        COUNT(id) AS message_count
    \\      FROM
    \\        _stremes_events
    \\      GROUP BY
    \\        stream_name,
    \\        type
    \\    ),
    \\
    \\    total_count AS (
    \\      SELECT
    \\        COUNT(id) AS total_count
    \\      FROM
    \\        _stremes_events
    \\    )
    \\
    \\SELECT
    \\  stream_name,
    \\  type,
    \\  message_count,
    \\  ROUND(CAST((message_count / total_count) AS REAL) * 100, 2) AS percent
    \\FROM
    \\  type_count,
    \\  total_count
    \\ORDER BY
    \\  stream_name,
    \\  type;
    ,
    \\CREATE VIEW IF NOT EXISTS stremes_type_category_summary AS
    \\  WITH
    \\    type_count AS (
    \\      SELECT
    \\        type,
    \\        substr(stream_name, 0, instr(stream_name, '-')) AS category,
    \\        COUNT(id) AS message_count
    \\      FROM
    \\        _stremes_events
    \\      GROUP BY
    \\        type,
    \\        category
    \\    ),
    \\
    \\    total_count AS (
    \\      SELECT
    \\        COUNT(id) AS total_count
    \\      FROM
    \\        _stremes_events
    \\    )
    \\
    \\SELECT
    \\  type,
    \\  category,
    \\  message_count,
    \\  ROUND(CAST((message_count / total_count) AS REAL) * 100, 2) AS percent
    \\FROM
    \\  type_count,
    \\  total_count
    \\ORDER BY
    \\  type,
    \\  category;
    ,
    \\CREATE VIEW IF NOT EXISTS stremes_type_stream_summary AS
    \\  WITH
    \\    type_count AS (
    \\      SELECT
    \\        type,
    \\        stream_name,
    \\        COUNT(id) AS message_count
    \\      FROM
    \\        _stremes_events
    \\      GROUP BY
    \\        type,
    \\        stream_name
    \\    ),
    \\
    \\    total_count AS (
    \\      SELECT
    \\        COUNT(id) AS total_count
    \\      FROM
    \\        _stremes_events
    \\    )
    \\
    \\SELECT
    \\  type,
    \\  stream_name,
    \\  message_count,
    \\  ROUND(CAST((message_count / total_count) AS REAL) * 100, 2) AS percent
    \\FROM
    \\  type_count,
    \\  total_count
    \\ORDER BY
    \\  type,
    \\  stream_name;
    ,
};
