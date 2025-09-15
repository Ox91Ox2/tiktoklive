CREATE TABLE IF NOT EXISTS tiktoklive.events_raw
(
    live_id        String,
    event_time     DateTime64(3, 'UTC'),
    event_id       UUID,                         -- idempotent/unique
    user_id        String,
    user_name        String,
    event_type     LowCardinality(String),       -- JOIN|LEAVE|COMMENT|SHARE|LIKE|GIFT|HEARTBEAT...
    comment        String DEFAULT '',
    gift_value     UInt64 DEFAULT 0,             -- 0 nếu không phải gift
    payload_json   String CODEC(ZSTD)            -- JSON thô
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (live_id, event_time, event_id)
TTL event_time + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS tiktoklive.events_by_minute
(
    live_id    String,
    minute_ts  DateTime('UTC'),
    metric     LowCardinality(String),   -- JOIN|LEAVE|COMMENT|SHARE|LIKE|GIFT_COUNT|GIFT_VALUE
    value      UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(minute_ts)
ORDER BY (live_id, minute_ts, metric)
TTL minute_ts + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS tiktoklive.comments_by_live
(
    live_id     String,
    event_time  DateTime64(3, 'UTC'),
    event_id    UUID,
    user_id     String,
    comment     String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (live_id, event_time, event_id)
TTL event_time + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS tiktoklive.mv_comments_by_live
TO tiktoklive.comments_by_live
AS
SELECT
    live_id,
    event_time,
    event_id,
    user_id,
    comment
FROM tiktoklive.events_raw
WHERE event_type = 'COMMENT';


CREATE TABLE IF NOT EXISTS tiktoklive.dim_users
(
    user_id         String,                       -- "id"
    sec_uid         String,
    username        String,
    nick_name       String,

    avatar_uri      String,                       -- avatar_thumb.m_uri
    avatar_urls     Array(String),                -- avatar_thumb.m_urls

    following_count UInt64,                       -- follow_info.following_count (ép số khi insert)
    follower_count  UInt64,                       -- follow_info.follower_count

    user_honor      JSON,                         -- object tuỳ biến
    user_attr       JSON,                         -- object tuỳ biến
    raw             JSON,                         -- lưu nguyên bản ghi đầu vào (audit/debug)

    ver             UInt64,                       -- version để ReplacingMergeTree thay thế bản cũ
    updated_at      DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY user_id
SETTINGS index_granularity = 8192;