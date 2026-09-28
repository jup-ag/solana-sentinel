CREATE TABLE IF NOT EXISTS alerts (
    id        UUID,
    rule      LowCardinality(String),
    severity  LowCardinality(String),
    message   String,
    metadata    Map(LowCardinality(String), String),
    fired_at  DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(fired_at)
ORDER BY (fired_at, id)
TTL fired_at + INTERVAL 90 DAY;