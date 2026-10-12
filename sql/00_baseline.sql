-- Reference DDL before optimization; used only by scripts.benchmark.
-- It receives a snapshot of the already imported transactions, not a second Kafka read.
CREATE TABLE IF NOT EXISTS homework.transactions_baseline
(
    source_file String,
    source_row UInt64,
    transaction_time DateTime('UTC'),
    merch String,
    cat_id String,
    amount Decimal(12, 2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;
