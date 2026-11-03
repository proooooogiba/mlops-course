CREATE TABLE IF NOT EXISTS scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score BETWEEN 0 AND 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL,
    transaction_time TIMESTAMPTZ NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    written_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    us_state TEXT NOT NULL,
    merch TEXT NOT NULL,
    cat_id TEXT NOT NULL,
    amount NUMERIC(18,2) NOT NULL CHECK (amount >= 0)
);
CREATE INDEX IF NOT EXISTS scores_scored_at_idx ON scores (scored_at DESC);
CREATE INDEX IF NOT EXISTS scores_written_at_idx ON scores (written_at DESC);
CREATE INDEX IF NOT EXISTS scores_state_merch_time_idx ON scores (us_state, merch, written_at DESC);
