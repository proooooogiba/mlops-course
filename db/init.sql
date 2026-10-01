CREATE TABLE IF NOT EXISTS transaction_scores (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    transaction_id VARCHAR(256) NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag SMALLINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT transaction_scores_pkey PRIMARY KEY (id),
    CONSTRAINT transaction_scores_transaction_id_key UNIQUE (transaction_id),
    CONSTRAINT transaction_scores_score_check CHECK (score >= 0 AND score <= 1),
    CONSTRAINT transaction_scores_fraud_flag_check CHECK (fraud_flag IN (0, 1))
);

CREATE INDEX IF NOT EXISTS transaction_scores_fraud_recent
    ON transaction_scores (id DESC) WHERE fraud_flag = 1;
