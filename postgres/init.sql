CREATE TABLE IF NOT EXISTS scores (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag INTEGER NOT NULL,
    us_state TEXT,
    merch TEXT,
    cat_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_scores_created_at ON scores(created_at);
CREATE INDEX IF NOT EXISTS idx_scores_fraud_flag ON scores(fraud_flag);
CREATE INDEX IF NOT EXISTS idx_scores_us_state ON scores(us_state);
CREATE INDEX IF NOT EXISTS idx_scores_merch ON scores(merch);
CREATE INDEX IF NOT EXISTS idx_scores_cat_id ON scores(cat_id);