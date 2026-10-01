import psycopg

from src.domain.entities import ResultsSnapshot, SavedScore, ScoreResult

INSERT_SCORE = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""


class PostgresScoreWriter:
    def __init__(self, connection):
        self.connection = connection

    def save(self, result: ScoreResult) -> None:
        with self.connection.transaction():
            self.connection.execute(INSERT_SCORE, (result.transaction_id, result.score, result.fraud_flag))


class PostgresResultsReader:
    def __init__(self, database_url):
        self.database_url = database_url

    def read(self, fraud_limit: int, scores_limit: int) -> ResultsSnapshot:
        with psycopg.connect(self.database_url, connect_timeout=5) as conn:
            fraud = conn.execute("""SELECT transaction_id, score, fraud_flag, received_at
                FROM transaction_scores WHERE fraud_flag = 1 ORDER BY id DESC LIMIT %s""", (fraud_limit,)).fetchall()
            scores = conn.execute("SELECT score FROM transaction_scores ORDER BY id DESC LIMIT %s", (scores_limit,)).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM transaction_scores").fetchone()[0]
        return ResultsSnapshot(
            fraud_transactions=tuple(SavedScore(ScoreResult(tx_id, score, flag), received_at) for tx_id, score, flag, received_at in fraud),
            recent_scores=tuple(row[0] for row in scores),
            total=total,
        )
