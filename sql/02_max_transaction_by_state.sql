-- Category of the largest SINGLE transaction, not the category with largest SUM.
-- For ties choose the greatest (source_file, source_row) lexicographically.
SELECT
    us_state,
    tupleElement(best, 1) AS cat_id,
    tupleElement(best, 2) AS max_amount
FROM
(
    SELECT
        us_state,
        argMax(
            tuple(toString(cat_id), amount),
            tuple(amount, toString(source_file), source_row)
        ) AS best
    FROM homework.transactions
    GROUP BY us_state
)
ORDER BY us_state
FORMAT CSVWithNames;
