-- Merge states at query time: correct even before background merges finish.
SELECT
    us_state,
    tupleElement(best, 1) AS cat_id,
    tupleElement(best, 2) AS max_amount
FROM
(
    SELECT us_state, argMaxMerge(winner) AS best
    FROM homework.state_max
    GROUP BY us_state
)
ORDER BY us_state
FORMAT CSVWithNames;
