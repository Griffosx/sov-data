-- Bull or Bear bars
WITH future_moves AS (
    SELECT
        b1.id,
        b1.symbol,
        b1.datetime_utc,
        b1.open as start_price,
        EXISTS (
            SELECT 1
            FROM bars b2
            WHERE b2.symbol = b1.symbol
                AND b2.datetime_utc > b1.datetime_utc
                AND b2.high >= (b1.open * 1.05)
        ) as reached_gain,
        EXISTS (
            SELECT 1
            FROM bars b2
            WHERE b2.symbol = b1.symbol
                AND b2.datetime_utc > b1.datetime_utc
                AND b2.low <= (b1.open * 0.95)
        ) as reached_loss,
        (
            SELECT MIN(b2.datetime_utc)
            FROM bars b2
            WHERE b2.symbol = b1.symbol
                AND b2.datetime_utc > b1.datetime_utc
                AND b2.high >= (b1.open * 1.05)
        ) as gain_timestamp,
        (
            SELECT MIN(b2.datetime_utc)
            FROM bars b2
            WHERE b2.symbol = b1.symbol
                AND b2.datetime_utc > b1.datetime_utc
                AND b2.low <= (b1.open * 0.95)
        ) as loss_timestamp
    FROM bars b1
    WHERE EXTRACT(HOUR FROM b1.datetime_utc AT TIME ZONE 'America/New_York') IN (10, 11, 12)
)
SELECT
    b.*,
    CASE
        WHEN reached_gain AND (
            NOT reached_loss
            OR (gain_timestamp < loss_timestamp)
        ) THEN true
        WHEN reached_loss THEN false
    END as bull
FROM bars b
JOIN future_moves fm ON b.id = fm.id
ORDER BY b.symbol, b.datetime_utc;
