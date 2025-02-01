CREATE TABLE holidays (
    date DATE PRIMARY KEY
);

INSERT INTO holidays (date) VALUES
    -- 2022
    ('2022-01-17'),  -- Martin Luther King Jr. Day
    ('2022-02-21'),  -- Washington's Birthday
    ('2022-04-15'),  -- Good Friday
    ('2022-05-30'),  -- Memorial Day
    ('2022-06-20'),  -- Juneteenth (observed)
    ('2022-07-04'),  -- Independence Day
    ('2022-09-05'),  -- Labor Day
    ('2022-11-24'),  -- Thanksgiving Day
    ('2022-12-26'),  -- Christmas Day (observed)

    -- 2023
    ('2023-01-02'),  -- New Year's Day
    ('2023-01-16'),  -- Martin Luther King Jr. Day
    ('2023-02-20'),  -- Washington's Birthday
    ('2023-04-07'),  -- Good Friday
    ('2023-05-29'),  -- Memorial Day
    ('2023-06-19'),  -- Juneteenth
    ('2023-07-04'),  -- Independence Day
    ('2023-09-04'),  -- Labor Day
    ('2023-11-23'),  -- Thanksgiving Day
    ('2023-12-25'),  -- Christmas Day

    -- 2024
    ('2024-01-01'),  -- New Year's Day
    ('2024-01-15'),  -- Martin Luther King Jr. Day
    ('2024-02-19'),  -- Washington's Birthday
    ('2024-03-29'),  -- Good Friday
    ('2024-05-27'),  -- Memorial Day
    ('2024-06-19'),  -- Juneteenth
    ('2024-07-04'),  -- Independence Day
    ('2024-09-02'),  -- Labor Day
    ('2024-11-28'),  -- Thanksgiving Day
    ('2024-12-25');  -- Christmas Day

CREATE TABLE bars (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    volume FLOAT,
    vwap DECIMAL(10,4),
    open DECIMAL(10,4),
    close DECIMAL(10,4),
    high DECIMAL(10,4),
    low DECIMAL(10,4),
    trade_count INT,
    datetime_utc TIMESTAMP WITH TIME ZONE,
    month INT,
    week INT
);

CREATE UNIQUE INDEX idx_bars_symbol_datetime ON bars(symbol, datetime_utc);