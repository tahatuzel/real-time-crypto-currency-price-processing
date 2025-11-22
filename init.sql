CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;


CREATE TABLE IF NOT EXISTS staging.raw_prices (
    event_time TIMESTAMP,
    symbol VARCHAR(10),
    price_usd DECIMAL(15,5),
	old_price_usd DECIMAL(15,5),
	exchange_rate DECIMAL(10,5)
);


CREATE TABLE IF NOT EXISTS analytics.daily_summary_dw (
	event_date DATE,

    symbol VARCHAR(10),

    average_price_usd_in_24h DECIMAL(15,5),
    max_price_usd_in_24h DECIMAL(15,5),
    min_price_usd_in_24h DECIMAL(15,5),

    open_price_usd DECIMAL(15,5),
    close_price_usd DECIMAL(15,5),

    CONSTRAINT daily_summary_pk PRIMARY KEY (event_date, symbol)
);


CREATE OR REPLACE PROCEDURE analytics.get_daily_summary()
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO analytics.daily_summary_dw  (
		event_date,
		symbol, 
		average_price_usd_in_24h, 
		max_price_usd_in_24h, 
		min_price_usd_in_24h, 
		open_price_usd, 
		close_price_usd
	)
	WITH extracted_date AS
	(
		SELECT
			event_time,
			DATE(event_time) AS event_date,
			symbol,
			price_usd
		FROM staging.raw_prices
		WHERE event_time >= CURRENT_DATE - INTERVAL '1 day' AND event_time < CURRENT_DATE
	)
	,with_open_and_close_prices AS
	(
		SELECT 
			event_date,
			FIRST_VALUE(price_usd) OVER (PARTITION BY event_date,symbol ORDER BY event_time ASC) as open_price_usd,
			FIRST_VALUE(price_usd) OVER (PARTITION BY event_date,symbol ORDER BY event_time DESC) as close_price_usd,
			symbol,
			price_usd
		FROM extracted_date
	)
	SELECT 
		event_date,
		symbol,
		AVG(price_usd) as average_price_usd_in_24h,
		MAX(price_usd) as max_price_usd_in_24h,
		MIN(price_usd) as min_price_usd_in_24h,
		open_price_usd,
		close_price_usd
	FROM with_open_and_close_prices
	GROUP BY(event_date,symbol,open_price_usd,close_price_usd)


	ON CONFLICT (event_date, symbol) 
    DO UPDATE SET
        average_price_usd_in_24h = EXCLUDED.average_price_usd_in_24h,
        max_price_usd_in_24h = EXCLUDED.max_price_usd_in_24h,
        min_price_usd_in_24h = EXCLUDED.min_price_usd_in_24h,
        open_price_usd = EXCLUDED.open_price_usd,
        close_price_usd = EXCLUDED.close_price_usd;


	DELETE FROM staging.raw_prices
	WHERE event_time < NOW() - INTERVAL '7 days';
END;
$$;