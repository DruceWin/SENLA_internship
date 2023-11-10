CREATE VIEW pdate_tran_art AS
	SELECT DATE(DATE_TRUNC('MONTH', t_dat) + INTERVAL '1 MONTH - 1 DAY') AS part_date
		, customer_id
		, SUM(price) AS transaction_amount
		, COUNT(*) as number_of_articles
	FROM transactions_train
	WHERE EXTRACT(YEAR FROM t_dat) = 2018 AND EXTRACT(MONTH FROM t_dat) = 10
	GROUP BY customer_id, part_date