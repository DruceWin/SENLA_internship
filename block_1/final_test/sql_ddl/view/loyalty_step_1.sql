CREATE VIEW loyalty_step_1 AS 
	SELECT customer_id
		, DATE(DATE_TRUNC('MONTH', t_dat)) AS active_month
		, DATE(DATE_TRUNC('MONTH', (SELECT MIN(t_dat) FROM transactions_train))) AS first_month
		, COUNT(*) OVER (PARTITION BY customer_id) AS total_month
	FROM transactions_train
	WHERE t_dat >= make_date(2018, 10, 1) - INTERVAL '2 MONTH' 
		AND t_dat < make_date(2018, 10, 1) + INTERVAL '1 MONTH'
	GROUP BY customer_id, active_month, first_month