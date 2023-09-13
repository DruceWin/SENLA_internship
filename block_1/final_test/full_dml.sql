WITH data_period AS (SELECT 2018 AS year, 12 AS month)

, pdate_tran_art AS (
	SELECT DATE(DATE_TRUNC('MONTH', t_dat) + INTERVAL '1 MONTH - 1 DAY') AS part_date
		, customer_id
		, SUM(price) AS transaction_amount
		, COUNT(*) as number_of_articles
	FROM transactions_train
	CROSS JOIN data_period
	WHERE EXTRACT(YEAR FROM t_dat) = data_period.year AND EXTRACT(MONTH FROM t_dat) = data_period.month
	GROUP BY customer_id, part_date
)
, cstmr_age AS (
	SELECT customer_id,
		CASE 
			WHEN age < 23 THEN 'S'
			WHEN age >= 23 AND age <= 59 THEN 'A'
			WHEN age > 59 THEN 'R'
			ELSE NULL
		END AS customer_group_by_age
	FROM customers
)
, most_exp AS (
	SELECT DISTINCT customer_id, 	
		FIRST_VALUE(article_id) OVER (
			PARTITION BY customer_id
			ORDER BY price DESC
		) AS most_exp_article_id
	FROM transactions_train
	CROSS JOIN data_period
	WHERE EXTRACT(YEAR FROM t_dat) = data_period.year AND EXTRACT(MONTH FROM t_dat) = data_period.month
)
, prdct_grps AS (
	SELECT customer_id, 
		COUNT(DISTINCT product_group_name) AS number_of_product_groups
	FROM transactions_train
	JOIN articles ON transactions_train.article_id = articles.article_id
	CROSS JOIN data_period
	WHERE EXTRACT(YEAR FROM t_dat) = data_period.year AND EXTRACT(MONTH FROM t_dat) = data_period.month
	GROUP BY customer_id
)

, decade_periods AS (
	SELECT customer_id,
		SUM(CASE WHEN EXTRACT(DAY FROM t_dat) >= 1 AND EXTRACT(DAY FROM t_dat) <= 10 THEN price
			ELSE 0 END) AS period_1,
		SUM(CASE WHEN EXTRACT(DAY FROM t_dat) >= 11 AND EXTRACT(DAY FROM t_dat) <= 20 THEN price
			ELSE 0 END) AS period_2,
		SUM(CASE WHEN EXTRACT(DAY FROM t_dat) >= 21 AND EXTRACT(DAY FROM t_dat) <= 31 THEN price
			ELSE 0 END) AS period_3
	FROM public.transactions_train
	CROSS JOIN data_period
	WHERE EXTRACT(YEAR FROM t_dat) = data_period.year AND EXTRACT(MONTH FROM t_dat) = data_period.month
	GROUP BY customer_id
)
, active_decade AS (
	SELECT customer_id, 
		CASE
			WHEN period_1 >= period_2 AND period_1 >= period_3 THEN 1
			WHEN period_2 >= period_1 AND period_2 >= period_3 THEN 2
			WHEN period_3 >= period_1 AND period_3 >= period_2 THEN 3
		END AS most_active_decade
	FROM decade_periods
)
, loyalty_step_1 AS (
	SELECT customer_id
		, DATE(DATE_TRUNC('MONTH', t_dat)) AS active_month
		, MIN(t_dat) OVER (PARTITION BY customer_id) AS first_buy
	FROM public.transactions_train
	CROSS JOIN data_period
	WHERE t_dat < make_date(data_period.year, data_period.month, 1) + INTERVAL '1 MONTH'
	GROUP BY customer_id, active_month, t_dat
)
, loyalty_step_2 AS (
	SELECT customer_id
		, active_month
		, DATE(DATE_TRUNC('MONTH', first_buy)) AS first_month
		, COUNT(*) OVER (PARTITION BY customer_id) AS total_month
	FROM loyalty_step_1
	CROSS JOIN data_period
	WHERE active_month >= make_date(data_period.year, data_period.month, 1) - INTERVAL '2 MONTH' 
		AND active_month < make_date(data_period.year, data_period.month, 1) + INTERVAL '1 MONTH'
	GROUP BY customer_id, active_month, first_month
)
, loyalty_result AS (
	SELECT customer_id
		, CASE 
			WHEN total_month = 3 THEN 1
			WHEN first_month = make_date(data_period.year, data_period.month, 1) THEN 1
			WHEN total_month = 2 
				AND first_month = make_date(data_period.year, data_period.month - 1, 1) THEN 1
			ELSE 0
		END AS customer_loyalty 
	FROM loyalty_step_2	
	CROSS JOIN data_period	
	GROUP BY customer_id, customer_loyalty
)
SELECT part_date, pdate_tran_art.customer_id, customer_group_by_age, transaction_amount, most_exp_article_id 
	, number_of_articles, number_of_product_groups, most_active_decade, customer_loyalty
FROM pdate_tran_art
JOIN cstmr_age ON pdate_tran_art.customer_id = cstmr_age.customer_id
JOIN most_exp ON pdate_tran_art.customer_id = most_exp.customer_id
JOIN prdct_grps ON pdate_tran_art.customer_id = prdct_grps.customer_id
JOIN active_decade ON pdate_tran_art.customer_id = active_decade.customer_id
JOIN loyalty_result ON pdate_tran_art.customer_id = loyalty_result.customer_id
