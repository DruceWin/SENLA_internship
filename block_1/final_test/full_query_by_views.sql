SELECT part_date, pdate_tran_art.customer_id, customer_group_by_age, transaction_amount, most_exp_article_id 
	, number_of_articles, number_of_product_groups, most_active_decade, customer_loyalty
FROM pdate_tran_art
JOIN cstmr_age ON pdate_tran_art.customer_id = cstmr_age.customer_id
JOIN most_exp ON pdate_tran_art.customer_id = most_exp.customer_id
JOIN prdct_grps ON pdate_tran_art.customer_id = prdct_grps.customer_id
JOIN active_decade ON pdate_tran_art.customer_id = active_decade.customer_id
JOIN loyalty_result ON pdate_tran_art.customer_id = loyalty_result.customer_id


CREATE VIEW pdate_tran_art AS
	SELECT DATE(DATE_TRUNC('MONTH', t_dat) + INTERVAL '1 MONTH - 1 DAY') AS part_date
		, customer_id
		, SUM(price) AS transaction_amount
		, COUNT(*) as number_of_articles
	FROM transactions_train
	WHERE EXTRACT(YEAR FROM t_dat) = 2018 AND EXTRACT(MONTH FROM t_dat) = 10
	GROUP BY customer_id, part_date
	

CREATE VIEW prdct_grps AS 
	SELECT customer_id, 
		COUNT(DISTINCT product_group_name) AS number_of_product_groups
	FROM transactions_train
	JOIN articles ON transactions_train.article_id = articles.article_id
	WHERE EXTRACT(YEAR FROM t_dat) = 2018 AND EXTRACT(MONTH FROM t_dat) = 10
	GROUP BY customer_id


CREATE VIEW most_exp AS 
	SELECT DISTINCT customer_id, 	
		FIRST_VALUE(article_id) OVER (
			PARTITION BY customer_id
			ORDER BY price DESC
		) AS most_exp_article_id
	FROM transactions_train
	WHERE EXTRACT(YEAR FROM t_dat) = 2018 AND EXTRACT(MONTH FROM t_dat) = 10


CREATE VIEW cstmr_age AS 
	SELECT customer_id,
		CASE 
			WHEN age < 23 THEN 'S'
			WHEN age >= 23 AND age <= 59 THEN 'A'
			WHEN age > 59 THEN 'R'
			ELSE NULL
		END AS customer_group_by_age
	FROM customers	


CREATE VIEW decade_periods AS 
		SELECT customer_id,
			SUM(CASE WHEN EXTRACT(DAY FROM t_dat) >= 1 AND EXTRACT(DAY FROM t_dat) <= 10 THEN price
				ELSE 0 END) AS period_1,
			SUM(CASE WHEN EXTRACT(DAY FROM t_dat) >= 11 AND EXTRACT(DAY FROM t_dat) <= 20 THEN price
				ELSE 0 END) AS period_2,
			SUM(CASE WHEN EXTRACT(DAY FROM t_dat) >= 21 AND EXTRACT(DAY FROM t_dat) <= 31 THEN price
				ELSE 0 END) AS period_3
		FROM transactions_train
		WHERE EXTRACT(YEAR FROM t_dat) = 2018 AND EXTRACT(MONTH FROM t_dat) = 10
		GROUP BY customer_id


CREATE VIEW active_decade AS 
	SELECT decade_periods.customer_id,
        CASE
            WHEN decade_periods.period_1 >= decade_periods.period_2 AND decade_periods.period_1 >= decade_periods.period_3 THEN 1
            WHEN decade_periods.period_2 >= decade_periods.period_1 AND decade_periods.period_2 >= decade_periods.period_3 THEN 2
            WHEN decade_periods.period_3 >= decade_periods.period_1 AND decade_periods.period_3 >= decade_periods.period_2 THEN 3
            ELSE NULL::integer
        END AS most_active_decade
   	FROM decade_periods;


CREATE VIEW loyalty_step_1 AS 
	SELECT customer_id
		, DATE(DATE_TRUNC('MONTH', t_dat)) AS active_month
		, DATE(DATE_TRUNC('MONTH', (SELECT MIN(t_dat) FROM transactions_train))) AS first_month
		, COUNT(*) OVER (PARTITION BY customer_id) AS total_month
	FROM transactions_train
	WHERE t_dat >= make_date(2018, 10, 1) - INTERVAL '2 MONTH' 
		AND t_dat < make_date(2018, 10, 1) + INTERVAL '1 MONTH'
	GROUP BY customer_id, active_month, first_month


CREATE VIEW loyalty_result AS 
	SELECT customer_id
		, CASE 
			WHEN total_month = 3 THEN 1
			WHEN first_month = make_date(2018, 10, 1) THEN 1
			WHEN total_month = 2 
				AND first_month = make_date(2018, 10 - 1, 1) THEN 1
			ELSE 0
		END AS customer_loyalty 
	FROM loyalty_step_1	
	GROUP BY customer_id, customer_loyalty