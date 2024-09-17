CREATE VIEW most_exp AS 
	SELECT DISTINCT customer_id, 	
		FIRST_VALUE(article_id) OVER (
			PARTITION BY customer_id
			ORDER BY price DESC
		) AS most_exp_article_id
	FROM transactions_train
	WHERE EXTRACT(YEAR FROM t_dat) = 2018 AND EXTRACT(MONTH FROM t_dat) = 10