CREATE VIEW prdct_grps AS 
	SELECT customer_id, 
		COUNT(DISTINCT product_group_name) AS number_of_product_groups
	FROM transactions_train
	JOIN articles ON transactions_train.article_id = articles.article_id
	WHERE EXTRACT(YEAR FROM t_dat) = 2018 AND EXTRACT(MONTH FROM t_dat) = 10
	GROUP BY customer_id