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