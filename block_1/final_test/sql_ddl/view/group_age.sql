CREATE VIEW cstmr_age AS 
	SELECT customer_id,
		CASE 
			WHEN age < 23 THEN 'S'
			WHEN age >= 23 AND age <= 59 THEN 'A'
			WHEN age > 59 THEN 'R'
			ELSE NULL
		END AS customer_group_by_age
	FROM customers