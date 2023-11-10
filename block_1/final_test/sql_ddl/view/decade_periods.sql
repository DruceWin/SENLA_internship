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