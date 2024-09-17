CREATE VIEW active_decade AS 
	SELECT decade_periods.customer_id,
        CASE
            WHEN decade_periods.period_1 >= decade_periods.period_2 AND decade_periods.period_1 >= decade_periods.period_3 THEN 1
            WHEN decade_periods.period_2 >= decade_periods.period_1 AND decade_periods.period_2 >= decade_periods.period_3 THEN 2
            WHEN decade_periods.period_3 >= decade_periods.period_1 AND decade_periods.period_3 >= decade_periods.period_2 THEN 3
            ELSE NULL::integer
        END AS most_active_decade
   	FROM decade_periods;