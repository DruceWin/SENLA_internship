SELECT part_date, pdate_tran_art.customer_id, customer_group_by_age, transaction_amount, most_exp_article_id 
	, number_of_articles, number_of_product_groups, most_active_decade, customer_loyalty
FROM pdate_tran_art
JOIN cstmr_age ON pdate_tran_art.customer_id = cstmr_age.customer_id
JOIN most_exp ON pdate_tran_art.customer_id = most_exp.customer_id
JOIN prdct_grps ON pdate_tran_art.customer_id = prdct_grps.customer_id
JOIN active_decade ON pdate_tran_art.customer_id = active_decade.customer_id
JOIN loyalty_result ON pdate_tran_art.customer_id = loyalty_result.customer_id
