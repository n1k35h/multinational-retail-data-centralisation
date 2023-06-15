/* The below query gives the percentage of sales that have come through each type of sales */

WITH sales_order AS (
	SELECT *
	FROM dim_store_details AS dsd
	INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code)
		SELECT store_type,
			SUM(product_price * product_quantity) AS total_sales,
			SUM(product_price * product_quantity) * 100.00 / 
			(SELECT SUM(product_price * product_quantity) FROM sales_order) AS percentage_total
		FROM sales_order
		GROUP BY store_type
		ORDER BY percentage_total DESC limit 5;