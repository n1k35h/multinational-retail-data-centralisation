/* The below query gives data in Germany on which store type has the most sales */

WITH german_store_sales AS (
	SELECT *
	FROM orders_table AS ot
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN dim_store_details AS dsd ON dsd.store_code = ot.store_code)
		SELECT 
			SUM(product_price * product_quantity) AS total_sales,
			store_type,
			country_code
		FROM german_store_sales
		WHERE country_code = 'DE'
		GROUP BY store_type, country_code
		ORDER BY total_sales ASC;