/* Below query is showing the average highest monthly cost  */

WITH highest_avg_monthly_sales AS(
	SELECT *
		FROM dim_date_times AS ddt
	INNER JOIN
		orders_table AS ot ON ot.date_uuid = ddt.date_uuid
	INNER JOIN 
		dim_products AS dp ON dp.product_code = ot.product_code)
		SELECT
			month,
			SUM(product_quantity*product_price) AS total_sales,
			AVG(product_quantity*product_price) AS avg_per_month
		FROM highest_avg_monthly_sales
		GROUP BY month
		ORDER BY total_sales DESC;
