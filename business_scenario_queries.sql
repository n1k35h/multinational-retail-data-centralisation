/* Milestone 4:
1. The below query will give the Operations team the information on which countries that are currently
operating and which country has the most stores. */

SELECT country_code AS country,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;

/* 2. The below query will give the business stakeholders the information on which location has the
most stores. */

SELECT locality,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

/* 3. Below query is showing the average highest monthly cost  */

WITH highest_avg_monthly_sales AS(
	SELECT *
		FROM dim_date_times AS ddt
	INNER JOIN
		orders_table AS ot ON ot.date_uuid = ddt.date_uuid
	INNER JOIN 
		dim_products AS dp ON dp.product_code = ot.product_code)
		SELECT			
			ROUND(CAST(SUM(product_quantity*product_price)AS NUMERIC), 2) AS total_sales,
			month,
			ROUND(CAST(AVG(product_quantity*product_price)AS NUMERIC), 2) AS avg_per_month
		FROM highest_avg_monthly_sales
		GROUP BY month
		ORDER BY total_sales DESC;

/* 4. The below query gives the company the information on how sales have been made online vs offline */

ALTER TABLE dim_store_details
	ADD location VARCHAR(11);

UPDATE dim_store_details
	SET location = 'Web'
	WHERE store_type = 'Web Portal';

UPDATE dim_store_details
	SET location = 'Offline'
	WHERE store_type != 'Web Portal';

WITH online_sales AS(
	SELECT * 
		FROM dim_store_details AS dsd
		INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code)
		SELECT COUNT(date_uuid) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count,
			location
		FROM online_sales
		GROUP BY location
		ORDER BY number_of_sales DESC;

-- select * from dim_store_details;

/* 5. The below query gives the percentage of sales that have come through each type of sales */

WITH sales_order AS (
	SELECT *
	FROM dim_store_details AS dsd
	INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code)
		SELECT store_type,
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
			ROUND(CAST(SUM(product_price * product_quantity) * 100.00 / 
			(SELECT SUM(product_price * product_quantity) FROM sales_order)AS NUMERIC), 2) AS percentage_total
		FROM sales_order
		GROUP BY store_type
		ORDER BY percentage_total DESC;

/* 6. The below query gives data on the highest month of cost sales in each year */

WITH highest_month_sales_per_year AS(
	SELECT * 
	FROM orders_table AS ot
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN dim_date_times AS ddt ON ddt.date_uuid = ot.date_uuid)
		SELECT 
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) AS total_sales,
			year,
			month
		FROM highest_month_sales_per_year
		GROUP BY year, month
		ORDER BY total_sales DESC;

/* 7. The below query gives data on overall head count in each country */

SELECT SUM(staff_numbers) AS total_staff_numbers,
	country_code
	FROM dim_store_details
	GROUP BY country_code
	ORDER BY total_staff_numbers DESC;

-- select * from dim_store_details;

/* 8. The below query gives data in Germany on which store type has the most sales */

WITH german_store_sales AS (
	SELECT *
	FROM orders_table AS ot
	INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN dim_store_details AS dsd ON dsd.store_code = ot.store_code)
		SELECT 
			ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
			store_type,
			country_code
		FROM german_store_sales
		WHERE country_code = 'DE'
		GROUP BY store_type, country_code
		ORDER BY total_sales ASC;

/* 9. Below query shows how quickly the company is making sales each year based on the average time 
taken between each sales per year */

WITH company_make_sales AS(
	SELECT TO_TIMESTAMP(CONCAT(day, '-', month, '-', year, ' ', timestamp), 'DD-MM-YYYY HH24:MI:SS') AS datetime,
	year
	FROM dim_date_times
	ORDER BY datetime DESC),
	company_make_sales_2 AS(
		SELECT 
			year,
			datetime,
			LEAD(datetime, 1) OVER (ORDER BY datetime DESC) AS time_elapse FROM company_make_sales)
			SELECT
				year,
				AVG((datetime - time_elapse)) AS actual_time_taken FROM company_make_sales_2
			GROUP BY year
			ORDER BY actual_time_taken DESC 
			limit 5;