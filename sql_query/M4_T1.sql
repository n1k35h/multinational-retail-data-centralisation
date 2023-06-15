/* The below query will give the Operations team the information on which countries that are currently
operating and which country has the most stores. */

SELECT country_code AS country,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC limit 3;

