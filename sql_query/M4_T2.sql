/* The below query will give the business stakeholders the information on which location has the
most stores. */

SELECT locality,
COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;