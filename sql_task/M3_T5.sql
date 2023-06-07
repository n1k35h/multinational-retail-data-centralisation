/* Renamed the 'removed' column to 'still_available' and changed the columns to the required 
datat types.*/

-- SELECT * FROM dim_products;

ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
	ALTER COLUMN weight TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(18),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE UUID USING CAST(uuid as UUID);
-- 	ALTER COLUMN still_available TYPE BOOL USING still_available::boolean;

SELECT *,
	CASE
		WHEN weight < 2.0 THEN 'Light'
		WHEN weight >= 2.0 AND weight < 40.0 THEN 'Mid_Sized'
		WHEN weight >= 40.0 AND weight < 140.0 THEN 'Heavy'
		WHEN weight >= 140.0 THEN 'Truck_Required'
	END AS weight_class
FROM dim_products ORDER BY index asc;
