ALTER TABLE orders_table 
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;


ALTER TABLE dim_users 
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
	ALTER COLUMN join_date TYPE DATE USING join_date::date;


ALTER TABLE dim_store_details 
	ALTER COLUMN longitude TYPE FLOAT,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
	ALTER COLUMN store_type TYPE VARCHAR(255), 
	ALTER COLUMN latitude TYPE FLOAT,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);
	
	
UPDATE dim_store_details
SET locality = COALESCE(locality, 'N/A');


UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');


ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(14);


UPDATE dim_products
SET weight_class = 
    CASE 
        WHEN CAST("weight(kg)" AS numeric) < 2 THEN 'Light'
        WHEN CAST("weight(kg)" AS numeric) >= 2 AND CAST("weight(kg)" AS numeric) < 40 THEN 'Mid_Sized'
        WHEN CAST("weight(kg)" AS numeric) >= 40 AND CAST("weight(kg)" AS numeric) < 140 THEN 'Heavy'
        WHEN CAST("weight(kg)" AS numeric) >= 140 THEN 'Truck_Required'
    END;


ALTER TABLE dim_products 
RENAME COLUMN product_status TO still_available;


UPDATE dim_products
SET still_available = 
CASE 
	WHEN still_available = 'Still_avaliable' THEN true
    ELSE false
END;


ALTER TABLE dim_products 
	ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
	ALTER COLUMN "weight(kg)" TYPE FLOAT USING "weight(kg)"::double precision,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
	ALTER COLUMN "uuid" TYPE UUID USING uuid::UUID,
	ALTER COLUMN still_available TYPE BOOLEAN USING still_available::boolean,
	ALTER COLUMN weight_class TYPE VARCHAR(14);


ALTER TABLE dim_date_times
	ALTER COLUMN "month" TYPE VARCHAR(2),
	ALTER COLUMN "year" TYPE VARCHAR(4),
	ALTER COLUMN "day" TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;


ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(50),
	ALTER COLUMN expiry_date TYPE VARCHAR(20),
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;


ALTER TABLE dim_date_times 
ADD PRIMARY KEY(date_uuid); 

ALTER TABLE dim_products 
ADD PRIMARY KEY(product_code); 

ALTER TABLE dim_store_details 
ADD PRIMARY KEY(store_code); 

ALTER TABLE dim_users 
ADD PRIMARY KEY(user_uuid); 

ALTER TABLE dim_card_details 
ADD PRIMARY KEY(card_number);


ALTER TABLE orders_table
  ADD CONSTRAINT fk_orders_date_times 
  	FOREIGN KEY (date_uuid) 
		REFERENCES dim_date_times(date_uuid),	
  ADD CONSTRAINT fk_orders_store_details 
  	FOREIGN KEY (store_code) 
		REFERENCES dim_store_details(store_code),	
  ADD CONSTRAINT fk_orders_users 
  	FOREIGN KEY (user_uuid) 
		REFERENCES dim_users(user_uuid),
  ADD CONSTRAINT fk_orders_products 
  	FOREIGN KEY (product_code) 
		REFERENCES dim_products(product_code),
  ADD CONSTRAINT fk_orders_card_details 
  	FOREIGN KEY (card_number) 
		REFERENCES dim_card_details(card_number);

SELECT country_code, 
COUNT (*) AS Number
FROM dim_store_details
GROUP BY country_code
ORDER BY country_code
LIMIT 7;


SELECT locality, 
COUNT (*) AS Number 
FROM dim_store_details
GROUP BY locality
ORDER BY Number DESC
LIMIT 7;


SELECT 
    dim_date_times.month,
    ROUND(SUM(CAST(orders_table.product_quantity * dim_products.product_price AS NUMERIC)), 2) AS total_sales --ROUND does not work without casting the quantity and price to numeric data type!
--Round to 2dp -- Sum of quantity x price per product -- cast to numeric datatype to round to 2dp -- column name: total_sales
FROM 
    dim_date_times
JOIN 
    orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
--date_uuid column connects these tables
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
--product_code column connects these tables
GROUP BY 
    dim_date_times.month
--groups the total_sales per month 
ORDER BY 
    total_sales DESC
LIMIT 6;


SELECT 
	COUNT(*) AS number_of_sales,
	SUM(orders_table.product_quantity) AS product_quantity_count,
    CASE WHEN 
		orders_table.store_code = 'WEB-1388012W' 
		THEN 'Web' 
		ELSE 'Offline' 
		END AS store_category
FROM 
    orders_table
GROUP BY 
    store_category;


SELECT 
	COUNT(*) AS number_of_sales,
	SUM(orders_table.product_quantity) AS product_quantity_count,
    CASE WHEN 
		orders_table.store_code = 'WEB-1388012W' 
		THEN 'Web' 
		ELSE 'Offline' 
		END AS store_category
FROM 
    orders_table
GROUP BY 
    store_category;


SELECT * FROM dim_store_details; --store_type


SELECT store_type FROM dim_store_details
GROUP BY store_type
ORDER BY store_type;
	
	
SELECT 
    dim_store_details.store_type,
    ROUND(SUM(CAST(orders_table.product_quantity * dim_products.product_price AS NUMERIC)), 2) AS total_sales,
    ROUND(SUM(CAST(orders_table.product_quantity * dim_products.product_price AS NUMERIC)) / 
           SUM(SUM(CAST(orders_table.product_quantity * dim_products.product_price AS NUMERIC))) OVER () * 100, 2) AS "percentage_total(%)"
FROM 
    dim_store_details
JOIN 
    orders_table ON orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    dim_store_details.store_type
ORDER BY 
    total_sales DESC;


SELECT "year", "month" 
FROM dim_date_times
GROUP BY "year", "month" 
ORDER BY "year", "month";


SELECT 
    dim_date_times.year, 
	dim_date_times.month,
    SUM(CAST(orders_table.product_quantity * dim_products.product_price AS NUMERIC)) AS total_sales
FROM 
	dim_date_times
JOIN 
    orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    dim_date_times.year, dim_date_times.month
ORDER BY 
    total_sales DESC
LIMIT 10;


SELECT 
	country_code, 
	SUM(staff_numbers) AS total_staff_numbers
FROM 
	dim_store_details
GROUP BY 
	country_code;


SELECT 
	dim_store_details.country_code,
    dim_store_details.store_type,
    ROUND(SUM(CAST(orders_table.product_quantity * dim_products.product_price AS NUMERIC)), 2) AS total_sales
FROM 
    dim_store_details
JOIN 
    orders_table ON orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
WHERE 
	dim_store_details.country_code = 'DE' AND dim_store_details.store_type != 'Web Portal'
GROUP BY 
    dim_store_details.store_type,
	dim_store_details.country_code
ORDER BY 
    total_sales;


--specified years:
WITH cte AS 
(
    SELECT 
        dim_date_times.year,
        dim_date_times.timestamp,
        LEAD(dim_date_times.timestamp, 1) OVER (PARTITION BY dim_date_times.year ORDER BY dim_date_times.timestamp) as next_timestamp
    FROM 
        dim_date_times
    WHERE
        dim_date_times.year IN ('1993', '2002', '2008', '2013', '2022')
)

SELECT
    cte.year,
	CONCAT(
    '"hours": ', AVG(EXTRACT(HOUR FROM cte.next_timestamp - cte.timestamp)), ', ',
    '"minutes": ', AVG(EXTRACT(MINUTE FROM cte.next_timestamp - cte.timestamp)), ', ',
    '"secoonds": ', AVG(EXTRACT(SECOND FROM cte.next_timestamp - cte.timestamp)), ', ',
    '"milliseconds": ', AVG(EXTRACT(MILLISECOND FROM cte.next_timestamp - cte.timestamp))
		) as actual_time_taken
FROM
    cte
WHERE
    cte.next_timestamp IS NOT NULL
GROUP BY 
	cte.year;


--every year since company opened:
WITH cte AS 
(
    SELECT 
        dim_date_times.year,
        dim_date_times.timestamp,
        LEAD(dim_date_times.timestamp, 1) OVER (PARTITION BY dim_date_times.year ORDER BY dim_date_times.timestamp) as next_timestamp
    FROM 
        dim_date_times
)

SELECT
    cte.year,
	CONCAT(
        '"hours": ', ROUND(AVG(EXTRACT(HOUR FROM cte.next_timestamp - cte.timestamp))),
        ', "minutes": ', ROUND(AVG(EXTRACT(MINUTE FROM cte.next_timestamp - cte.timestamp))),
        ', "seconds": ', ROUND(AVG(EXTRACT(SECOND FROM cte.next_timestamp - cte.timestamp))),
        ', "milliseconds": ', ROUND(AVG(EXTRACT(MILLISECOND FROM cte.next_timestamp - cte.timestamp)))
    ) as actual_time_taken
FROM
    cte
WHERE
    cte.next_timestamp IS NOT NULL
GROUP BY 
    cte.year;


