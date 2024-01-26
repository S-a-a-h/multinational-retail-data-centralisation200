SELECT country_code, 
COUNT (*) AS Number
FROM dim_store_details
GROUP BY country_code
ORDER BY country_code
LIMIT 7;

"DE"	141
"GB"	266
"US"	34


-------


SELECT locality, 
COUNT (*) AS Number 
FROM dim_store_details
GROUP BY locality
ORDER BY Number DESC
LIMIT 7;

"Chapletown"	14
"Belper"	13
"Bushey"	12
"Exeter"	11
"Arbroath"	10
"High Wycombe"	10
"Rutherglen"	10


-------


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

"8"	673295.68
"1"	668041.45
"10"	657335.84
"5"	650321.43
"7"	645741.70
"3"	645463.00


-------


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

93166	374047	"Offline"
26957	107739	"Web"


-------


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

93166	374047	"Offline"
26957	107739	"Web"


-------


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
	
"Local"			3440896.52	44.56
"Web Portal"	1726547.05	22.36
"Super Store"	1224293.65	15.85
"Mall Kiosk"	698791.61	9.05
"Outlet"		631804.81	8.18


-------


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

"1994"	"3"		27936.77
"2019"	"1"		27356.14
"2009"	"8"		27091.67
"1997"	"11"	26679.98
"2018"	"12"	26310.97
"2019"	"8"		26277.72
"2017"	"9"		26236.67
"2010"	"5"		25798.12
"1996"	"8"		25648.29
"2000"	"1"		25614.54


-------


SELECT 
	country_code, 
	SUM(staff_numbers) AS total_staff_numbers
FROM 
	dim_store_details
GROUP BY 
	country_code;

"US"	1304
"GB"	13132
"DE"	6054


-------


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

"DE"	"Outlet"		198373.57
"DE"	"Mall Kiosk"	247634.20
"DE"	"Super Store"	384625.03
"DE"	"Local"			1109909.59


-------


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


