SELECT 'Inserting data into: public.product';
	
COPY public.product
FROM 'C:\data\Brazilian E-Commerce\olist_products_dataset.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.geolocation';
	
COPY public.geolocation
FROM 'C:\data\Brazilian E-Commerce\olist_geolocation_dataset.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.customer';
	
COPY public.customer
FROM 'C:\data\Brazilian E-Commerce\olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.sellers';
	
COPY public.sellers
FROM 'C:\data\Brazilian E-Commerce\olist_sellers_dataset.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.orders';
	
COPY public.orders
FROM 'C:\data\Brazilian E-Commerce\olist_orders_dataset.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.order_items';
	
COPY public.order_items
FROM 'C:\data\Brazilian E-Commerce\olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.reviews';
	
COPY public.reviews
FROM 'C:\data\Brazilian E-Commerce\olist_order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER;
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.payment';
	
COPY public.payment
FROM 'C:\data\Brazilian E-Commerce\olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';
---------------------------------------------------------------------------
SELECT 'Inserting data into: public.product_category_name';
	
COPY public.product_category_name
FROM 'C:\data\Brazilian E-Commerce\product_category_name_translation.csv'
DELIMITER ','
CSV HEADER
encoding 'windows-1251';


