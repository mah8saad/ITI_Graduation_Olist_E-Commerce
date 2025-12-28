--CREATE DATABASE olist_database;
-----------------------------------------------------------
DROP TABLE IF EXISTS public.product;

CREATE TABLE public.product
(
 product_id VARCHAR(50) PRIMARY KEY,
 product_category_name VARCHAR(50),
 product_name_lenght INTEGER,
 product_description_lenght INTEGER,
 product_photos_qty INTEGER,
 product_weight_g DOUBLE PRECISION,
 product_length_cm DOUBLE PRECISION,
 product_height_cm DOUBLE PRECISION,
 product_width_cm DOUBLE PRECISION
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.geolocation;

CREATE TABLE public.geolocation
(
 geolocation_zip_code_prefix INTEGER,
 geolocation_lat DOUBLE PRECISION,
 geolocation_lng DOUBLE PRECISION,
 geolocation_city VARCHAR(50),
 geolocation_state VARCHAR(5)
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.customer;

CREATE TABLE public.customer
(
 customer_id VARCHAR(50) PRIMARY KEY,
 customer_unique_id VARCHAR(50),
 customer_zip_code_prefix INTEGER,
 customer_city VARCHAR(50),
 customer_state VARCHAR(5)
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.sellers;

CREATE TABLE public.sellers
(
 seller_id VARCHAR(50) PRIMARY KEY,
 seller_zip_code_prefix INTEGER,
 seller_city VARCHAR(50),
 seller_state VARCHAR(5)
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.orders;

CREATE TABLE public.orders
(
 order_id VARCHAR(50) PRIMARY KEY,
 customer_id VARCHAR(50),
 order_status VARCHAR(50),
 order_purchase_timestamp TIMESTAMP,
 order_approved_at TIMESTAMP,
 order_delivered_carrier_date TIMESTAMP,
 order_delivered_customer_date TIMESTAMP,
 order_estimated_delivery_date TIMESTAMP,
 CONSTRAINT fk_customer_orders
     FOREIGN KEY (customer_id)
     REFERENCES public.customer(customer_id)
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.order_items;

CREATE TABLE public.order_items
(
 order_id VARCHAR(50),
 order_item_id INTEGER,
 product_id VARCHAR(50),
 seller_id VARCHAR(50),
 shipping_limit_date TIMESTAMP,
 price DOUBLE PRECISION,
 freight_value DOUBLE PRECISION,
 CONSTRAINT fk_orders_order_items
    FOREIGN KEY (order_id)
    REFERENCES public.orders(order_id),
 CONSTRAINT fk_product_order_items
    FOREIGN KEY (product_id)
    REFERENCES public.product(product_id),
 CONSTRAINT fk_sellers_order_items
    FOREIGN KEY (seller_id)
    REFERENCES public.sellers(seller_id)	
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.reviews;

CREATE TABLE public.reviews
(
 review_id VARCHAR(50),
 order_id VARCHAR(50),
 review_score INTEGER,
 review_comment_title VARCHAR(50),
 review_comment_massage VARCHAR(250),
 review_creation_date TIMESTAMP,
 review_answer_timestamp TIMESTAMP,
 CONSTRAINT fk_orders_reviews
    FOREIGN KEY (order_id)
    REFERENCES public.orders(order_id)
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.payment;

CREATE TABLE public.payment
(
 order_id VARCHAR(50),
 payment_sequential INTEGER,
 payment_type VARCHAR(50),
 payment_installments INTEGER,
 payment_value DOUBLE PRECISION,
 CONSTRAINT fk_orders_payment
    FOREIGN KEY (order_id)
    REFERENCES public.orders(order_id) 
);
-----------------------------------------------------------
DROP TABLE IF EXISTS public.product_category_name;

CREATE TABLE public.product_category_name
(
 product_category_name VARCHAR(50),
 product_category_name_english VARCHAR(50)
);