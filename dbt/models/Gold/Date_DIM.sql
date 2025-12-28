{{ config(
    materialized='incremental',
    unique_key='full_date'
) }}

WITH all_timestamps AS (
    SELECT ORDER_PURCHASE_TIMESTAMP AS ts FROM {{ ref('Olist_order') }}
    UNION
    SELECT order_approved_at FROM {{ ref('Olist_order') }}
    UNION
    SELECT order_delivered_carrier_date FROM {{ ref('Olist_order') }}
    UNION
    SELECT order_delivered_customer_date FROM {{ ref('Olist_order') }}
    UNION
    SELECT order_estimated_delivery_date FROM {{ ref('Olist_order') }}
    UNION
    SELECT SHIPPING_LIMIT_DATE FROM {{ ref('order_item') }}
)

SELECT DISTINCT
    cast (ts as TIMESTAMP_NTZ(0) ) as full_date ,
    EXTRACT(YEAR FROM ts) AS year,
    EXTRACT(MONTH FROM ts) AS month,
    EXTRACT(DAY FROM ts) AS day,
    dayname(ts) AS day_name,
    monthname(ts) as month_name,
    quarter(ts) as quarter_number,
    CASE WHEN EXTRACT(DOW FROM ts) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
FROM all_timestamps
WHERE ts IS NOT NULL

{% if is_incremental() %}
  AND ts NOT IN (
      SELECT full_date FROM {{ this }}
  )
{% endif %}