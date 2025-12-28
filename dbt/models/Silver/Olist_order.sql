{{
    config(materialized = 'incremental')
}}
select
    row_number() over(order by _AB_CDC_UPDATED_AT) as Order_SK,
    ORDER_ID,
    CUSTOMER_ID,
    initcap(trim(ORDER_STATUS)) as ORDER_STATUS,
    cast(ORDER_PURCHASE_TIMESTAMP as TIMESTAMP_NTZ(0) ) as ORDER_PURCHASE_TIMESTAMP,
    cast (ORDER_APPROVED_AT as TIMESTAMP_NTZ(0) ) as ORDER_APPROVED_AT,
    cast (ORDER_DELIVERED_CARRIER_DATE as TIMESTAMP_NTZ(0) ) as ORDER_DELIVERED_CARRIER_DATE,
    cast (ORDER_DELIVERED_CUSTOMER_DATE as TIMESTAMP_NTZ(0) ) as ORDER_DELIVERED_CUSTOMER_DATE,
    cast (ORDER_ESTIMATED_DELIVERY_DATE as TIMESTAMP_NTZ(0) ) as ORDER_ESTIMATED_DELIVERY_DATE,
    cast (_AB_CDC_UPDATED_AT as TIMESTAMP_NTZ(0) ) as  batch_date
from {{ source('Bronze','ORDERS') }}
{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}