{{
    config(materialized = 'incremental')
}}
select 
    row_number() over(order by _AIRBYTE_EXTRACTED_AT) as oreder_item_SK,
    ORDER_ID,
    ORDER_ITEM_ID,
    PRODUCT_ID,
    SELLER_ID,
    cast(SHIPPING_LIMIT_DATE as TIMESTAMP_NTZ(0) ) as SHIPPING_LIMIT_DATE,
    round(cast(PRICE as float), 2) as PRICE,
    round(cast(FREIGHT_VALUE as float), 2) as FREIGHT_VALUE,
    cast(_AIRBYTE_EXTRACTED_AT as TIMESTAMP_NTZ(0)) as  batch_date
from {{source('Bronze','ORDER_ITEMS')}}
{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}