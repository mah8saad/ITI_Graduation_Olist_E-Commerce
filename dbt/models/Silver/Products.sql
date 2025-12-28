{{
    config(materialized = 'incremental')
}}

with transalate as(
    select 
    product_name_en,
    PRODUCT_CATEGORY_NAME
    FROM {{ ref('product_category_name_translation') }}
)
select
    row_number() over(order by _AB_CDC_UPDATED_AT)  as Product_SK,
    PRODUCT_ID,
    trim(t.product_name_en)as Product_Name,
    PRODUCT_HEIGHT_CM ,
    PRODUCT_LENGTH_CM ,
    PRODUCT_WIDTH_CM,
     case
        when p.PRODUCT_WEIGHT_G< 1000 then 'small'
        when p.PRODUCT_WEIGHT_G between 1000 and 5000 then 'medium'
        else 'large'
    end as product_size,
    PRODUCT_PHOTOS_QTY,
    cast (_AB_CDC_UPDATED_AT as TIMESTAMP_NTZ(0) ) as  batch_date
from {{source('Bronze','PRODUCT')}} p left join transalate t
on t.PRODUCT_CATEGORY_NAME = p.PRODUCT_CATEGORY_NAME

{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}