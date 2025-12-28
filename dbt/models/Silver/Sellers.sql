{{
    config(materialized = 'incremental')
}}

select 
    row_number() over(order by _AB_CDC_UPDATED_AT) as Sellers_SK,
    SELLER_ID,
    cast (SELLER_ZIP_CODE_PREFIX as string) as Zip_Code,
    initcap(trim(SELLER_CITY)) as City,
    upper(trim(SELLER_STATE)) as State,
    cast (_AB_CDC_UPDATED_AT as TIMESTAMP_NTZ(0) ) as  batch_date
from {{source('Bronze','SELLERS')}}
{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}
