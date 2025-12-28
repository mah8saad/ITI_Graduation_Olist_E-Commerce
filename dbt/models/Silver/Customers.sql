{{ config(materialized='incremental' ) }}

select 
    row_number() over(order by _AB_CDC_UPDATED_AT)  as Customer_SK,
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID ,
    cast (CUSTOMER_ZIP_CODE_PREFIX as string) as Zip_Code,
    initcap(trim(CUSTOMER_CITY)) as City,
    upper(trim(CUSTOMER_STATE)) as State,
    _AB_CDC_UPDATED_AT  as  batch_date
from {{source('Bronze','CUSTOMER')}}
   
{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}