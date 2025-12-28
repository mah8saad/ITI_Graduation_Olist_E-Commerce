
{{
    config(materialized = 'incremental')
}}
select
    Sellers_SK,
    SELLER_ID,
    Zip_Code,
    City,
    State,
    batch_date,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is not null then  0 else 1 end as Is_Available
from {{ ref('Sellers_snapshot') }}


{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}
