{{
    config(materialized = 'incremental')
}}
with cleaned_payments as (
    select *
    from {{ source('Bronze', 'PAYMENT') }}
    where payment_type != 'not_defined'
)

select
    row_number() over(order by _AIRBYTE_EXTRACTED_AT) as Payment_SK,
    ORDER_ID,
    cast(PAYMENT_SEQUENTIAL as int) as payment_seq,
    INITCAP((REPLACE(PAYMENT_TYPE, '_', ' '))) AS PAYMENT_TYPE,
    cast(PAYMENT_INSTALLMENTS as int) as installments,
    round(cast(PAYMENT_VALUE as float), 2) as payment_value,
    cast (_AIRBYTE_EXTRACTED_AT as TIMESTAMP_NTZ(0) ) as  batch_date,
from cleaned_payments
{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}