{{
    config(materialized = 'incremental')
}}

select
    *
from {{ ref('Payment') }}
{% if is_incremental() %}
    WHERE batch_date > (SELECT MAX(batch_date) FROM {{ this }})
{% endif %}