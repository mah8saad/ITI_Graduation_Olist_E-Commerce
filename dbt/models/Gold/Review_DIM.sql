{{
    config(materialized = 'incremental')
}}

select  
    *
FROM {{ ref('review') }}

{% if is_incremental() %}
    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}