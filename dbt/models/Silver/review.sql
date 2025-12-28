
{{
    config(materialized = 'incremental')
}}
SELECT
   row_number() over(order by _AIRBYTE_EXTRACTED_AT) as review_SK,
   REVIEW_ID,
   ORDER_ID,
   REVIEW_SCORE,
   cast(REVIEW_ANSWER_TIMESTAMP as TIMESTAMP_NTZ(0) ) as REVIEW_ANSWER_TIMESTAMP,
   cast (REVIEW_CREATION_DATE as TIMESTAMP_NTZ(0) ) as REVIEW_CREATION_DATE,
   cast (_AIRBYTE_EXTRACTED_AT as TIMESTAMP_NTZ(0) )  as load_date
from {{ source('Bronze','REVIEWS') }}
{% if is_incremental() %}
    WHERE load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}