{% snapshot product_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='PRODUCT_ID',
    strategy='timestamp',
    updated_at='batch_date'

) }}

SELECT
   *
FROM {{ ref('Products') }}

{% endsnapshot %}