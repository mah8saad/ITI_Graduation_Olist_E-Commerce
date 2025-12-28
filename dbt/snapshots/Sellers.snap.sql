{% snapshot Sellers_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='SELLER_ID',
    strategy='timestamp',
    updated_at='batch_date'

) }}

SELECT
   *
FROM {{ ref('Sellers') }}

{% endsnapshot %}