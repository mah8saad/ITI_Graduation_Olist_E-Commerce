{% snapshot customer_snapshot %}

{{ config(
    target_schema='snapshots',
    unique_key='CUSTOMER_UNIQUE_ID',
    strategy='check',
    check_cols=['Zip_Code','City','State'],
    invalidate_hard_deletes=True
) }}

SELECT
   *
FROM {{ ref('Customers') }}

{% endsnapshot %}