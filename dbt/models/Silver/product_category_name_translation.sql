WITH clean AS (
    SELECT
        REPLACE(COALESCE(PRODUCT_CATEGORY_NAME_ENGLISH, ''), '_', ' ') AS product_name_en,
        PRODUCT_CATEGORY_NAME
    FROM {{ source('Bronze','PRODUCT_CATEGORY_NAME') }}
)

SELECT 
    INITCAP(TRIM(product_name_en)) AS product_name_en,
    PRODUCT_CATEGORY_NAME
FROM clean
