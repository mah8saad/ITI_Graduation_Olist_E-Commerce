{{
    config(materialized = 'incremental')
}}

with CTE_order_item as (
    select
        oreder_item_SK,
        ORDER_ID,
        ORDER_ITEM_ID,
        SELLER_ID,
        PRODUCT_ID,
        SHIPPING_LIMIT_DATE,
        PRICE,
        FREIGHT_VALUE,
        (PRICE + FREIGHT_VALUE) as Total_Item_Cost,
        batch_date
    from {{ ref('order_item')}}
    {% if is_incremental() %}
         where batch_date >= (select max(batch_date) from {{ this }})
    {% endif %}
)
select
    O.oreder_item_SK,
    O.ORDER_ID,
    F.Order_SK as Order_Fact_FK,
    O.ORDER_ITEM_ID,
    P.Product_SK as PRODUCT_FK,
    S.Sellers_SK as SELLER_FK,
    O.SHIPPING_LIMIT_DATE as SHIPPING_LIMIT_DATE_FK ,
    O.PRICE,
    O.FREIGHT_VALUE,
    O.Total_Item_Cost,
    o.batch_date

from CTE_order_item O join {{ref('Sellers_DIM')}} S 
on O.SELLER_ID= S.SELLER_ID join {{ref('Products_DIM')}} P
on O.PRODUCT_ID=P.PRODUCT_ID join {{ref ('Order_Fact')}} F
on O.ORDER_ID =F.ORDER_ID

