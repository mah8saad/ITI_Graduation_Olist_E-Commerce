{{
    config(materialized = 'incremental',
    unique_key ='ORDER_ID')
}}

with CTE_Order as (
    select 
        Order_SK,
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_STATUS,
        ORDER_PURCHASE_TIMESTAMP,
        ORDER_APPROVED_AT,
        ORDER_DELIVERED_CARRIER_DATE,
        ORDER_DELIVERED_CUSTOMER_DATE,
        ORDER_ESTIMATED_DELIVERY_DATE,
        DATEDIFF(day, ORDER_DELIVERED_CARRIER_DATE, ORDER_DELIVERED_CUSTOMER_DATE) as delivery_days,
        case
        when DATEDIFF(day, ORDER_DELIVERED_CARRIER_DATE, ORDER_DELIVERED_CUSTOMER_DATE) 
            > DATEDIFF(day, ORDER_DELIVERED_CARRIER_DATE, ORDER_ESTIMATED_DELIVERY_DATE)
        then 1 else 0 end as delayed ,
        batch_date
    from {{ ref('Olist_order') }}
    {% if is_incremental() %}
         where batch_date >= (select max(batch_date) from {{ this }})
    {% endif %}
    
)

select
    O.Order_SK,
    O.ORDER_ID,
    C.Customer_SK as Customer_FK ,
    P.Payment_SK  as Payment_FK,
    R.review_SK as REVIEW_FK,
    O.ORDER_STATUS as Delivery_Statuse,
    O.ORDER_PURCHASE_TIMESTAMP,
    O.ORDER_APPROVED_AT,
    O.ORDER_DELIVERED_CARRIER_DATE,
    O.ORDER_DELIVERED_CUSTOMER_DATE,
    O.ORDER_ESTIMATED_DELIVERY_DATE,
    O.delivery_days,
    O.delayed,
    R.REVIEW_SCORE,
    O.batch_date
from  CTE_Order O  left join {{ ref('Customer_DIM') }} C
on O.CUSTOMER_ID = C.CUSTOMER_ID
left join {{ ref('Payment_DIM') }} P
on O.ORDER_ID = P.ORDER_ID
left join {{ ref('Review_DIM') }} R
on O.ORDER_ID = R.ORDER_ID

