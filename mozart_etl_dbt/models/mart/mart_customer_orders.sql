{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select
        c.customer_id,
        count(o.order_id) as total_orders
    from customers c
    left join orders o on c.customer_id = o.order_id
    group by c.customer_id
)

select * from customer_orders
