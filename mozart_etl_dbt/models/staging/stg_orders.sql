{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_sample', 'orders') }}
),

renamed as (
    select
        order_id,
        *
    from source
)

select * from renamed
