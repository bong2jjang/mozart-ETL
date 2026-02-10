{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_sample', 'customers') }}
),

renamed as (
    select
        id as customer_id,
        *
    from source
)

select * from renamed
