{{ config(materialized='table') }}

{{ union_tenants('cfg_item_master') }}
