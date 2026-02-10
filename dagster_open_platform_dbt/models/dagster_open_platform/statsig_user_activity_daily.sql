-- Stub model for local development
{{ config(materialized='incremental') }}
select null as id
