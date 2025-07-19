{{ config(materialized='view') }}

select
    customer_key,
    name as customer_name,
    address as customer_address,
    nation_key,
    phone_number,
    account_balance,
    market_segment,
    comment
from {{ ref('stg_tpch_customers') }}