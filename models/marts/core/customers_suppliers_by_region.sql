{{ config(materialized='view') }}

with customers as (
    select * from {{ ref('stg_tpch_customers') }}
),

suppliers as (
    select * from {{ ref('stg_tpch_suppliers') }}
),

nations as (
    select * from {{ ref('stg_tpch_nations') }}
),

regions as (
    select * from {{ ref('stg_tpch_regions') }}
),

customers_with_region as (
    select 
        c.customer_key,
        c.name as customer_name,
        c.address as customer_address,
        c.phone_number as customer_phone,
        c.account_balance as customer_account_balance,
        c.comment as customer_comment,
        n.name as customer_nation,
        n.nation_key as customer_nation_key,
        r.name as customer_region,
        r.region_key as customer_region_key
    from customers c
    left join nations n
        on c.nation_key = n.nation_key
    left join regions r
        on n.region_key = r.region_key
),

suppliers_with_region as (
    select 
        s.supplier_key,
        s.supplier_name,
        s.supplier_address,
        s.phone_number as supplier_phone,
        s.account_balance as supplier_account_balance,
        s.comment as supplier_comment,
        n.name as supplier_nation,
        n.nation_key as supplier_nation_key,
        r.region_key as supplier_region_key
    from suppliers s
    left join nations n
        on s.nation_key = n.nation_key
    left join regions r
        on n.region_key = r.region_key
)

select 
    c.customer_key,
    c.customer_name,
    c.customer_address,
    c.customer_phone,
    c.customer_account_balance,
    c.customer_nation,
    c.customer_nation_key,
    c.customer_region,
    c.customer_region_key,
    s.supplier_key,
    s.supplier_name,
    s.supplier_address,
    s.supplier_phone,
    s.supplier_account_balance,
    s.supplier_nation,
    s.supplier_nation_key,
    s.supplier_region_key
from customers_with_region c
cross join suppliers_with_region s
where c.customer_region_key = s.supplier_region_key