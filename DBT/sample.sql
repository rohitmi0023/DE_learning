-- Staging model for customers - basic cleaning and standardization
{{ config(materialized='view') }}

select
    customer_id,
    lower(trim(email)) as email,
    initcap(trim(first_name)) as first_name,
    initcap(trim(last_name)) as last_name,
    created_at,
    updated_at
    
from {{ source('raw_data', 'customers') }}


-- Staging model for orders
{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    order_date,
    lower(trim(status)) as status,
    created_at
    
from {{ source('raw_data', 'orders') }}

where order_date is not null


-- Staging model for products
{{ config(materialized='view') }}

select
    product_id,
    trim(product_name) as product_name,
    category_id,
    price,
    created_at
    
from {{ source('raw_data', 'products') }}

-- Intermediate model joining order items with product information
{{ config(materialized='ephemeral') }}

select
    oi.order_id,
    oi.product_id,
    p.product_name,
    p.category_id,
    oi.quantity,
    oi.price as unit_price,
    (oi.quantity * oi.price) as line_total
    
from {{ source('raw_data', 'order_items') }} oi
left join {{ ref('stg_products') }} p
    on oi.product_id = p.product_id



-- Customer dimension with calculated metrics
{{ config(materialized='table') }}

with customer_orders as (
    select
        customer_id,
        count(*) as lifetime_orders,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.created_at,
    coalesce(co.lifetime_orders, 0) as customer_lifetime_orders,
    co.first_order_date,
    co.most_recent_order_date,
    case 
        when co.first_order_date is null then 'prospect'
        when co.most_recent_order_date < current_date - 90 then 'inactive'
        else 'active'
    end as customer_status
    
from {{ ref('stg_customers') }} c
left join customer_orders co
    on c.customer_id = co.customer_id


-- Orders fact table with calculated totals
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    sum(oi.line_total) as order_total,
    count(oi.product_id) as total_items,
    current_timestamp as dbt_updated_at
    
from {{ ref('stg_orders') }} o
left join {{ ref('int_order_items_joined') }} oi
    on o.order_id = oi.order_id

{% if is_incremental() %}
    -- Only process new/updated orders
    where o.created_at > (select max(dbt_updated_at) from {{ this }})
{% endif %}

group by
    o.order_id, 
    o.customer_id, 
    o.order_date,
    o.status


-- Customer analytics summary
{{ config(materialized='table') }}

select
    dc.customer_status,
    count(*) as customer_count,
    avg(dc.customer_lifetime_orders) as avg_lifetime_orders,
    sum(case when fo.order_total is not null then fo.order_total else 0 end) as total_revenue
    
from {{ ref('dim_customers') }} dc
left join {{ ref('fct_orders') }} fo
    on dc.customer_id = fo.customer_id
    
group by dc.customer_status



-- macro
-- macro to customize schema naming 
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set defualt_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ defualt_schema }}
    {%- else -%}
        {{ defualt_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}