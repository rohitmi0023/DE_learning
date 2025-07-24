Interval is SQL Data Type that represents a span of time. Its more of like 'duration' rather than a specific point in time.

Syntax:
INTERVAL 'value' unit

Common Units:
* SECOND, MINUTE, HOUR
* DAY, WEEK, MONTH, YEAR
* MICROSECOND, MILLISECOND

Examples
* INTERVAL '5' DAY -> 5 days
* INTERVAL '30' MINUTE -> 30 minutes

Core Operations

1. Adding Time
SELECT current_timestamp + INTERVAL '7' DAY;

2. Subtrating Time
SELECT current_timestamp - INTERVAL '7' DAY;

3. Complex Intervals
Interval '1' year + interval '6' month;

LEVEL 1: Basic practice

-- 1.1  Write a query to find the date that is exactly 10 days from today.
select current_timestamp + INTERVAL '10' day;

-- 1.2 Given a table orders with order_date column, find all orders placed in the last 7 days.
select
*
from orders
where order_date >= current_timestamp - interval '7' day
and order_date <= current_timestamp
;

-- 1.3 Calculate what the date will be 3 months from now.
select current_date + interval '3' Month;

LEVEL 2: Filtering and Condition

-- 2.1 From an employees table with hire_date, find all employees hired in the last 90 days.
select *
from employee 
where hire_date >= current_date - interval '90' day 
and hire_date <= current_date
;

-- 2.2 Find all products from a products table where created_date is between 30 days ago and 10 days ago.
select 
from products 
where created_date >= current_date - interval '30' day
and created_date <= current_date - interval '10' day 
;

-- 2.3 Calculate the timestamp that is exactly 2 hours and 30 minutes from the current timestamp.
select current_timestamp + interval '2' hour + interval '30' minute;
select current_timestamp + interval '150' minute;


-- LEVEL 3: Calculations and Aggregations

-- 3.1 Given a subscriptions table with start_date and duration_months, calculate the end_date for each subscription.
select 
start_date + (interval '1 month' * duration_months) as end_date
from subscription


-- 3.2 Find the average number of days between order date and ship date from an orders table (columns: order_date, ship_date).
select
avg(extract(day from (ship_date - order_date))) as avg_days
from orders 
;

-- 3.3 From a tasks table with created_at and completed_at, find tasks that took longer than 2 days to complete.
select 
*
from tasks 
where datediff('day', created_at, completed_at) > 2
-- or completed_at - created_at > interval '2' day
;

-- Level 4: Advnaced Grouping and Window Functions 
-- 4.1 Group sales by quarters, but define quarters as starting from February 1st instead of January 1st. Show sales for each custom quarter in the last year
with ct1 as (
select
*
,case when extract(month from sales_date) in (2,3,4) then 'Q1'
    when extract(month from sales_date) in (5,6,7) then 'Q2'
    when extract(month from sales_date) in (8,9,10) then 'Q3'
    else 'Q4'
end as modified_quarter
from sales 
)
select
modified_quarter
, sum(sales)
from ct1 
-- where sales_date >= current_date - interval '1' year
where sales_date >= date_trunc('year', current_date - INTERVAL '1 YEAR')
and sales_date < date_trunc('year', current_date)
group by modified_quarter
;

-- 4.2 Find customers who made purchases in consecutive months. Use a sales table with customer_id and sale_date.
with ct as (
select 
distinct
customer_id,
date_trunc('month', sales_date) as sales_month
from sales
)
,ct1 as (
select 
customer_id
, row_number() over(partition by customer_id order by sales_month) as row_num
,sales_month - row_number() over(partition by customer_id order by sales_month) * INTERVAL '1 MONTH' as customers_month_group
from ct
)
select 
customer_id 
from ct1 
group by customers_month_group, customer_id 
having count(*) > 1
;

-- alteranate
with ct1 as (
    select
    distinct
    customer_id
    ,date_trunc('month', sales_date) as sale_month
    from sales 
)
, lagged_sales as (
    select
    customer_id
    , sale_month
    , lag(sale_month, 1) over(partition by customer_id order by sale_month) as prev_month
    from monthly_sales 
)
select 
distinct customer_id
from lagged_sales
where sale_month = prev_month + interval '1 month'
;

-- 4.3 Calculate a rolling 7-day average of daily sales amounts, but only for dates within the last 30 days.
with ct1 as (
    select
    sales_date
    ,sum(sales_amount) as agg_daily_sales
    from daily_sales
    -- where datediff('day', current_date, sales_date) <= 30
    where sales_date >= sales_date - interval '30 day'
    group by sales_date 
)
select 
sales_date
,avg(agg_daily_sales) over(order by sales_date rows between 6 preceding and current_row) as avg_7_day_rolling
from ct1 
order by sales_date
;


-- Level 5: Complex Business Logic
-- 5.1 Create a query that generates a series of dates for the next 12 months, but only for the first Monday of each month.
select 
current_date
from dates 
;

-- 5.2 From an events table with event_start and event_end timestamps, find events that overlap with a given time period (use INTERVAL to define the period).
select 
event_start
, event_end
from events
;

-- 5.3 Calculate customer retention rate by finding what percentage of customers who made a purchase also made another purchase within 30 days, 60 days, and 90 days.