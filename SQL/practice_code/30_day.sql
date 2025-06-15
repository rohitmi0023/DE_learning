-- Day 1 
-- Basic SQL Refresher (SELECT, WHERE, GROUP BY, JOINs)
-- Article- https://sqlbolt.com/
-- Questions:
-- https://leetcode.com/problems/combine-two-tables/
select
p.firstname,
p.lastname,
coalesce(a.city) as city,
coalesce(a.state) as state
from person p
left join address a
on a.personid = p.personid
; 
-- https://leetcode.com/problems/second-highest-salary/
select
distinct
case when count(distinct salary) < 2 then null else (select distinct salary from employee order by salary desc limit 1 offset 1) end as SecondHighestSalary
from employee
;

-- Day 2
-- Advanced Joins & Subqueries
-- Article- https://www.w3schools.com/sql/sql_join.asp
-- Questions:
-- https://leetcode.com/problems/employees-earning-more-than-their-managers/
select
e.name as Employee
from employee e
join employee m
on e.managerid = m.id
where e.salary > m.salary
;
-- https://leetcode.com/problems/department-top-three-salaries/description/
with ct as (
select
d.name as Department, e.name as Employee, e.salary as Salary,
dense_rank() over(partition by d.name order by e.Salary desc) as ranked
from employee e
join department d 
on e.departmentid = d.id
)
select
Department, Employee, Salary
from ct
where ranked <= 3
;
-- https://platform.stratascratch.com/coding/10300-premium-vs-freemium?code_type=1
-- finding total downloads for paying and non paying users by date 
-- non paying has more number that paying
with paid_customer as (
select
df.date, sum(df.downloads)
from ms_user_dimension ud 
join ms_acc_dimension ad 
on ad.acc_id = ud.acc_id
and paying_customer = 'yes'
join ms_download_facts df
on df.user_id = ud.user_id 
group by df.date
)
,non_paid_customer as (
select
df.date, sum(df.downloads)
from ms_user_dimension ud 
join ms_acc_dimension ad 
on ad.acc_id = ud.acc_id
and paying_customer = 'no'
join ms_download_facts df
on df.user_id = ud.user_id 
group by df.date
)
select
np.date, np.sum as non_paying, pc.sum as paying
from non_paid_customer np 
join paid_customer pc 
on pc.date = np.date
where np.sum > pc.sum
order by np.date
;

-- Day 3
-- CTEs & Nested Queries
-- Article- https://www.udemy.com/course/the-complete-sql-bootcamp/
-- Questions:
-- https://leetcode.com/problems/customers-who-bought-all-products/
select
customer_id
from customer
group by customer_id
having count(distinct product_key) = (select count(1) from product)
;
-- https://platform.stratascratch.com/coding/10064-highest-energy-consumption
with ct as (
select 
 coalesce(e.recorded_date,a.recorded_date,n.recorded_date) recorded_date
 ,sum(COALESCE(e.consumption,0) + COALESCE(a.consumption,0) + COALESCE(n.consumption,0)) total_consumption
from fb_eu_energy e
full outer join fb_asia_energy a
on a.recorded_date = e.recorded_date
full outer join fb_na_energy n 
on n.recorded_date = a.recorded_date
and n.recorded_date = e.recorded_date
group by 1
)
select 
recorded_date, total_consumption
from ct
where total_consumption = (select max(total_consumption) from ct)
;-- Execution time: 0.01104 seconds

2nd Approach with Union All
with ct as (
select
recorded_date, sum(consumption) as total_consumption
from (
select 
recorded_date, consumption
from fb_eu_energy e
union all
select 
recorded_date, consumption
from fb_asia_energy 
union all
select 
recorded_date, consumption
from fb_na_energy
)
group by 1
)
select 
recorded_date, total_consumption
from ct
where total_consumption = (select max(total_consumption) from ct)
; -- Execution time: 0.04681 seconds
-- https://datalemur.com/questions/linkedin-power-creators
    select
    pp.profile_id
    from personal_profiles pp
    join company_pages cp 
    on cp.company_id = pp.employer_id
    where pp.followers > cp.followers
    order by 1
    ;

-- Day 4
-- Window Functions (ROW_NUMBER, RANK, DENSE_RANK)
-- Article:
    --  https://mode.com/sql-tutorial/sql-window-functions  
-- Questions:
    -- https://leetcode.com/problems/rank-scores/
        select
        score, ranked as `rank`
        from (
        select 
        score
        ,dense_rank() over(order by score desc) ranked
        from scores
        order by ranked  
        ) s
        ;
    -- https://platform.stratascratch.com/coding/9680-most-profitable-companies
        select
        company
        ,profits
        from (
        select 
        company
        ,profits
        ,dense_rank() over(order by profits desc) ranked
        from forbes_global_2010_2014
        ) s
        where ranked <=3
        order by ranked
        limit 3
        ;
    -- https://datalemur.com/questions/supercloud-customer
        SELECT
        c.customer_id
        FROM customer_contracts c 
        left join products p 
        on p.product_id = c.product_id
        group by c.customer_id
        having count(distinct p.product_category) = (select count(distinct product_category) from products)
        ;

-- Day 5
-- Window Functions (LEAD, LAG, FIRST_VALUE)
-- Article: https://www.sqlservertutorial.net/sql-server-window-functions/
    --     https://www.udemy.com/course/the-advanced-sql-course-2021/

-- Questions:
    -- https://leetcode.com/problems/consecutive-numbers/
    -- https://platform.stratascratch.com/coding/2053-monthly-retention-rate
    -- https://datalemur.com/questions/yoy-growth-rate  