--------------------------------------------------------- Week 2: Advanced SQL & Query Optimization ---------------------------------
--------------------- Day 8 : Query Optimization (EXPLAIN, INDEXING)
------------ Article:
-- https://use-the-index-luke.com/
-- https://leetcode.com/problems/trips-and-users/
SELECT
  t.Request_at AS Day
  ,ROUND(SUM(t.Status != "completed") / COUNT(*), 2) AS 'Cancellation Rate'
FROM Trips t
JOIN Users d ON t.Driver_Id = d.Users_Id
JOIN Users c ON t.Client_Id = c.Users_Id
WHERE d.Banned = "No"
  AND c.Banned = "No"
  AND t.Request_at BETWEEN "2013-10-01" AND "2013-10-03"
GROUP BY t.Request_at
ORDER BY t.Request_at;
-- https://platform.stratascratch.com/coding/10315-query-optimization



------------------- Day 9 :  Pivoting Data (CASE WHEN, PIVOT)
------------- Article: 
-- https://www.sqlshack.com/sql-pivot-and-unpivot-examples/
-- https://leetcode.com/problems/market-analysis-i/
select
u.user_id as buyer_id
,u.join_date
,coalesce(sum(extract(year from order_date) = '2019'),0) as orders_in_2019
from users u
left join orders o
on o.buyer_id = u.user_id
group by u.user_id, u.join_date
;


----------------------- Day 10 : Recursive CTEs (Hierarchical Data)
-- Article: https://neon.com/postgresql/postgresql-tutorial/postgresql-recursive-query

with recursive cte as (
    select employee_id
    from employees
    where manager_id = 1 and employee_id <> 1

    union 

    select 
    from cte c
    join employees e 
    on e.manager_id = c.employee_id
)
select *
from cte
;


with ct1 as (
select department_id, count(*) as counts
from az_employees
group by department_id
)
, ct2 as (
select department_id, counts, rank() over(order by counts desc) as ranked
from ct1
)
,ct3 as (
select department_id
from ct2 
where ranked = 1
)
select 
first_name, last_name
from az_employees e
join ct3 d 
on d.department_id = e.department_id
where lower(e.position) like '%manager%'
;

----------------------- Day 11 : Stored Procedures & UDFs
-- Article: https://docs.snowflake.com/en/sql-reference/stored-procedures

create procedure output_message(message VARCHAR)
returns varchar not null 
language sql 
as 
    begin
        return message;
    end
;

create procedure myproc(from_table STRING, to_table STRING, COUNT INT)
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
as 
$$
def run(session, from_table, to_table, count):
    session.table(from_table).limit(count).write.save_as_table(to_table)
    return 'SUCCESS'
$$
;

----------------------- Day 12 : Data Quality Checks (NULLs, Duplicates)
-- Article: https://towardsdatascience.com/data-cleaning-with-sql

-- https://datalemur.com/questions/matching-skills
SELECT 
candidate_id
FROM candidates
where skill = 'Python' or skill = 'Tableau' or skill = 'PostgreSQL'
group by candidate_id
having count(*)= 3
;

----------------------- Day 13 : Snowflake-Specific SQL (CLONE, TIME TRAVEL)
-- Article: https://docs.snowflake.com/



----------------------- Day 14 : Mock Interview (SQL + DE Concepts)
-- Article: https://www.pramp.com/


