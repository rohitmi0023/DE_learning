/*Your PostgreSQL script goes here*/
with ct1 as (
    select 
    seat_no,
    request,
    person_id,
    dense_rank() over(partition by seat_no order by request_id) as ranked
    from requests 
)
,requests as (
    select 
    seat_no,
    request,
    person_id
    from ct1
    where ranked = 1
)
select 
s.seat_no,
case 
    when s.status = 2 then s.status
    else 
        case 
            when s.status = 1 and s.person_id = r.person_id then r.request 
            else 
                case when s.status = 0 and r.request <> 0 
                then r.request else s.status end 
            end
    end as status,
case 
    when s.status = 2 then s.person_id
    else 
        case 
            when s.status = 1 and s.person_id = r.person_id then r.person_id 
            else
                case when s.status = 0 and r.request <> 0 
                then r.person_id else s.person_id end
            end
    end as person_id
from seats s
left join requests r
on s.seat_no = r.seat_no
order by s.seat_no
;


-- regexp
-- '^U[0-9]{3}$' starts with U, followed by exactly 3 digits
-- '[a-zA-Z0-9._%+-]+@gmail.com$' ends with gmail.com must
-- 'ABC[0-9]+' OR 'ABC\\d+' contain ABC followed by any number
-- '\\d{4}-\\d{2}-\\d{2}$'
-- '[0-9A-Za-z._-+%]+@[a-zA-Z0-9.-]+\.[a-zA-Z]+' only email part from line
-- '#[0-9]+' only hashtag and followed number from line

-- '\d{12}' Showing only last 4 digits of a 16 digit card number

------- domains from weird emails
select domain
from table
where regexp_like(user_id, '[0-9A-Za-z]@')


----------- PIVOTING
select
student
, max(case when subject = 'Math' then grade else 0 end) as Math
, max(case when subject = 'Science' then grade else 0 end) as Science
, max(case when subject = 'History' then grade else 0 end) as History
from student_grades
group by student;

Execution Step 1
student,(CASE... Math),(CASE... Science),(CASE... History)
John,90,0,0
John,0,85,0
Jane,95,0,0
Jane,0,0,88

Execution Step 2
Group by student SQUASHES all 'John' rows into one.

---------- UNPIVOTING
-- Method #1
select year, 'Jan', Jan_sales as sales from widetable
union all
select year, 'Feb', Feb_sales as sales from widetable;

-- Method #2
select t.year, x.month, x.sales
from widetable t,
lateral (values ('Jan', t.jan_sales), ('Feb', t.feb_sales)) as x(month, sales);

---------- GROUPING SETS
-- Method #1
-- Query 1: Granular Data
SELECT Region, Product, SUM(Amount) FROM Sales GROUP BY Region, Product
UNION ALL
-- Query 2: Subtotals by Region (Product becomes NULL here)
SELECT Region, NULL, SUM(Amount) FROM Sales GROUP BY Region
UNION ALL
-- Query 3: Grand Total (Region and Product become NULL)
SELECT NULL, NULL, SUM(Amount) FROM Sales;

-- Method #2
select region, product, sum(amount)
from sales group by grouping sets(
    (region, product),
    (region),
    ()
);


