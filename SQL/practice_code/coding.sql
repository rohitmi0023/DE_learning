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