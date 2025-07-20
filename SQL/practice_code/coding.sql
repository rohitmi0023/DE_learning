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