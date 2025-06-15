-- recursive CTEs
with recursiveCTE() as (
    select manager_id, id, name, 0 as level
    from dbo.employees
    where manager_id is null
    union all
    select e.*, level + 1
    from dbo.employees e 
    inner join recursiveCTE r
    on e.manager_id = r.id
)
select 
from recursiveCTE
;
