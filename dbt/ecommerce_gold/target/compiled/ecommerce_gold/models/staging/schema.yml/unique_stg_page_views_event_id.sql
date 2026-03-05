
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "ecommerce"."main"."stg_page_views"
where event_id is not null
group by event_id
having count(*) > 1


