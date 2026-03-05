-- Staging models are thin wrappers over Silver tables
-- They rename columns, cast types, and add nothing else
-- Rule: staging models should be boring and predictable

with source as (

    select * from read_parquet(
        'C:/Users/LENOVO/Downloads/merge-handler/ecommerce-pipeline/data/silver/page_views/*.parquet'
    )

)

select
    event_id,
    user_id,
    session_id,
    product_id,
    product_name,
    category,
    event_timestamp                         as viewed_at,
    date_trunc('day', event_timestamp)      as view_date
from source