with source as (

    select * from read_parquet(
        'C:/Users/LENOVO/Downloads/merge-handler/ecommerce-pipeline/data/silver/cart_events/*.parquet'
    )

)

select
    event_id,
    user_id,
    product_id,
    product_name,
    category,
    quantity,
    price,
    line_total,
    event_timestamp                         as added_at,
    date_trunc('day', event_timestamp)      as cart_date
from source