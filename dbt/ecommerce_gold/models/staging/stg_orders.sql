with source as (

    select * from read_parquet(
        'C:/Users/LENOVO/Downloads/merge-handler/ecommerce-pipeline/data/silver/orders/*.parquet'
    )

)

select
    event_id,
    order_id,
    user_id,
    product_id,
    product_name,
    category,
    quantity,
    price,
    total_amount,
    payment_method,
    status,
    is_failed,
    event_timestamp                         as ordered_at,
    date_trunc('day', event_timestamp)      as order_date
from source