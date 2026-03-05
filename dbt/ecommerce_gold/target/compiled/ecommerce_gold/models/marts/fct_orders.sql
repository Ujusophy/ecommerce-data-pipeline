-- The most important Gold table: one row per order
-- This is what the revenue dashboard reads from

with orders as (
    select * from "ecommerce"."main"."stg_orders"
),

final as (
    select
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

        -- Business metrics
        case when not is_failed then total_amount else 0 end    as revenue,
        case when not is_failed then 1 else 0 end               as successful_order_flag,

        ordered_at,
        order_date
    from orders
)

select * from final