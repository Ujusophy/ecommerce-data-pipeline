-- Product performance dimension: which products drive the most revenue
-- Combines views, cart adds, and orders per product

with views as (
    select
        product_id,
        product_name,
        category,
        count(*)                as total_views,
        count(distinct user_id) as unique_viewers
    from "ecommerce"."main"."stg_page_views"
    group by 1, 2, 3
),

carts as (
    select
        product_id,
        count(*)    as total_cart_adds,
        sum(quantity) as total_units_carted
    from "ecommerce"."main"."stg_cart_events"
    group by 1
),

orders as (
    select
        product_id,
        count(order_id)     as total_orders,
        sum(quantity)       as total_units_sold,
        sum(revenue)        as total_revenue,
        avg(total_amount)   as avg_order_value
    from "ecommerce"."main"."fct_orders"
    group by 1
),

final as (
    select
        v.product_id,
        v.product_name,
        v.category,
        v.total_views,
        v.unique_viewers,
        coalesce(c.total_cart_adds,    0)   as total_cart_adds,
        coalesce(o.total_orders,       0)   as total_orders,
        coalesce(o.total_units_sold,   0)   as total_units_sold,
        coalesce(o.total_revenue,      0.0) as total_revenue,
        coalesce(o.avg_order_value,    0.0) as avg_order_value,

        -- Product conversion rate
        round(
            coalesce(o.total_orders, 0) * 100.0
            / nullif(v.total_views, 0), 2
        )                                   as product_conversion_rate

    from views v
    left join carts  c using (product_id)
    left join orders o using (product_id)
)

select * from final
order by total_revenue desc