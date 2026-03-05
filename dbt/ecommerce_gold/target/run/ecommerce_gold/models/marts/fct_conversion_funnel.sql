
  
    
    

    create  table
      "ecommerce"."main"."fct_conversion_funnel__dbt_tmp"
  
    as (
      -- The conversion funnel: how many users go from view → cart → order per day
-- This is the most watched metric in any e-commerce company

with page_views as (
    select
        view_date               as event_date,
        count(distinct user_id) as users_who_viewed
    from "ecommerce"."main"."stg_page_views"
    group by 1
),

carts as (
    select
        cart_date               as event_date,
        count(distinct user_id) as users_who_carted
    from "ecommerce"."main"."stg_cart_events"
    group by 1
),

orders as (
    select
        order_date              as event_date,
        count(distinct user_id) as users_who_ordered,
        count(order_id)         as total_orders,
        sum(revenue)            as total_revenue
    from "ecommerce"."main"."fct_orders"
    group by 1
),

final as (
    select
        p.event_date,
        p.users_who_viewed,
        coalesce(c.users_who_carted,  0)    as users_who_carted,
        coalesce(o.users_who_ordered, 0)    as users_who_ordered,
        coalesce(o.total_orders,      0)    as total_orders,
        coalesce(o.total_revenue,     0)    as total_revenue,

        -- Conversion rates (the KPIs everyone watches)
        round(
            coalesce(c.users_who_carted, 0) * 100.0
            / nullif(p.users_who_viewed, 0), 2
        )                                   as view_to_cart_rate,

        round(
            coalesce(o.users_who_ordered, 0) * 100.0
            / nullif(p.users_who_viewed, 0), 2
        )                                   as view_to_order_rate

    from page_views p
    left join carts   c using (event_date)
    left join orders  o using (event_date)
)

select * from final
order by event_date desc
    );
  
  