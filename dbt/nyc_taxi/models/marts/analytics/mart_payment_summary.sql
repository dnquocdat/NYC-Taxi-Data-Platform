select
    f.payment_type_id,
    p.payment_type_name,
    count() as trip_count,
    sum(f.total_amount) as total_revenue,
    sum(f.tip_amount) as total_tips,
    avg(f.tip_amount) as average_tip_amount,
    avg(f.total_amount) as average_total_amount,
    sum(f.tip_amount) / nullIf(sum(f.total_amount), 0) as tip_revenue_ratio
from {{ ref('fact_trips') }} as f
left join {{ ref('dim_payment_type') }} as p
    on f.payment_type_id = p.payment_type_id
group by f.payment_type_id, p.payment_type_name
