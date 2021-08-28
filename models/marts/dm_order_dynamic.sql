with cte_week as (
    with prep_week_year as (
        select
            sod.order_pk,
            sod.status,
            date_part('year', sod.order_date::date) as year,
            date_part('week', sod.order_date::date) as week
        from
            {{ ref('sat_order_details') }}  sod )
    select
        pwy.order_pk,
        pwy.status,
        pwy.year,
        pwy.week,
        year::text || '-' || week::text as week_text
    from
        prep_week_year pwy 
)
select
    count((od.order_pk)) as cnt_order,
    od.year,
    od.week,
    od.status
from
    cte_week od 
group by
    od.year,
    od.week,
    od.status
order by
    od.year,
    od.week,
    od.status