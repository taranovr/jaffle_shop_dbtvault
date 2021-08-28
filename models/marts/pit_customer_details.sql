with customer_prepare as (
select
	customer_pk,
	customer_hashdiff,
	first_name,
	last_name,
	email,
	effective_from,
	load_date,
	record_source,
	row_number() over(partition by customer_pk
order by
	load_date desc ) as rn
from
	{{ ref('sat_customer_details') }}
),
customer_crm_prepare as (
select
	customer_pk,
	customer_hashdiff,
    country,
    age,
	effective_from,
	load_date,
	record_source,
	row_number() over(partition by customer_pk
order by
	load_date desc ) as rn
from
	{{ ref('sat_customer_crm_details') }}
)
select
	cp.customer_pk,
	cp.first_name,
	cp.last_name,
	cp.email,
	ccp.country,
    ccp.age
from
	customer_prepare cp
left join
	customer_crm_prepare ccp on cp.customer_pk = ccp.customer_pk and ccp.rn = 1
where
	cp.rn = 1
--and customer_pk  = '38b3eff8baf56627478ec76a704e9b52'