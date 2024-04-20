insert into analysis.tmp_rfm_monetary_value (
	user_id, 
	monetary_value
)
select 
	user_id, 
	monetary_value::int4 
from (
	select u.id as user_id, 
		coalesce (payment_sum, 0 ) as payment_sum,
		ntile(5) over(order by payment_sum nulls FIRST) as monetary_value
	from(
		select 
			user_id, 
			sum(payment) as payment_sum
		from 
			analysis.orders
		where 
			status = (select id from analysis.orderstatuses where key = 'Closed')
		group by 
			user_id
	) o
	right join 
		analysis.users u 
	on 
		o.user_id = u.id
	order by 3 desc
) as to_insert;