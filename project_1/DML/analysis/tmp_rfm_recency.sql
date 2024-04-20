insert into analysis.tmp_rfm_recency(
	user_id, 
	recency
)
select 
	user_id, 
	recency::int4
from (
	select 
		u.id as user_id, 
		last_order, 
		ntile(5) over(order by last_order nulls FIRST) as recency
	from(
		select 
			user_id, 
			max(order_ts) as last_order
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