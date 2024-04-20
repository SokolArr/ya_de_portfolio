insert into analysis.tmp_rfm_frequency (
	user_id, 
	frequency
)
select 
	user_id, 
	frequency::int4 
from (
	select u.id as user_id, 
		coalesce (order_cnt, 0) as order_cnt, 
		ntile(5) over(order by order_cnt nulls FIRST) as frequency
	from(
		select 
			user_id, 
			count(*) as order_cnt
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