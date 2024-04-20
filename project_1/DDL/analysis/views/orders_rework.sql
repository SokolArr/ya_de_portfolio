CREATE OR REPLACE VIEW analysis.orders
as select 
	o.order_id,
	o.order_ts,
	o.user_id,
	o.bonus_payment,
	o.payment,
	o."cost",
	o.bonus_grant, 
	order_statuses.status_id as status
from 
	production.orders o
inner join( 
	select 
		osl.order_id, 
		osl.status_id
	from (
		select 
			order_id, 
			max(dttm) max_dttm
		from 
			production.OrderStatusLog 
		group by 
			order_id
	) as order_max_dttm 
	inner join 
		production.OrderStatusLog osl
	on 
		order_max_dttm.max_dttm = osl.dttm
) as order_statuses
using(order_id);