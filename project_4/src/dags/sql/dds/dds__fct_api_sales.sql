-- 6 dds.fct_api_sales
delete from dds.fct_api_sales
where order_id in 
(
	select id 
	from dds.dm_api_orders 
	where date_trunc('day', order_ts) = '{{ds}}'
); 
with stg_api_deliveries as 
(
	select 
	("content"::json ->> 'order_id')::varchar as order_id,
	("content"::json ->> 'delivery_id')::varchar as delivery_id,
	("content"::json ->> 'sum')::numeric(12,2) as order_sum,
	("content"::json ->> 'tip_sum')::numeric(12,2) as tip_sum
	from stg.api_deliveries
)
insert into dds.fct_api_sales (order_id,delivery_id,order_sum,tip_sum)
select o.id as order_id,
	   dd.id as delivery_id,
	   d.order_sum,
	   d.tip_sum
from stg_api_deliveries d
inner join dds.dm_api_orders o using(order_id)
inner join dds.dm_api_delivery_details dd using(delivery_id);