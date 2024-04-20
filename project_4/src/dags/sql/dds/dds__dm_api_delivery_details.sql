-- 5 dds.dm_api_delivery_details 
delete from dds.dm_api_delivery_details
where order_id in 
(
	select order_id 
	from dds.dm_api_orders 
	where date_trunc('day', order_ts) = '{{ds}}'
); 
with stg_api_deliveries as 
(
	select 
	("content"::json ->> 'order_id')::varchar as order_id,
	("content"::json ->> 'order_ts')::timestamp as order_ts,
	("content"::json ->> 'delivery_id')::varchar as delivery_id,
	("content"::json ->> 'courier_id')::varchar as courier_id,
	("content"::json ->> 'address')::varchar as address,
	("content"::json ->> 'delivery_ts')::timestamp as delivery_ts,
	("content"::json ->> 'rate')::smallint as rate,
	("content"::json ->> 'sum')::numeric(12,2)  as sum,
	("content"::json ->> 'tip_sum')::numeric(12,2) as tip_sum
	from stg.api_deliveries
)
insert into dds.dm_api_delivery_details (delivery_id,courier_id,address_id,delivery_ts,rate,order_id)
select d.delivery_id,
	   c.id as courier_id,
	   a.id as address_id,
	   d.delivery_ts,
	   d.rate,
	   d.order_id
from stg_api_deliveries d
inner join dds.dm_api_couriers c using(courier_id)
inner join dds.dm_api_address a on d.address = a.address;