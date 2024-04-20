-- 3 dds.dm_api_orders
delete from dds.dm_api_orders where date_trunc('day', order_ts) = '{{ds}}';
insert into dds.dm_api_orders (order_id,order_ts)
select 
	("content"::json ->> 'order_id')::varchar as order_id,
	("content"::json ->> 'order_ts')::timestamp as order_ts
from stg.api_deliveries;