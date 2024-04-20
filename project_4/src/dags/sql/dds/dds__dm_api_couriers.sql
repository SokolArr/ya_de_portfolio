-- 1 dds.dm_api_couriers 
insert into dds.dm_api_couriers (courier_id, name)
select distinct ("content"::json ->> '_id')::varchar as courier_id,
		        ("content"::json ->> 'name')::varchar as name
from stg.api_couriers
where ("content"::json ->> '_id')::varchar not in (select courier_id from dds.dm_api_couriers);