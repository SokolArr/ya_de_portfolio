-- 4 dds.dm_api_restaurants
insert into dds.dm_api_restaurants (restaurant_id, name)
select distinct ("content"::json ->> '_id')::varchar as restaurant_id,
				("content"::json ->> 'name')::varchar as name
from stg.api_restaurants
where ("content"::json ->> '_id')::varchar not in (select restaurant_id from dds.dm_api_restaurants);