-- 2 dds.dm_api_address
insert into dds.dm_api_address (address)
	select distinct ("content"::json ->> 'address')::varchar as address 
	from stg.api_deliveries
	where ("content"::json ->> 'address')::varchar not in (select address from dds.dm_api_address);