--dds
-- drop schema if exists dds cascade;
create schema if not exists dds;
create table if not exists dds.dm_api_couriers
(
	id serial primary key,
	courier_id varchar unique not null,
	name varchar not null
);
create table if not exists dds.dm_api_address
(
	id serial primary key,
	address varchar unique not null
);
create table if not exists dds.dm_api_restaurants
(
	id serial primary key,
	restaurant_id varchar unique not null,
	name text not null
);
create table if not exists dds.dm_api_orders
(
	id serial primary key,
	order_id varchar unique not null,
	order_ts timestamp not null
);
create table if not exists dds.dm_api_delivery_details
(
	id serial primary key,
	delivery_id varchar not null,
	courier_id integer references dds.dm_api_couriers(id) on delete cascade not null,
	address_id integer references dds.dm_api_address(id) on delete cascade not null,
	delivery_ts timestamp not null,
	rate smallint not null,
	order_id varchar not null	
);

create table if not exists dds.fct_api_sales
(
	id serial primary key,
	order_id integer references dds.dm_api_orders(id) on delete cascade not null,
	delivery_id integer references dds.dm_api_delivery_details(id) on delete cascade not null,
	order_sum numeric(12,2) not null,
	tip_sum numeric (12,2) not null
);