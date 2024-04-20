--stg
-- drop schema if exists stg cascade;
create schema if not exists stg;
create table if not exists stg.api_restaurants 
(
	id serial primary key,
	content text unique,
	load_ts timestamp
);
create table if not exists stg.api_couriers
(
	id serial primary key,
	content text unique,
	load_ts timestamp
);
create table if not exists stg.api_deliveries
(
	id serial primary key,
	content text unique,
	load_ts timestamp
);	