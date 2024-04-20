drop table if exists cdm.user_product_counters cascade;
create table cdm.user_product_counters (
	id serial primary key,
	user_id uuid not null,
	product_id uuid not null,
	product_name varchar not null,
	order_cnt int4 check(order_cnt>=0) not null
);
CREATE UNIQUE INDEX user_product_counters_idx ON cdm.user_product_counters (user_id, product_id);

drop table if exists cdm.user_category_counters cascade;
create table cdm.user_category_counters (
	id serial primary key,
	user_id uuid not null,
	category_id uuid not null,
	category_name varchar not null,
	order_cnt int4 check(order_cnt>=0) not null
);
CREATE UNIQUE INDEX user_category_counters_idx ON cdm.user_category_counters (user_id, category_id);

drop table if exists stg.order_events cascade;
create table stg.order_events (
	id serial primary key,
	object_id integer unique not null,
	payload json not null,
	object_type varchar not null,
	sent_dttm timestamp not null
);



drop table if exists dds.h_user cascade;
create table dds.h_user (
	h_user_pk uuid primary key not null,
	user_id varchar not null,
	load_dt timestamp not null,
	load_src varchar not null
);

drop table if exists dds.h_product cascade;
create table dds.h_product (
	h_product_pk uuid primary key not null,
	product_id varchar not null,
	load_dt timestamp not null,
	load_src varchar not null
);

drop table if exists dds.h_category cascade;
create table dds.h_category (
	h_category_pk uuid primary key not null,
	category_name varchar not null,
	load_dt timestamp not null,
	load_src varchar not null
);

drop table if exists dds.h_restaurant cascade;
create table dds.h_restaurant (
	h_restaurant_pk uuid primary key not null,
	restaurant_id varchar not null,
	load_dt timestamp not null,
	load_src varchar not null
);

drop table if exists dds.h_order cascade;
create table dds.h_order (
	h_order_pk uuid primary key not null,
	order_id integer not null,
	load_dt timestamp not null,
	load_src varchar not null,
	order_dt timestamp not null
);



drop table if exists dds.l_order_product cascade;
create table dds.l_order_product (
	hk_order_product_pk uuid not null,
	h_order_pk uuid not null REFERENCES dds.h_order (h_order_pk),
	h_product_pk uuid not null REFERENCES dds.h_product (h_product_pk),
	load_dt timestamp not null,
	load_src varchar not null
);

drop table if exists dds.l_product_restaurant cascade;
create table dds.l_product_restaurant (
	hk_product_restaurant_pk uuid not null,
	h_restaurant_pk uuid not null REFERENCES dds.h_restaurant (h_restaurant_pk),
	h_product_pk uuid not null REFERENCES dds.h_product (h_product_pk),
	load_dt timestamp not null,
	load_src varchar not null
);

drop table if exists dds.l_product_category cascade;
create table dds.l_product_category (
	hk_product_category_pk uuid not null,
	h_category_pk uuid not null REFERENCES dds.h_category (h_category_pk),
	h_product_pk uuid not null REFERENCES dds.h_product (h_product_pk),
	load_dt timestamp not null,
	load_src varchar not null
);

drop table if exists dds.l_order_user cascade;
create table dds.l_order_user (
	hk_order_user_pk uuid not null,
	h_user_pk uuid not null REFERENCES dds.h_user (h_user_pk),
	h_order_pk uuid not null REFERENCES dds.h_order (h_order_pk),
	load_dt timestamp not null,
	load_src varchar not null
);


drop table if exists dds.s_user_names cascade;
create table dds.s_user_names (
	h_user_pk uuid not null REFERENCES dds.h_user (h_user_pk),
	username varchar not null,
	userlogin varchar not null,
	load_dt timestamp not null,
	load_src varchar not null,
	hk_user_names_hashdiff uuid not null
);
alter table dds.s_user_names add primary key (h_user_pk, load_dt);

drop table if exists dds.s_product_names cascade;
create table dds.s_product_names (
	h_product_pk uuid not null REFERENCES dds.h_product (h_product_pk),
	name varchar not null,
	load_dt timestamp not null,
	load_src varchar not null,
	hk_product_names_hashdiff uuid not null
);
alter table dds.s_product_names add primary key (h_product_pk, load_dt);

drop table if exists dds.s_restaurant_names cascade;
create table dds.s_restaurant_names (
	h_restaurant_pk uuid not null REFERENCES dds.h_restaurant (h_restaurant_pk),
	name varchar not null,
	load_dt timestamp not null,
	load_src varchar not null,
	hk_restaurant_names_hashdiff uuid not null
);
alter table dds.s_restaurant_names add primary key (h_restaurant_pk, load_dt);

drop table if exists dds.s_order_cost cascade;
create table dds.s_order_cost (
	h_order_pk uuid not null REFERENCES dds.h_order (h_order_pk),
	cost decimal(19, 5) not null,
	payment decimal(19, 5) not null,
	load_dt timestamp not null,
	load_src varchar not null,
	hk_order_cost_hashdiff uuid not null
);
alter table dds.s_order_cost add primary key (h_order_pk, load_dt);

drop table if exists dds.s_order_status cascade;
create table dds.s_order_status (
	h_order_pk uuid not null REFERENCES dds.h_order (h_order_pk),
	status varchar not null,
	load_dt timestamp not null,
	load_src varchar not null,
	hk_order_status_hashdiff uuid not null
);
alter table dds.s_order_status add primary key (h_order_pk, load_dt);