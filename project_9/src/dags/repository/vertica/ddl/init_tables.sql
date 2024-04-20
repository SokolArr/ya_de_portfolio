-- DDL
-- STAGE
drop table if exists STV2023121136__STAGING.transactions;
create table STV2023121136__STAGING.transactions(
	id identity,
	operation_id varchar,
	account_number_from varchar,
	account_number_to varchar,
	currency_code varchar,
	country varchar,
	status varchar,
	transaction_type varchar,
	amount varchar,
	transaction_dt varchar,
	load_time timestamp default current_timestamp
)
order by load_time
SEGMENTED BY id all nodes
PARTITION BY load_time::date
GROUP BY calendar_hierarchy_day(load_time::date, 3, 2);

drop table if exists STV2023121136__STAGING.currencies;
create table STV2023121136__STAGING.currencies(
	id identity,
	date_update varchar,
	currency_code varchar,
	currency_code_with varchar,
	currency_with_div varchar,
	load_time timestamp default current_timestamp
)
order by load_time
SEGMENTED BY id all nodes
PARTITION BY load_time::date
GROUP BY calendar_hierarchy_day(load_time::date, 3, 2);

-- DDS
drop table if exists STV2023121136__DWH.transactions;
create table STV2023121136__DWH.transactions(
	hk_operation_id bigint primary key,
	operation_id uuid,
	account_number_from bigint,
	account_number_to bigint,
	currency_code int,
	country varchar,
	status varchar,
	transaction_type varchar,
	amount bigint,
	transaction_dt timestamp
)
order by transaction_dt
SEGMENTED BY hk_operation_id all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);

drop table if exists STV2023121136__DWH.currencies;
create table STV2023121136__DWH.currencies(
	hk_currency_code_id bigint primary key,
	date_update date,
	currency_code int,
	currency_code_with int,
	currency_with_div numeric(32,5)
)
order by date_update
SEGMENTED BY hk_currency_code_id all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

drop table if exists STV2023121136__DWH.global_metrics;
create table STV2023121136__DWH.global_metrics(
	id bigint,
	date_update date,
	currency_from integer,
	amount_total numeric(32,5),
	cnt_transactions integer,
	avg_transactions_per_account numeric(32,5),
	cnt_accounts_make_transactions integer
)
order by date_update
SEGMENTED BY id all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);