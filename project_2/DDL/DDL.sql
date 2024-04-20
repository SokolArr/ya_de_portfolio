drop table if exists shipping_country_rates cascade;
drop table if exists shipping_agreement cascade;
drop table if exists shipping_transfer cascade;
drop table if exists shipping_info cascade; 
drop table if exists shipping_status cascade; 
drop table if exists shipping_datamart cascade; 

-- create shipping_country_rates
create table shipping_country_rates
(
	id serial primary key not null,
	shipping_country text NULL,
	shipping_country_base_rate numeric(14, 3) NULL
);

-- create shipping_agreement
create table shipping_agreement
(
	agreement_id int8 primary key not null,
	agreement_number text not null,
	agreement_rate numeric(14, 2) not null,
	agreement_commission numeric(14, 2) not null
);

-- create shipping_transfer
create table shipping_transfer 
(
	id serial primary key not null,
	transfer_type text not null,
	transfer_model text not null,
	shipping_transfer_rate numeric(14, 3) not null
);

-- create shipping_info
create table shipping_info 
(
	shipping_id int8 primary key not null,
	vendor_id int8 not null,
	payment_amount numeric(14, 2) NULL,
	shipping_plan_datetime timestamp NULL,
	shipping_transfer_id int8 not null,
	shipping_agreement_id int8 not null,
	shipping_country_rate_id int8 not null,
	FOREIGN KEY (shipping_transfer_id) REFERENCES public.shipping_transfer(id) ON UPDATE cascade,
	FOREIGN KEY (shipping_agreement_id) REFERENCES public.shipping_agreement(agreement_id) ON UPDATE cascade,
	FOREIGN KEY (shipping_country_rate_id) REFERENCES public.shipping_country_rates(id) ON UPDATE cascade
);

-- create shipping_status
create table shipping_status
(
	shipping_id int8 primary key not null,
	status text not null,
	state text not null,
	shipping_start_fact_datetime timestamp NULL,
	shipping_end_fact_datetime timestamp NULL
);

-- create shipping_datamart
create table shipping_datamart 
(
	shipping_id int8 primary key not null,
	vendor_id int8 NOT NULL,
	transfer_type text NOT NULL,
	full_day_at_shipping int4 NUll,
	is_delay int4 not null,
	is_shipping_finish int4 not null,
	delay_day_at_shipping int4 NUll,
	payment_amount numeric(14,2) not null,
	vat numeric(14,3) not null,
	profit numeric(14,3) not null
);
