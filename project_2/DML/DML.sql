-- insert shipping_country_rates
insert into shipping_country_rates (shipping_country, shipping_country_base_rate)
	select 
	shipping_country,
	shipping_country_base_rate 
	FROM shipping
	group by 1,2;

-- insert shipping_agreement
insert into shipping_agreement
	select distinct
	(regexp_split_to_array(vendor_agreement_description , E'\\:+'))[1]::int8 as agreement_id,
	(regexp_split_to_array(vendor_agreement_description , E'\\:+'))[2] as agreement_number,
	cast((regexp_split_to_array(vendor_agreement_description , E'\\:+'))[3] as DOUBLE PRECISION) as agreement_rate,
	cast((regexp_split_to_array(vendor_agreement_description , E'\\:+'))[4] as DOUBLE PRECISION) as agreement_commission
	from shipping
	order by 1;

-- insert shipping_agreement
insert into shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
	select distinct
	(regexp_split_to_array(shipping_transfer_description , E'\\:+'))[1] as transfer_type,
	(regexp_split_to_array(shipping_transfer_description , E'\\:+'))[2] as transfer_model,
	shipping_transfer_rate
	FROM shipping
	order by 1, 2;

-- insert shipping_info
with shipping_distinct as (
	select distinct
	shippingid as shipping_id, 
	vendorid as vendor_id,
	payment_amount, 
	shipping_plan_datetime,
	(regexp_split_to_array(shipping_transfer_description , E'\\:+'))[1] as transfer_type,
	(regexp_split_to_array(shipping_transfer_description , E'\\:+'))[2] as transfer_model,
	shipping_country,
	(regexp_split_to_array(vendor_agreement_description , E'\\:+'))[1]::int8 as agreement_id
	from shipping)
insert into shipping_info 
	select sd.shipping_id,sd.vendor_id,sd.payment_amount,sd.shipping_plan_datetime, 
		   st.id as shipping_transfer_id, 
		   sa.agreement_id as shipping_agreement_id,
		   scr.id as shipping_country_rate_id
	from shipping_distinct sd
	inner join shipping_transfer st 
		  on sd.transfer_type = st.transfer_type 
		  and sd.transfer_model = st.transfer_model
	inner join shipping_agreement sa using(agreement_id)
	inner join shipping_country_rates scr using(shipping_country)
	order by 1;

-- insert shipping_status
with fact_ship_date as (
	select 
	shippingid,
	max(case when state = 'booked' then state_datetime else null end) as shipping_start_fact_datetime,
	max(case when state = 'recieved' then state_datetime else null end) as shipping_end_fact_datetime
	from shipping
	where state in ('booked','recieved')
	group by 1),
max_ship_date as (
	select 
	shippingid,
	max(state_datetime) as max_date
	from shipping
	group by 1
)
insert into shipping_status 
select s.shippingid as shipping_id, s.status, s.state, 
	   fact_ship_date.shipping_start_fact_datetime,
	   fact_ship_date.shipping_end_fact_datetime
from fact_ship_date 
inner join max_ship_date using(shippingid)
inner join shipping s on max_ship_date.max_date = s.state_datetime
order by 1;

-- insert shipping_datamart
insert into shipping_datamart
select ss.shipping_id, 
	   si.vendor_id,
	   st.transfer_type,
	   date_part('day',ss.shipping_end_fact_datetime 
	   			 - ss.shipping_start_fact_datetime) as full_day_at_shipping,
	   (case when ss.shipping_end_fact_datetime 
	   			 > si.shipping_plan_datetime then 1 else 0 end) as is_delay,
	   (case when ss.status = 'finished' then 1 else 0 end) as is_shipping_finish,
	   (case when ss.shipping_end_fact_datetime 
	   			 > si.shipping_plan_datetime then date_part('day',ss.shipping_end_fact_datetime 
	   			   - shipping_plan_datetime) else 0 end) as delay_day_at_shipping,
	   si.payment_amount,
	   si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate 
	   						+ st.shipping_transfer_rate) as vat,
	   si.payment_amount * sa.agreement_commission as profit
from shipping_status ss
inner join shipping_info si using(shipping_id)
inner join shipping_transfer st on si.shipping_transfer_id = st.id
inner join shipping_country_rates scr on si.shipping_country_rate_id = scr.id
inner join shipping_agreement sa on si.shipping_agreement_id = sa.agreement_id;