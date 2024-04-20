-- ALTER TABLE mart.f_sales DROP CONSTRAINT f_sales_customer_id_fkey;
-- clear before insert
delete
from mart.d_customer
where customer_id in
(
	select distinct customer_id 
	from staging.user_order_log 
	where date_time::Date = '{{ds}}'
);
--insert increment
insert into mart.d_customer (customer_id, first_name, last_name, city_id)
select customer_id, first_name, last_name, max(city_id) from staging.user_order_log
where customer_id not in (select customer_id from mart.d_customer)
group by customer_id, first_name, last_name;