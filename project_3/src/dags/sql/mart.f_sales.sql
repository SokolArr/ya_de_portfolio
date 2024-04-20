-- clear before insert
delete
from mart.f_sales
where id in 
(
	select fss.id 
	from mart.f_sales fss
		inner join mart.d_calendar dc using(date_id)
	where dc.date_actual::Date = '{{ds}}'
);
-- insert increment
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, 
case 
	when status = 'refunded' then 0 - payment_amount
	else payment_amount
end as payment_amount
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';