-- ALTER TABLE mart.f_sales DROP CONSTRAINT f_sales_item_id_fkey;
-- clear before insert
delete
from mart.d_item
where item_id in 
(
	select distinct item_id 
	from staging.user_order_log
	where date_time::Date = '{{ds}}'
);
--insert increment
insert into mart.d_item (item_id, item_name)
select item_id, item_name from staging.user_order_log
where item_id not in (select item_id from mart.d_item)
group by item_id, item_name;