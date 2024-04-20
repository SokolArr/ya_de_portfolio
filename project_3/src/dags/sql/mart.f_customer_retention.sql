--clear period before insert
delete from mart.f_customer_retention 
where period_id = (
	select week_of_year
	from mart.d_calendar
	where date_actual::Date = '{{ds}}'
);
--insert increment
insert into mart.f_customer_retention
with new_cl as
(	-- new_customers_count
	select week_of_year, item_id, count(*) as new_customers_count
	from 
	(   
		select uniq_id, item_id, week_of_year, count(*) cnt_orders_per_year_week
		from staging.user_order_log uol
			left join mart.d_calendar as dc 
			on uol.date_time::Date = dc.date_actual
		where week_of_year = 
		(	-- get period by ds
			select week_of_year
			from mart.d_calendar
			where date_actual::Date = '{{ds}}'
		)
		group by uniq_id, week_of_year
		having count(*) = 1
	) new_cl_orders_per_week
	group by week_of_year, item_id
),
returning_cl as 
(   -- returning_customers_count
	select week_of_year, item_id, count(*) as returning_customers_count 
	from 
	(   
		select uniq_id, item_id, week_of_year, count(*) cnt_orders_per_year_week
		from staging.user_order_log uol
			left join mart.d_calendar as dc 
			on uol.date_time::Date = dc.date_actual
		where week_of_year = 
		(	-- get period by ds
			select week_of_year
			from mart.d_calendar
			where date_actual::Date = '{{ds}}'
		)
		group by uniq_id, week_of_year
		having count(*) > 1
	) ret_cl_orders_per_week 
	group by week_of_year, item_id
),
refunded_cl as 
(   -- refunded_customer_count
	select week_of_year, item_id, count(*) as refunded_customer_count
	from
	(
		select week_of_year, uniq_id, item_id
		from staging.user_order_log uol
			left join mart.d_calendar as dc 
			on uol.date_time::Date = dc.date_actual
		where status = 'refunded' and 
		week_of_year = 
		(	-- get period by ds
			select week_of_year
			from mart.d_calendar
			where date_actual::Date = '{{ds}}'
		)
		group by week_of_year, uniq_id, item_id
	) ref_cl_orders_per_week
	group by week_of_year, item_id
),
new_customers_revenue as 
(   -- new_customers_revenue
	select week_of_year, item_id, sum(sum_payment_amount) as new_customers_revenue
	from 
	(   
		select uniq_id, item_id, week_of_year, 
		count(*) cnt_orders_per_year_week, 
		sum(payment_amount) as sum_payment_amount
		from staging.user_order_log uol
			left join mart.d_calendar as dc 
			on uol.date_time::Date = dc.date_actual
		where week_of_year = 
		(	-- get period by ds
			select week_of_year
			from mart.d_calendar
			where date_actual::Date = '{{ds}}'
		)
		group by uniq_id, item_id, week_of_year
		having count(*) = 1
	) sum_pa_new_cl_orders_per_week
	group by week_of_year, item_id
),
returning_customers_revenue as 
(   -- returning_customers_revenue
	select week_of_year, item_id, sum(sum_payment_amount) as returning_customers_revenue
	from 
	(   
		select uniq_id, item_id, week_of_year, 
		count(*) cnt_orders_per_year_week, 
		sum(payment_amount) as sum_payment_amount
		from staging.user_order_log uol
			left join mart.d_calendar as dc 
			on uol.date_time::Date = dc.date_actual
		where week_of_year = 
		(	-- get period by ds
			select week_of_year
			from mart.d_calendar
			where date_actual::Date = '{{ds}}'
		)
		group by uniq_id, week_of_year
		having count(*) > 1
	) sum_pa_ret_cl_orders_per_week
	group by week_of_year, item_id
),
customers_refunded as 
(	-- customers_refunded
	select week_of_year, item_id, count(*) as customers_refunded
	from staging.user_order_log uol
		left join mart.d_calendar as dc 
		on uol.date_time::Date = dc.date_actual
	where status = 'refunded' and 
	week_of_year = 
	(	-- get period by ds
		select week_of_year
		from mart.d_calendar
		where date_actual::Date = '{{ds}}'
	)
	group by week_of_year, item_id
)
select
	coalesce(new_customers_count, 0) as new_customers_count,
	coalesce(returning_customers_count, 0) as returning_customers_count,
	coalesce(refunded_customer_count, 0) as refunded_customer_count,
	'weekly' as period_name,
	nc.week_of_year as period_id,
	nc.item_id,
	coalesce(new_customers_revenue, 0) as new_customers_revenue,
	coalesce(returning_customers_revenue, 0) as returning_customers_revenue,
	coalesce(customers_refunded, 0) as customers_refunded
from new_cl nc
left join returning_cl rtc
on 
	nc.week_of_year = rtc.week_of_year and 
	nc.item_id = rtc.item_id
left join refunded_cl rfc
on 
	nc.week_of_year = rfc.week_of_year and 
	nc.item_id = rfc.item_id
left join new_customers_revenue ncr
on 
	nc.week_of_year = ncr.week_of_year and 
	nc.item_id = ncr.item_id
left join returning_customers_revenue rtcr
on 
	nc.week_of_year = rtcr.week_of_year and 
	nc.item_id = rtcr.item_id
left join customers_refunded crf
on 
	nc.week_of_year = crf.week_of_year and 
	nc.item_id = crf.item_id
order by period_id, item_id;