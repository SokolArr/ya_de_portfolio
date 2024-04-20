delete from cdm.dm_courier_ledger
where settlement_year = extract(year from '{{ds}}'::timestamp) 
	  and settlement_month = extract(month from '{{ds}}'::timestamp)
;
insert into cdm.dm_courier_ledger (courier_id,courier_name,settlement_year,settlement_month,orders_count,orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)
select  courier_id,
		name as courier_name,
		year as settlement_year,
		month as settlement_month,
		orders_count,
		orders_total_sum,
		rate as rate_avg,
		(orders_total_sum * 0.25) as order_processing_fee,
		courier_order_sum,
		tip_sum as courier_tips_sum,
		((courier_order_sum + tip_sum) * 0.95) as courier_reward_sum
from cdm.courier_agg_with_rates
where year = extract(year from '{{ds}}'::timestamp) 
	  and month = extract(month from '{{ds}}'::timestamp)
order by 1,3,4;