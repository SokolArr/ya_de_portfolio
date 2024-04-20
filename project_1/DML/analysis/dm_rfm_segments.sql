insert into analysis.dm_rfm_segments (
	user_id,
	recency,
	frequency,
	monetary_value
)
select 
	user_id,
	recency,
	frequency,
	monetary_value 
from 
	analysis.tmp_rfm_recency r
inner join 
	analysis.tmp_rfm_frequency f using(user_id)
inner join 
	analysis.tmp_rfm_monetary_value mv using(user_id)
order by 
	user_id;