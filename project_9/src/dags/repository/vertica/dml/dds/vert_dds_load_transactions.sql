insert into STV2023121136__DWH.transactions(
	hk_operation_id,
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
)
select 
	hash(operation_id, transaction_dt),
	operation_id::uuid,
	account_number_from::bigint,
	account_number_to::bigint,
	currency_code::int,
	country::varchar,
	status::varchar,
	transaction_type::varchar,
	amount::bigint,
	transaction_dt::timestamp
from 
	STV2023121136__STAGING.transactions
where
    transaction_dt::date = %s
    and (account_number_from >= 0 and account_number_to >= 0);