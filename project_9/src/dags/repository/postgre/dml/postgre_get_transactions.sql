select 
    operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
from 
    public.transactions
where
    transaction_dt::date = %(dag_start_dt)s