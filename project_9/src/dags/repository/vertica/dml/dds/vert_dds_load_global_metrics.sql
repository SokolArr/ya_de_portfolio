insert into STV2023121136__DWH.global_metrics(
    id,
	date_update,
	currency_from,
	amount_total,
	cnt_transactions,
	avg_transactions_per_account,
	cnt_accounts_make_transactions
)
select
	hash(total_tr.transaction_dt, total_tr.currency_code),
	total_tr.transaction_dt as date_update,
	total_tr.currency_code as currency_from,
	round(amount_total * coalesce(currency_with_div, 1), 5) as amount_total,
	total_tr.cnt_transactions,
	round(total_tr_acc.avg_transactions_per_account, 5) as avg_transactions_per_account,
	total_tr_acc.cnt_accounts_make_transactions
from(
	select -- получаю агрегат по транзакциям
		transaction_dt::date,
		currency_code,
		sum(amount) as amount_total,
		count(*) as cnt_transactions
	from 
		STV2023121136__DWH.transactions
	where
		status = 'done' -- беру только завершенные транзакции
	group by
		currency_code,
		transaction_dt::date
) total_tr
left join(
	select  -- получаю актуальный курс пары (валюта транзакции, доллар)
		date_update,
		currency_code,
		currency_with_div
	from
		STV2023121136__DWH.currencies cur
	where 
		currency_code_with = 420 -- в доллар
) curs
on 
	total_tr.transaction_dt = curs.date_update
	and total_tr.currency_code = curs.currency_code
inner join (
	select -- получаю агрегат транзакций в разрезе аккаунта отправителя
		transaction_dt,
		currency_code,
		sum(tr_per_acc)/count(*) as avg_transactions_per_account,
		count(account_number_from) as cnt_accounts_make_transactions
	from(
		select
			transaction_dt::date,
			currency_code,
			account_number_from,
			count(account_number_from) as tr_per_acc
		from 
			STV2023121136__DWH.transactions
		where
			status = 'done'
		group by
			transaction_dt::date,
			currency_code,
			account_number_from
	) tr_per_acc
	group by
		transaction_dt,
		currency_code
) total_tr_acc
on
	total_tr.transaction_dt = total_tr_acc.transaction_dt
	and total_tr.currency_code = total_tr_acc.currency_code
where
    total_tr.transaction_dt::date = %s;