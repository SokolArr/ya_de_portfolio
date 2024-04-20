insert into STV2023121136__DWH.currencies(
    hk_currency_code_id,
	date_update,
	currency_code,
	currency_code_with,
	currency_with_div
)
select 
	hash(currency_code, currency_code_with, date_update),
	date_update::date,
	currency_code::int,
	currency_code_with::int,
	currency_with_div::numeric(12,4)
from 
	STV2023121136__STAGING.currencies
where
    date_update::date = %s;