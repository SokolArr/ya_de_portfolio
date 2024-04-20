select 
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
from 
    public.currencies
where
    date_update::date = %(dag_start_dt)s