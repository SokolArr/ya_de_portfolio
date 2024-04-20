delete from 
    STV2023121136__STAGING.transactions
where
    transaction_dt::date = %s;