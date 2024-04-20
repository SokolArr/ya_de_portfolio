delete from 
    STV2023121136__DWH.transactions
where
    transaction_dt::date = %s;