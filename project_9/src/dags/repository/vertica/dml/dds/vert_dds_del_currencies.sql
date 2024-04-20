delete from 
    STV2023121136__DWH.currencies
where
    date_update::date = %s;