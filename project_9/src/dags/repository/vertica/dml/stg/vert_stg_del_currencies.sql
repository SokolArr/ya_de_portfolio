delete from 
    STV2023121136__STAGING.currencies
where
    date_update::date = %s;