delete from 
    STV2023121136__DWH.global_metrics
where
    date_update::date = %s;