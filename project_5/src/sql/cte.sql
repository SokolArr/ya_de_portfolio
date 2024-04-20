-- user_group_messages
with user_group_messages as (
    select
    gd.hk_group_id,
    count(distinct hk_user_id) as cnt_users_in_group_with_messages
	from STV2023121136__DWH.l_groups_dialogs gd 
	inner join STV2023121136__DWH.l_user_message um on gd.hk_message_id = um.hk_message_id
    group by hk_group_id
)
select 
hk_group_id,
cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;

/*
|hk_group_id              |cnt_users_in_group_with_messages|
|-------------------------|--------------------------------|
|3 573 821 581 643 801 282|5                               |
|6 210 160 447 255 813 510|36                              |
|8 764 442 430 478 790 635|49                              |
|1 319 692 034 796 300 830|69                              |
|4 038 157 487 054 977 717|80                              |
|6 640 162 946 765 271 748|83                              |
|1 594 722 103 625 592 852|96                              |
|5 504 994 290 564 188 728|130                             |
|7 271 205 554 897 122 496|144                             |
|4 651 097 343 657 001 468|148                             |
*/

-- user_group_log
with user_group_log as (
    select 
    luga.hk_group_id,
    count(distinct hk_user_id) as cnt_added_users
    from 
    STV2023121136__DWH.l_user_group_activity luga
    inner join STV2023121136__DWH.s_auth_history sah using(hk_l_user_group_activity)
    where sah.event = 'add'
    group by hk_group_id
)
select hk_group_id,
cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;

/*
|hk_group_id              |cnt_added_users|
|-------------------------|---------------|
|3 573 821 581 643 801 282|19             |
|4 038 157 487 054 977 717|131            |
|6 210 160 447 255 813 510|153            |
|1 594 722 103 625 592 852|168            |
|1 319 692 034 796 300 830|202            |
|6 640 162 946 765 271 748|208            |
|5 504 994 290 564 188 728|237            |
|3 270 802 980 963 574 263|283            |
|6 413 166 516 071 255 877|328            |
|7 271 205 554 897 122 496|329            |
*/

-- user_group_log, user_group_messages
with user_group_log as (
select 
    luga.hk_group_id,
    count(distinct hk_user_id) as cnt_added_users
    from 
    STV2023121136__DWH.l_user_group_activity luga
    inner join STV2023121136__DWH.s_auth_history sah using(hk_l_user_group_activity)
    where sah.event = 'add'
    group by hk_group_id
)
,user_group_messages as (
    select
    gd.hk_group_id,
    count(distinct hk_user_id) as cnt_users_in_group_with_messages
	from STV2023121136__DWH.l_groups_dialogs gd 
	inner join STV2023121136__DWH.l_user_message um on gd.hk_message_id = um.hk_message_id
    group by hk_group_id
)
select ugl.hk_group_id,
	   ugl.cnt_added_users,
	   ugm.cnt_users_in_group_with_messages,
	   ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
inner join STV2023121136__DWH.h_groups hg on ugl.hk_group_id = hg.hk_group_id
order by 
hg.registration_dt asc,
ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc
limit 10;

/*
|hk_group_id              |cnt_added_users|cnt_users_in_group_with_messages|group_conversion|
|-------------------------|---------------|--------------------------------|----------------|
|7 757 992 142 189 260 835|2 505          |1 136                           |0,453493014     |
|6 014 017 525 933 240 454|3 405          |1 778                           |0,5221732746    |
|7 279 971 728 630 971 062|1 914          |861                             |0,4498432602    |
|5 568 963 519 328 366 880|4 298          |2 387                           |0,5553745928    |
|206 904 954 090 724 337  |4 311          |2 448                           |0,5678496868    |
|9 183 043 445 192 227 260|3 725          |2 045                           |0,5489932886    |
|7 174 329 635 764 732 197|4 794          |2 757                           |0,5750938673    |
|2 461 736 748 292 367 987|3 575          |1 967                           |0,5502097902    |
|3 214 410 852 649 090 659|3 781          |2 126                           |0,5622851098    |
|4 350 425 024 258 480 878|4 172          |2 360                           |0,5656759348    |
*/