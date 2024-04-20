-- hubs
INSERT INTO STV2023121136__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023121136__STAGING.users
where hash(id) not in (select hk_user_id from STV2023121136__DWH.h_users);

INSERT INTO STV2023121136__DWH.h_groups(hk_group_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2023121136__STAGING.groups
where hash(id) not in (select hk_group_id from STV2023121136__DWH.h_groups);

INSERT INTO STV2023121136__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2023121136__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV2023121136__DWH.h_dialogs);

-- linkers
INSERT INTO STV2023121136__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
hash(hg.hk_group_id,hu.hk_user_id),
hg.hk_group_id,
hu.hk_user_id,
now() as load_dt,
's3' as load_src
from STV2023121136__STAGING.groups as g
left join STV2023121136__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV2023121136__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV2023121136__DWH.l_admins);

INSERT INTO STV2023121136__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id,hk_group_id,load_dt,load_src)
select
hash(hd.hk_message_id,hg.hk_group_id),
hd.hk_message_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV2023121136__STAGING.dialogs as d
left join STV2023121136__DWH.h_dialogs hd on d.message_id = hd.message_id
left join STV2023121136__DWH.h_groups hg on  d.message_group = hg.group_id
where d.message_group is not null and 
	hash(hd.hk_message_id,hg.hk_group_id) not in (select hk_l_groups_dialogs from STV2023121136__DWH.l_groups_dialogs);

INSERT INTO STV2023121136__DWH.l_user_message(hk_l_user_message, hk_user_id,hk_message_id,load_dt,load_src)
select
hash(hd.hk_message_id,hu.hk_user_id),
hu.hk_user_id,
hd.hk_message_id,
now() as load_dt,
's3' as load_src
from STV2023121136__STAGING.dialogs as d
left join STV2023121136__DWH.h_users as hu on d.message_from = hu.user_id
left join STV2023121136__DWH.h_dialogs as hd on d.message_id = hd.message_id
where  hash(hd.hk_message_id,hu.hk_user_id) not in (select hk_l_user_message from STV2023121136__DWH.l_user_message) and hu.hk_user_id is not null;

INSERT INTO STV2023121136__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select 
distinct
hash(hu.hk_user_id,hg.hk_group_id),
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from STV2023121136__STAGING.group_log as gl
left join STV2023121136__DWH.h_users hu on gl.user_id = hu.user_id 
left join STV2023121136__DWH.h_groups hg on gl.group_id = hg.group_id 
where hash(hu.hk_user_id,hg.hk_group_id) not in (select hk_l_user_group_activity from STV2023121136__DWH.l_user_group_activity);


--sattelites
INSERT INTO STV2023121136__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id
,True as is_admin
,hg.registration_dt
,now() as load_dt
,'s3' as load_src
from STV2023121136__DWH.l_admins as la
left join STV2023121136__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

INSERT INTO STV2023121136__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
select hk_group_id
        ,group_name

        ,now() as load_dt
       ,'s3' as load_src
from STV2023121136__STAGING.groups as g
left join STV2023121136__DWH.h_groups as hg on g.id = hg.group_id;

INSERT INTO STV2023121136__DWH.s_group_private_status(hk_group_id, is_private,load_dt,load_src)
select hk_group_id
        ,is_private

        ,now() as load_dt
       ,'s3' as load_src
from STV2023121136__STAGING.groups as g
left join STV2023121136__DWH.h_groups as hg on g.id = hg.group_id;

INSERT INTO STV2023121136__DWH.s_dialog_info(hk_message_id, message,message_from,message_to,load_dt,load_src)
select hk_message_id
        ,message
        ,message_from
        ,message_to
        ,now() as load_dt
       ,'s3' as load_src
from STV2023121136__STAGING.dialogs as d
left join STV2023121136__DWH.h_dialogs as hg on d.message_id = hg.message_id;

INSERT INTO STV2023121136__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
select hk_user_id
        ,country
        ,age
        ,now() as load_dt
       ,'s3' as load_src
from STV2023121136__STAGING.users as u
left join STV2023121136__DWH.h_users as hg on u.id = hg.user_id;

INSERT INTO STV2023121136__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)

select hk_user_id
        ,chat_name
        ,now() as load_dt
       ,'s3' as load_src
from STV2023121136__STAGING.users as u
left join STV2023121136__DWH.h_users as hg on u.id = hg.user_id;

INSERT INTO STV2023121136__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
select 
luga.hk_l_user_group_activity,
gl.user_id_from,
gl.event,
gl.event_dt,
now() as load_dt,
's3' as load_src
from STV2023121136__STAGING.group_log as gl
left join STV2023121136__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023121136__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023121136__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;