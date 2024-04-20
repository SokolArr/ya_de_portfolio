--hubs
create table STV2023121136__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id int,
 	message_ts timestamp,
 	load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- linkers
drop table if exists STV2023121136__DWH.l_user_message;

create table STV2023121136__DWH.l_user_message
(
hk_l_user_message bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV2023121136__DWH.h_users (hk_user_id),
hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV2023121136__DWH.h_dialogs (hk_message_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table STV2023121136__DWH.l_groups_dialogs;
create table STV2023121136__DWH.l_groups_dialogs
(
hk_l_groups_dialogs bigint primary key,
hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES STV2023121136__DWH.h_dialogs (hk_message_id),
hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES STV2023121136__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.l_admins
(
hk_l_admin_id bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES STV2023121136__DWH.h_users (hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_admins_group REFERENCES STV2023121136__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES STV2023121136__DWH.h_users(hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES STV2023121136__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--sattelites
drop table if exists STV2023121136__DWH.s_admins;
create table STV2023121136__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV2023121136__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.s_group_name
(
    hk_group_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV2023121136__DWH.h_groups (hk_group_id),
    group_name varchar (100),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.s_group_private_status
(
    hk_group_id bigint not null CONSTRAINT fk_s_group_private_status REFERENCES STV2023121136__DWH.h_groups (hk_group_id),
    is_private boolean,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.s_dialog_info
(
    hk_message_id bigint not null CONSTRAINT fk_s_dialog_info REFERENCES STV2023121136__DWH.h_dialogs (hk_message_id),
    message varchar(1000),
    message_from int,
    message_to int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.s_user_socdem
(
    hk_user_id bigint not null CONSTRAINT s_user_socdem REFERENCES STV2023121136__DWH.h_users (hk_user_id),
    country varchar(100),
    age int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2023121136__DWH.s_user_chatinfo
(
    hk_user_id bigint not null CONSTRAINT s_user_chatinfo REFERENCES STV2023121136__DWH.h_users (hk_user_id),
    chat_name varchar(1000),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121136__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2023121136__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(200),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);