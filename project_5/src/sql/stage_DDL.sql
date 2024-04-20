create table STV2023121136__STAGING.users
(
    id      int PRIMARY KEY,
    chat_name varchar(200),
    registration_dt datetime,
    country varchar(200),
    age int
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;

create table STV2023121136__STAGING.groups
(
    id      int PRIMARY KEY,
    admin_id int,
    group_name varchar(100),
    registration_dt datetime,
    is_private boolean
)
order by id, admin_id
SEGMENTED BY hash(id) all nodes
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

create table STV2023121136__STAGING.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   datetime,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int
)
order by message_id
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

create table STV2023121136__STAGING.group_log(
	group_id int not null,
	user_id int,
	user_id_from int,
	event varchar(200),
	event_dt timestamp
)
ORDER BY group_id
SEGMENTED BY HASH(group_id) ALL NODES;