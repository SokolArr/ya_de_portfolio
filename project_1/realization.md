# 1. Витрина RFM
## 1.1. Требования к целевой витрине
Витрина  RFM-классификации `dm_rfm_segments` располагается в базе de в схеме `analysis`, в ней учтены только успешно выполненные заказы.\
Сруктура витрины:
- `user_id` - уникальный id пользователя;
- `recency` - фактор "сколько времени прошло с момента последнего заказа" по которому ставится оценка от 1 до 5, где 1 - получат те, кто либо вообще не делал заказов, либо делал их очень давно;
- `frequency` -  фактор "количество заказов" по которому ставится оценка от 1 до 5, где 1 - получат клиенты с наименьшим количеством заказов;
- `monetary_value` - фактор "сумма затрат клиента" по которому ставится оценка от 1 до 5, где 1 - получат клиенты с наименьшей суммой затрат.
Данные в витрине с начала 2022 года.

## 1.2. Структура исходных данных
В базе две схемы: `production` и `analysis`.\
В схеме `production` содержатся оперативные таблицы: 
- `orderitems`
- `orders`
- `orderstatuses`
- `orderstatuslog`
- `products`
- `users`
 
Для расчета атрибута `recency` витрины понадобится атрибут `order_ts` таблицы `orders` в схеме `production`.\
Для расчета атрибута `frequency` понадобится сам факт успешно завершенного заказа из связки таблиц `orders` и `orderstatuses` т.е. заказ со статусом `Closed`\
Для расчета атрибута `monetary_value` понадобится также статус `Closed` и просумированный атрибут `payment` в разрезе каждого пользователя.


## 1.3. Качество данных
### Проверки
+ Проверка на дубли в данных - *не показала задублированных значений по ключам таблиц;*
+ Проверка на пропущенные значения в важных полях - *не показала пропущенных значений;*
+ Проверка на некорректные типы данных - *не показала несоответствие типов данных и значений.*

*Вывод: в представленных таблицах хранятся качественные данные.*

### Инструменты обеспечивающие качество данных в источнике
| Таблицы             | Объект                      | Инструмент      | Для чего используется |
| ------------------- | --------------------------- | --------------- | --------------------- |
| production.Orderitems | discount, price numeric(19,5) NOT NULL | CHECK  | Обеспечивает проверку на discount >= 0 и discount <= price|
| production.Orderitems | price numeric(19,5) NOT NULL | CHECK  | Обеспечивает проверку на price >= 0|
| production.Orderitems | quantity int NOT NULL| CHECK  | Обеспечивает проверку на quantity > 0|
| production.Orderitems | order_id, product_id int NOT NULL | UNIQUE KEY  | Обеспечивает уникальность пары ключей order_id и product_id|
| production.Orderitems | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о заказанных товаров |
| production.Orderitems | order_id int NOT NULL FOREIGN KEY | FOREIGN KEY  | Обеспечивает внешнюю связь с сущностью oreders по ключу order_id |
| production.Orderitems | product_id int NOT NULL FOREIGN KEY | FOREIGN KEY  | Обеспечивает внешнюю связь с сущностью products по ключу product_id |
| production.Orderitems | order_id, product_id int NOT NULL | INDEX  | Обеспечивает индекс по нескольким полям order_id, product_id|
| production.Orderitems | id int NOT NULL PRIMARY KEY | INDEX  | Обеспечивает индексацию таблицы по ключу id |
| production.Orders | bonus_payment, payment, cost numeric(19,5) NOT NULL | CHECK  | Обеспечивает проверку чтобы цена была суммой платежа и бонусного платежа cost = (payment + bonus_payment) |
| production.Orders | order_id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о заказах |
| production.Orders | order_id int NOT NULL PRIMARY KEY | INDEX  | Обеспечивает индексацию таблицы по ключу id |
| production.Orderstatuses | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о статусах |
| production.Orderstatuses | id int NOT NULL PRIMARY KEY | INDEX  | Обеспечивает индексацию таблицы по ключу id |
| production.Orderstatuslog | order_id, status_id int NOT NUL| UNIQUE KEY | Обеспечивает уникальность записей о id заказов и id их статусов |
| production.Orderstatuslog | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность id логов заказов |
| production.Orderstatuslog | id int NOT NULL PRIMARY KEY | INDEX  | Обеспечивает индексацию таблицы по ключу id |
| production.Orderstatuslog | order_id, status_id int NOT NUL| INDEX | Обеспечивает индекс по нескольким полям order_id, status_id |
| production.Products | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность записей о товарах |
| production.Products | price numeric(19,5) NOT NULL | CHECK | Обеспечивает проверку положительной цены price >= 0 |
| production.Products | id int NOT NULL PRIMARY KEY | INDEX  | Обеспечивает индексацию таблицы по ключу id |
| production.Users | id int NOT NULL PRIMARY KEY | Первичный ключ  | Обеспечивает уникальность id пользователей |
| production.Users | id int NOT NULL PRIMARY KEY | INDEX  | Обеспечивает индексацию таблицы по ключу id |

## 1.4. Разработка витрины
### 1.4.1. Скрипт создания представлений из базы production
[project_1/DDL/analysis/views/all_views.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DDL/analysis/views/all_views.sql)
```SQL
create or replace view analysis.users as 
select * from production.users;

create or replace view analysis.orderitems as 
select * from production.orderitems;

create or replace view analysis.orderstatuses as 
select * from production.orderstatuses;

create or replace view analysis.products as 
select * from production.products;

create or replace view analysis.orders as 
select * from production.orders;
```

### 1.4.2. Скрипт создания витрин в схеме `analysis`
#### dm_rfm_segments
[project_1/DDL/analysis/tables/dm_rfm_segments.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DDL/analysis/tables/dm_rfm_segments.sql)
```SQL
CREATE TABLE analysis.dm_rfm_segments (
    user_id int4 UNIQUE NOT NULL,
    recency int4 NOT NULL CHECK(recency >= 1 AND recency <= 5),
    frequency int4 NOT NULL CHECK(frequency >= 1 AND frequency <= 5),  
    monetary_value int4 NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
```

---
#### tmp_rfm_recency
[project_1/DDL/analysis/tables/tmp_rfm_recency.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DDL/analysis/tables/tmp_rfm_recency.sql)
```SQL
CREATE TABLE analysis.tmp_rfm_recency (
	user_id INT NOT NULL PRIMARY KEY,
	recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
```

---
#### tmp_rfm_frequency
[project_1/DDL/analysis/tables/tmp_rfm_frequency.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DDL/analysis/tables/tmp_rfm_frequency.sql)
```SQL
CREATE TABLE analysis.tmp_rfm_frequency (
 user_id INT NOT NULL PRIMARY KEY,
 frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
```

---
#### tmp_rfm_monetary_value
[project_1/DDL/analysis/tables/tmp_rfm_monetary_value.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DDL/analysis/tables/tmp_rfm_monetary_value.sql)
```SQL
CREATE TABLE analysis.tmp_rfm_monetary_value (
 user_id INT NOT NULL PRIMARY KEY,
 monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
```

### 1.4.3. DML скрипт для заполнения витрины в схеме `analysis`
#### tmp_rfm_recency
[project_1/DML/analysis/tmp_rfm_recency.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DML/analysis/tmp_rfm_recency.sql)
```SQL
insert into analysis.tmp_rfm_recency(
	user_id, 
	recency
)
select 
	user_id, 
	recency::int4
from (
	select 
		u.id as user_id, 
		last_order, 
		ntile(5) over(order by last_order nulls FIRST) as recency
	from(
		select 
			user_id, 
			max(order_ts) as last_order
		from 
			analysis.orders
		where 
			status = (select id from analysis.orderstatuses where key = 'Closed')
		group by 
			user_id
	) o
	right join 
		analysis.users u 
	on 
		o.user_id = u.id
	order by 3 desc
) as to_insert;
```

---
#### tmp_rfm_frequency
[project_1/DML/analysis/tmp_rfm_frequency.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DML/analysis/tmp_rfm_frequency.sql)
```SQL
insert into analysis.tmp_rfm_frequency (
	user_id, 
	frequency
)
select 
	user_id, 
	frequency::int4 
from (
	select u.id as user_id, 
		coalesce (order_cnt, 0) as order_cnt, 
		ntile(5) over(order by order_cnt nulls FIRST) as frequency
	from(
		select 
			user_id, 
			count(*) as order_cnt
		from 
			analysis.orders
		where 
			status = (select id from analysis.orderstatuses where key = 'Closed')
		group by 
			user_id
	) o
	right join 
		analysis.users u 
	on 
		o.user_id = u.id
	order by 3 desc
) as to_insert;
```

---
#### tmp_rfm_monetary_value
[project_1/DML/analysis/tmp_rfm_monetary_value.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DML/analysis/tmp_rfm_monetary_value.sql)
```SQL
insert into analysis.tmp_rfm_monetary_value (
	user_id, 
	monetary_value
)
select 
	user_id, 
	monetary_value::int4 
from (
	select u.id as user_id, 
		coalesce (payment_sum, 0 ) as payment_sum,
		ntile(5) over(order by payment_sum nulls FIRST) as monetary_value
	from(
		select 
			user_id, 
			sum(payment) as payment_sum
		from 
			analysis.orders
		where 
			status = (select id from analysis.orderstatuses where key = 'Closed')
		group by 
			user_id
	) o
	right join 
		analysis.users u 
	on 
		o.user_id = u.id
	order by 3 desc
) as to_insert;
```

---
#### dm_rfm_segments
[project_1/DML/analysis/dm_rfm_segments.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DML/analysis/dm_rfm_segments.sql)
```SQL
insert into analysis.dm_rfm_segments (
	user_id,
	recency,
	frequency,
	monetary_value
)
select 
	user_id,
	recency,
	frequency,
	monetary_value 
from 
	analysis.tmp_rfm_recency r
inner join 
	analysis.tmp_rfm_frequency f using(user_id)
inner join 
	analysis.tmp_rfm_monetary_value mv using(user_id)
order by 
	user_id;
```

---
#### Результат, `limit 10`:
|user_id|recency|frequency|monetary_value|
|-------|-------|---------|--------------|
|0|1|3|4|
|1|4|3|3|
|2|2|3|5|
|3|2|3|3|
|4|4|3|3|
|5|5|5|5|
|6|1|3|5|
|7|4|2|2|
|8|1|1|3|
|9|1|3|2|

# 2. Доработка представлений
#### Изменения для потеряного `status`, переключение на `OrderStatusLog`
Из описания задания: 
> необходимо внести изменения в то, как формируется представление analysis.Orders: вернуть в него поле status. Значение в этом поле должно соответствовать последнему по времени статусу из таблицы production.OrderStatusLog.

[project_1/DDL/analysis/views/orders_rework.sql](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1/DDL/analysis/views/orders_rework.sql)
```SQL
CREATE OR REPLACE VIEW analysis.orders
as select 
	o.order_id,
	o.order_ts,
	o.user_id,
	o.bonus_payment,
	o.payment,
	o."cost",
	o.bonus_grant, 
	order_statuses.status_id as status
from 
	production.orders o
inner join( 
	select 
		osl.order_id, 
		osl.status_id
	from (
		select 
			order_id, 
			max(dttm) max_dttm
		from 
			production.OrderStatusLog 
		group by 
			order_id
	) as order_max_dttm 
	inner join 
		production.OrderStatusLog osl
	on 
		order_max_dttm.max_dttm = osl.dttm
) as order_statuses
using(order_id);
```