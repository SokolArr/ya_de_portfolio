# Проект 3-го спринта
## Описание
## Этап 1
#### **Исходные данные**

Новые инкременты с информацией о продажах приходят по API, спецификация к которому лежит ниже, и содержат статус заказа (`shipped`/`refunded`).

---

#### **Спецификация API**

**POST /generate_report**

Метод `/generate_report` инициализирует формирование отчёта. В заголовке запроса нужно указать:

- `X-Nickname` — ваш никнейм (например, sashanikonov).
- `X-Project` со значением `True` — так мы узнаем, что вы выполняете итоговый проект.
- `X-Cohort` со значением номера вашей когорты (например, 1).
- `X-API-KEY` со значением `5f55e6c0-e9e5-4a9c-b313-63c01fc31460`. Если не указать верный ключ к API, то вместо ожидаемого результата выйдет ошибка доступа.

Установите предварительно утилиту curl, чтобы с помощью неё обратиться к API. Выполните следующие команды в командной строке:

```
curl --location --request POST 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report' \
--header 'X-Nickname: {{ your_nickname }}' \
--header 'X-Cohort: {{ your_cohort_number }}' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' \
--data-raw '' 
```

В качестве переменных, указанных в `{{ }}`, передайте ваши актуальные данные.

Метод возвращает `task_id` — ID задачи, в результате выполнения которой должен сформироваться отчёт.

**GET /get_report**

Метод `get_report` используется для получения отчёта после того, как он будет сформирован на сервере.

С помощью утилиты curl обратитесь к API:

```
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id={{ task_id }}' \
--header 'X-Nickname: {{ your_nickname }}' \
--header 'X-Cohort: {{ your_cohort_number }}' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' 
```

Пока отчёт будет формироваться, будет возвращаться статус `RUNNING`.

Если отчёт сформирован, то метод вернёт статус `SUCCESS` и `report_id`.

Сформированный отчёт содержит четыре файла:

- `customer_research.csv`,
- `user_order_log.csv`,
- `user_activity_log.csv`,
- `price_log.csv`.

Файлы отчетов можно получить по URL из параметра `s3_path` или сформировать URL самостоятельно по следующему шаблону:

```
https://storage.yandexcloud.net/s3-sprint3/cohort_{{ your_cohort_number }}/{{ your_nickname }}/project/{{ report_id }}/{{ file_name }} 
```

**GET /get_increment**

Метод `get_increment` используется для получения данных за те даты, которые не вошли в основной отчёт. Дата обязательно в формате `2020-01-22T00:00:00`.

С помощью утилиты curl обратитесь к API:

```
curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id={{ report_id }}&date={{ date }}' \
--header 'X-Nickname: {{ your_nickname }}' \
--header 'X-Cohort: {{ your_cohort_number }}' \
--header 'X-Project: True' \
--header 'X-API-KEY: {{ api_key }}' 
```

Если инкремент сформирован, то метод вернёт статус `SUCCESS` и `increment_id`. Если инкремент не сформируется, то вернётся `NOT FOUND` с описанием причины.

Сформированный инкремент содержит три файла:

- `customer_research_inc.csv`,
- `user_order_log_inc.csv`,
- `user_activity_log_inc.csv`.

Файлы отчетов можно получить по URL из параметра `s3_path` или сформировать URL самостоятельно по следующему шаблону:

Скопировать кодJSON

```
https://storage.yandexcloud.net/s3-sprint3/cohort_{{ your_cohort_number }}/{{ your_nickname }}/project/{{ increment_id }}/{{ file_name }} 
```

## Этап 2

Витрина должна отражать следующую информацию:

- Рассматриваемый период — weekly.
- Возвращаемость клиентов:
    - `new` — кол-во клиентов, которые оформили один заказ за рассматриваемый период;
    - `returning` — кол-во клиентов, которые оформили более одного заказа за рассматриваемый период;
    - `refunded` — кол-во клиентов, которые вернули заказ за рассматриваемый период.
- Доход (`revenue`) и `refunded` для каждой категории покупателей.

Благодаря витрине можно будет выяснить, какие категории товаров лучше всего удерживают клиентов.

Витрина `mart.f_customer_retention`:

Скопировать код

```
mart.f_customer_retention
1. new_customers_count — кол-во новых клиентов (тех, которые сделали только один 
заказ за рассматриваемый промежуток времени).
2. returning_customers_count — кол-во вернувшихся клиентов (тех,
которые сделали только несколько заказов за рассматриваемый промежуток времени).
3. refunded_customer_count — кол-во клиентов, оформивших возврат за 
рассматриваемый промежуток времени.
4. period_name — weekly.
5. period_id — идентификатор периода (номер недели или номер месяца).
6. item_id — идентификатор категории товара.
7. new_customers_revenue — доход с новых клиентов.
8. returning_customers_revenue — доход с вернувшихся клиентов.
9. customers_refunded — количество возвратов клиентов. 
```
---

## Техническая информация по запуску
1. В папке `src` хранятся все необходимые исходники: 
    * Папка `dags` содержит DAG's Airflow.

Запустите локально команду:

```bash
docker run -d --rm -p 3000:3000 -p 15432:5432 --name=de-project-sprint-3-server cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-3:latest
```

После того как запустится контейнер, у вас будут доступны:
1. Visual Studio Code
2. Airflow
3. Database

Выполнить:
```f_customer_retention_DDL.sql```

---