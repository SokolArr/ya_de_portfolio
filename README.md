# В текущем репозитории преставлено портфолио проектов курса Яндекс Практикум - `Инженер Данных`.
## Проекты
### [1. RFM витрина](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1)
#### Здание
1. Выяснить требования к целевой витрине.
2. Изучить структуру исходных данных.
3. Подготовить витрину.
4. Доработать представления.

#### Технологии
- `SQL`
- `PostgreSQL`
  
| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_1](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_1) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_1/README.md) |
---

### [2. Оптимизация модели данных интернет-магазина](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_2)
#### Здание
1. Разложить данные из одной таблицы по нескольким логическим таблицам.
2. Создать удобное представление данных на основе новых логических таблиц.

#### Технологии
- `Python`
- `PostgreSQL`
- `SQL`
  
| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_2](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_2) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_2/README.md) |
---

### [3. Обновление пайплайна обработки данных](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_3)
#### Здание
1. Адаптировать пайплайн для текущей задачи: учесть в витрине нужные статусы и обновить пайплайн с учётом этих статусов. 
2. На основе пайплайна наполнить витрину данными по «возвращаемости клиентов» в разрезе недель. 
3. Перезапустить пайплайн и убедиться, что после перезапуска не появилось дубликатов в витринах (опционально).

#### Технологии
- Python
- Airflow
- PostgreSQL
- S3
- REST-API

| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_3](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_3) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_3/README.md) |
---

### [4. Реализация витрины для расчётов выплат курьерам](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_4)
#### Здание
1. Изучить API системы доставки заказов.
2. Спроектировать структуру таблиц для слоёв в хранилище.
3. Реализовать DAG.

#### Технологии
- Python
- Airflow
- PostgreSQL
- MongoDB
- REST-API

| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_4](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_4) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_4/README.md) |
---

### [5. Поиск сообществ с высокой конверсией в первое сообщение](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_5)
#### Здание
1. Перенести из S3 в staging-слой новые данные о входе и выходе пользователей из групп.
2. Создать в слое постоянного хранения таблицы для новых данных.
3. Перенести новые данные из staging-области в слой DDS.
4. Рассчитать конверсионные показатели для десяти самых старых сообществ.

#### Технологии
- Airflow
- PostgreSQL
- S3
- Vertica
- REST-API

| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_5](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_5) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_5/README.md) |
---

### [6. Обновление хранилища данных для соцсети](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_6)
#### Здание
1. Обновить структуру Data Lake.
2. Создать витрину в разрезе пользователей.
3. Создать витрину в разрезе зон.
4. Построить витрину для рекомендации друзей.
5. Автоматизировать обновление витрин.

#### Технологии
- Hadoop
- MapReduce
- HDFS
- Apache Spark

| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_6](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_6) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_6/README.md) |
---

### [7. Настройка потоковой обработки данных для агрегатора доставки еды](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_7)
#### Здание
Написать сервис, который будет:
1. Читать данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени.
2. Получать список подписчиков из базы данных Postgres. 
3. Джойнить данные из Kafka с данными из БД.
4. Сохранять в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka.
5. Отправлять выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане.
6. Вставлять записи в Postgres, чтобы получить фидбэк от пользователя. 

#### Технологии
- Kafka
- Spark Streaming
- PostgreSQL
- Python
- pySpark

| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_7](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_7) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_7/README.md) |
---

### [8. Создание DWH с использованием облачных технологий для агрегатора доставки еды](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_8)
#### Здание
1. С помощью Kubernetes написать сервис для наполнения слоя с сырыми данными.
2. С помощью Kubernetes написать сервис для наполнения слоя DDS.
3. С помощью Kubernetes написать сервис для наполнения слоя с витринами.
4. Сделать визуализацию для аналитиков по популярности блюд (опционально). 

#### Технологии
- Yandex Cloud 
- Kubernetes 
- Redis
- PostgreSQL 
- Docker 
- Kafka 
- Python 
- SQL 
- DataLens

| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_8](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_8) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_8/README.md) |
---

### [9. Итоговый проект](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_9)
#### Здание
   - 

#### Технологии
 - 

| Репозиторий                                                                  | Описание                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [project_9](https://github.com/SokolArr/ya_de_portfolio/tree/main/project_9) | [README](https://github.com/SokolArr/ya_de_portfolio/blob/main/project_9/README.md) |
---





    