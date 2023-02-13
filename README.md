# ETL задача

## Краткое описание
Создание DAG в Airflow, который наполняет таблицу данными за вчерашний день. 
Данные выгружаются из Clickhouse по двум таблицам: данные по ленте новостей и данные по сообщениям. В таблице feed_actions для каждого ползователя посчитаем число просмотров и лайков контента. В таблице message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. В результате должны получить новую таблицу со всем подсчитанными значениями и загрузить ее обратно в Clickhouse. 

Структра итоговой таблицы:

- Дата - event_date
- Название среза - dimension
- Значение среза - dimension_value
- Число просмотров - views
- Числой лайков - likes
- Число полученных сообщений - messages_received
- Число отправленных сообщений - messages_sent
- От скольких пользователей получили сообщения - users_received
- Скольким пользователям отправили сообщение - users_sent
- Срез - это os, gender и age

---
## Стэк: 
- JupiterHub
- Clickhouse
- Python
- SQL
- Airflow

---
# Основной код
## Настройка связи

Настроили подключение к Clickhouse и задали параметры для выполнения дага. После чего с помощью SQL-запроса получим нужные данные, которые будут ежедневно выгружаться в таблицу.

```
default_args = {
    'owner': 'a-lesihina', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=1), # Промежуток между перезапусками
    'start_date': datetime(2022, 11, 13), # Дата начала выполнения DAG
}

schedule_interval = '0 10 * * *' # cron-выражение
```

## Создание тасков

Всего создали 8 задач. Двое из них выгружают данные по действиям новостной ленты и по данным с сообщениямию.

```
    @task()
    def extract_feed():

        # вытащим данные
        p1 = """
        SELECT 
            user_id, 
            sum(action= 'like') as like, 
            countIf(action='view') as view,
            age, 
            city, 
            toDate(time), 
            gender, 
            os, 
            source 
        FROM  simulator_20220920.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP by user_id, age, city, toDate(time), gender, os, source
        """
        f_actions = ph.read_clickhouse(p1, connection=connection)
        return f_actions 
    
    @task()
    def extract_message():
    
        p2 = """
        SELECT user_id, 
        messages_received, messages_sent, users_received, users_sent
        from 
        (SELECT user_id, 
        count(reciever_id) as messages_sent, 
        count(distinct reciever_id) as users_sent
        FROM simulator_20220920.message_actions
        WHERE toDate(time) = yesterday()
        Group by user_id) t1

        join 

        (SELECT reciever_id, 
        count(user_id) as messages_received, 
        count(distinct user_id) as users_received
        FROM simulator_20220920.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY reciever_id) t2

        on t1.user_id = t2.reciever_id
        """       
        m_actions = ph.read_clickhouse(p2, connection=connection)
        return m_actions
```

Следующий task после выгрузки данных объединяет таблицы. Еще три task-а отвечают за получение срезов по полу, возрасту и операционной системе, в последствии эти данные объединяются. И финальный task выгружает итоговый результат в одну таблицу.
```
@task()
    def load_bd(df_load):
        
        creat_q = """
        CREATE TABLE IF NOT EXISTS test.a_lesihina
        (
        dimension String,
        dimension_value String,
        event_date Date,
        like UInt64,
        view UInt64,
        messages_received UInt64,
        messages_sent UInt64,
        users_received UInt64,
        users_sent UInt64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        """
        ph.execute(creat_q, connection=connection_test)
        ph.to_clickhouse(df_load, 'a_lesihina', index=False, connection=connection_test)
    
    f_actions = extract_feed()
    m_actions = extract_message()
    
    df_merged = merged_df(f_actions, m_actions)
    df_gender = df_gender(df_merged)
    df_os = df_os(df_merged)
    df_age = df_age(df_merged)
    df_load = df_contact(df_gender, df_os, df_age)
    load_bd(df_load)
    
yesterday_lesihina = yesterday_lesihina()
```

---
## Ежедневная выгрузка

В airflow настроена ежедневная выгрузка. Граф выглядит следующим образом.

![image](https://user-images.githubusercontent.com/100629361/206929210-b2a83a59-060a-4869-b99a-d0d3ca67b556.png)

![image](https://user-images.githubusercontent.com/100629361/206929266-40053df6-c5ce-4145-8436-669047590402.png)
---
## Итоговая таблица, ежедневно обновляющаяся в Clickhouse:

![image](https://user-images.githubusercontent.com/100629361/206929634-4ff2ce08-d4b1-4f0a-975d-782bfc99bcb2.png)

