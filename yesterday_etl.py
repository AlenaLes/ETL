from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'password': 'dpo_python_2020',
              'user': 'student',
              'database': 'simulator_20220920'
}

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c' 
                     }

# Зададим параметры
default_args = {
    'owner': 'a-lesihina', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=1), # Промежуток между перезапусками
    'start_date': datetime(2022, 11, 13), # Дата начала выполнения DAG
}

schedule_interval = '0 10 * * *' # cron-выражение

@dag (default_args = default_args, schedule_interval=schedule_interval, catchup=False)
def yesterday_lesihina():
    
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
    
    @task()
    #объединим таблицы
    def merged_df(f_actions, m_actions):
        df_merged = pd.merge(f_actions, m_actions)
        df_merged.rename(columns = {'toDate(time)' : 'event_date'}, inplace = True)
        return df_merged
    
    
    # срез по полу
    @task()
    def df_gender(df_merged):
        df_gender = df_merged.groupby(['gender', 'event_date'])['like', 'view', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_gender.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        df_gender.insert(0, 'dimension', 'gender')
        return df_gender
    
    # срез по операционной системе
    @task()
    def df_os(df_merged):
        df_os = df_merged.groupby(['os', 'event_date'])['like', 'view', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        df_os.insert(0, 'dimension', 'os')
        return df_os
    
    # срез по возрасту пользователей
    @task()
    def df_age(df_merged):
        df_modify_age= df_merged 
        df_modify_age['Age_modify'] = pd.cut(df_merged['age'], bins = [0 ,18 , 25, 30, 45, 100], labels =['До 18', '18-25', '25-30', '30-45', '45+'])
        df_age = df_modify_age.groupby(['Age_modify', 'event_date'])['like', 'view', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index() 
        df_age.rename(columns = {'Age_modify' : 'dimension_value'}, inplace = True)
        df_age.insert(0, 'dimension', 'Age')
        return df_age
    
    #объединяем срезы 
    @task()
    def df_contact(df_gender,df_os,df_age):
        df_load = pd.concat([df_gender, df_os, df_age]).reset_index()
        df_load = df_load.drop(['index'], axis=1)
        return df_load
    
    #выгружаем результаты работы в новую таблицу 
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