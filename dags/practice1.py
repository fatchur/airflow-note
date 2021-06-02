from airflow.models import DAG 
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator


import json
from pandas import json_normalize
from datetime import datetime 


def _processing_user(ti): 
    data = ti.xcom_pull(task_ids=['extracting_user'])
    if len(data)==0 or 'results' not in data[0]:
        raise ValueError('user empty')

    data = data[0]['results'][0]
    processed_user = json_normalize({
        'firstname': data['name']['first'],
        'lastname': data['name']['last'],
        'country': data['location']['country'],
        'username': data['login']['username'],
        'password': data['login']['password'],
        'email': data['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)



default_args = { "start_date": datetime(2021, 1, 1)}
with DAG("user-processing", 
            schedule_interval="@daily",
            default_args=default_args,
            catchup=False) as dag: 
    
    creating_table = SqliteOperator(task_id="create_table",
                                    sqlite_conn_id="db_sqlite",
                                    sql='''CREATE TABLE IF NOT EXISTS users (
                                        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        firstname TEXT NOT NULL,
                                        lastname TEXT NOT NULL,
                                        country TEXT NOT NULL,
                                        username TEXT NOT NULL,
                                        password TEXT NOT NULL
                                        );
                                        ''')

    is_api_available = HttpSensor(task_id='is_api_available',
                                    http_conn_id='user_api',
                                    endpoint='api/')
    
    extracting_user = SimpleHttpOperator(task_id='extracting_user',
                                            http_conn_id='user_api',
                                            endpoint='api/',
                                            method='GET',
                                            response_filter=lambda response: json.loads(response.text),
                                            log_response=True)
    
    processing_user = PythonOperator(task_id='processing_user',
                                        python_callable=_processing_user)


    creating_table >> is_api_available >> extracting_user >> processing_user


    