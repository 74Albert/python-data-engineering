from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(__file__))

import requests
from sqlalchemy import create_engine, text
from config import settings

url ='https://allratestoday.com/api/v1/central-bank/cbr/latest'
headers = {
    "Authorization": f"Bearer {settings.ALLRATES_API_KEY}"
}

def get_currency_rates(**kwargs):
    print('--- Запуск get ---')
    try:
        response = requests.get(url, headers=headers, timeout=10)
        match response.status_code:
            case 200:
                data = response.json()
                data_list = []
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for rate_info in data['rates']:
                    if rate_info["base"] in ["USD", "EUR", "CNY"]:
                        print(f"[INFO] 1 {rate_info['base']} = {rate_info['value']} RUB")
                        data_list.append({"date": current_time, "currency": rate_info['base'], "rate": rate_info['value'], "target_currency": "RUB"})
                return data_list
            case 401:
                print('[INFO] Неверный ключ')
                raise 
            case 429: 
                print('[INFO] Превышен лимит запросов')
                raise
            case _:
                print(f"[INFO] Ошибка API: статус {response.status_code}")
                raise
    except requests.exceptions.RequestException as e:
        print(f"[INFO] Ошибка сети: {e}")
        raise e
    except KeyError as e_key:
        print("[INFO] В ответе API изменилась структура, нет нужного поля")
        raise e_key
    except Exception as e:
        print(f"[INFO] Неизвестная ошибка: {e}")
        raise e

def load_currency_rates(**kwargs):
    print('--- Запуск get ---')
    try:
        ti = kwargs["ti"]
        data_list = ti.xcom_pull(task_ids='get_task')
        df = pd.DataFrame(data_list)
        engine = create_engine(url=settings.DATABASE_URL_psycopg, echo=False)
        df.to_sql('currency_rates', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print(f"Ошибка: {ex}")
        raise ex 
    finally:
        if 'engine' in locals():
            engine.dispose()


with DAG(
    dag_id='currency_etl_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['api', 'etl', 'currency'],
) as dag:

    task_get = PythonOperator(
        task_id='get_task',
        python_callable=get_currency_rates,
    )

    task_load = PythonOperator(
        task_id='load_task',
        python_callable=load_currency_rates,
    )

    task_get >> task_load