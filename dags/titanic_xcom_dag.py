from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from config import settings

def extract_data(**kwargs):
    print("--- Запуск Extract ---")
    try:
        df_csv = pd.read_csv('/opt/airflow/dags/train.csv')
        row_count = len(df_csv)
        print(f"Найдено строк в файле: {row_count}")
        
        return {"status": "success", "rows": row_count}
    except Exception as e: 
        print(f"Ошибка при извлечении данных: {e}")
        raise 

def load_data(**kwargs):
    print("--- Запуск Load ---")
    ti = kwargs['ti']

    extract_result = ti.xcom_pull(task_ids='extract_task') 
    

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        engine = create_engine(url=settings.DATABASE_URL_psycopg, echo=False)
        if extract_result.get("status") == "error":
            df_log = pd.DataFrame([{
                "info": f"[{current_time}] Extract failed: {extract_result.get('error_msg')}",
                "error": "Y"
            }])
            df_log.to_sql('pipeline_logs', con=engine, if_exists='append', index=False)
            
            raise Exception(extract_result.get('error_msg'))

        row_count = extract_result.get("rows")
        df_log = pd.DataFrame([{
            "info": f"[{current_time}] Load success. Processed {row_count} rows.",
            "error": "N"
        }])
        

        df_log.to_sql('pipeline_logs', con=engine, if_exists='append', index=False)
        print(f"Успешно загружено {len(df_log)} строк лога.")

    except Exception as ex:
        # Если упало во время Load, пытаемся записать это в БД
        print(f"Ошибка: {ex}")
        try:
            df_err = pd.DataFrame([{
                "info": f"[{current_time}] Load error: {str(ex)}",
                "error": "Y"
            }])
            df_err.to_sql('pipeline_logs', con=engine, if_exists='append', index=False)
        except Exception as db_ex:
            print(f"Не удалось даже записать лог в БД: {db_ex}")
        raise ex 
    finally:
        if 'engine' in locals():
            engine.dispose()

with DAG(
    dag_id='titanic_xcom_demo',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['titanic', 'xcom'],
) as dag:


    task_extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
    )

    task_load = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
    )

    task_extract >> task_load