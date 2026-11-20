from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os


sys.path.append(os.path.dirname(__file__))

from config import settings
import pandas as pd
from sqlalchemy import create_engine, text

def load_titanic_to_postgres():
    print("Начинаем загрузку данных...")

    engine = create_engine(url=settings.DATABASE_URL_psycopg, echo=False)
    
    try:

        df = pd.read_csv('/opt/airflow/dags/train.csv')
        

        df = df.filter(items=["PassengerId", "Name", "Fare"])
        df = df.rename(columns={'PassengerId': 'emp_id', 'Name': 'emp_name', 'Fare': 'salary'})
        

        df.to_sql('employees', con=engine, if_exists='replace', index=False)
        print(f"Успешно загружено {len(df)} строк.")
        

        with engine.connect() as connect:
            res = connect.execute(text("SELECT COUNT(*) FROM employees;")).scalar()
            print(f"В базе данных теперь {res} строк.")
            
    except Exception as ex:
        print(f"Ошибка: {ex}")
        raise ex
    finally:
        engine.dispose()

# Создаем DAG
with DAG(
    dag_id='titanic_etl_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['titanic', 'etl'],
) as dag:

    # Определяем задачу
    load_data_task = PythonOperator(
        task_id='load_titanic_data',
        python_callable=load_titanic_to_postgres,
    )

    load_data_task