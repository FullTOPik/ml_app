import pendulum
from airflow.decorators import dag, task
import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, String, inspect, UniqueConstraint

@dag(
    schedule='@once',
    dag_id='bank_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    catchup=False,
    tags=["ETL"],
)
def prepare_bank_dataset() -> None:
    @task()
    def create_table():
        metadata = MetaData()
        hook = PostgresHook('data')
        db_conn = hook.get_sqlalchemy_engine()

        users_bank_table = Table(
            'users_bank',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('age', Integer),
            Column('job', String),
            Column('marital', String),
            Column('education', String),
            Column('default', String),
            Column('balance', Integer),
            Column('housing', String),
            Column('loan', String),
            Column('contact', String),
            Column('day', Integer),
            Column('month', String),
            Column('duration', Integer),
            Column('campaign', Integer),
            Column('pdays', Integer),
            Column('previous', Integer),
            Column('poutcome', String),
            Column('y', Integer),

            UniqueConstraint('id', name='unique_id'),
        )

        if not inspect(db_conn).has_table(users_bank_table.name):
            db_conn.execute("DROP TABLE IF EXISTS users_bank")
            metadata.create_all(db_conn)

    @task()
    def extract() -> pd.DataFrame:
        data = pd.read_csv('/opt/airflow/data/train.csv')

        return data
    
    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        data = data.drop_duplicates(subset=['id'], keep='first')

        return data

    @task()
    def load(data: pd.DataFrame) -> None:
        hook = PostgresHook('data')
        target_fields = [col if col != 'default' else '"default"' for col in data.columns]

        hook.insert_rows(
            table="users_bank",
            replace=False,
            target_fields=target_fields,
            rows=data.values.tolist()
        )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

prepare_bank_dataset()
