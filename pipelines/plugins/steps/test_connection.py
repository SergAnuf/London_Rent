from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import random
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime,Float,Boolean,UniqueConstraint,create_engine


def create_table():
        # ваш код здесь тоже #
        metadata = MetaData()
        salaries_table = Table(
         'raw_data2',
          metadata,
          Column('id', Integer, primary_key=True, autoincrement=True),
          Column('price', Float),
          UniqueConstraint('id', name='hi_flat_id')
        ) 
        hook = PostgresHook('ai_agency')
        metadata.create_all(hook.get_sqlalchemy_engine())
        pass 


def load(**kwargs):
    # Connect to Postgres
    hook = PostgresHook('ai_agency')

    # Generate random data to populate the "raw_data" table
    data = pd.DataFrame({
        "id": range(1, 11),  # IDs 1 to 10
        "price": [round(random.uniform(1000, 5000), 2) for _ in range(10)],  # Random prices
    })

    rows = data.values.tolist()  # Get the row values

    # Insert rows into the table with UPSERT logic
    insert_query = """
    INSERT INTO raw_data2 (id, price)
    VALUES (%s, %s)
    ON CONFLICT (id) DO UPDATE
    SET price = EXCLUDED.price;
    """

    conn = hook.get_conn()
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, rows)  # Efficient batch insert
        conn.commit()

