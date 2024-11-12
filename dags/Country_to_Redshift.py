from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json

def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

@task
def etl(schema, table):
    url = "https://restcountries.com/v3/all"
    response = requests.get(url)
    data = response.json()
    
    country_data = []
    for d in data:
        country_name = d['name']['official'].replace("'","")
        population = d['population']
        area = d['area']
        country_data.append("('{}',{},'{}')".format(country_name, population, area))
        
    cur = get_Redshift_connection()
    drop_recreate_sql = f"""
    DROP TABLE IF EXISTS {schema}.{table};
    CREATE TABLE {schema}.{table} (
        name VARCHAR(256),
        population INTEGER,
        area VARCHAR(256)
    );
    """
    
    insert_sql = f"""
    INSERT INTO {schema}.{table} (name, population, area)
    VALUES """ + ",".join(country_data)
    logging.info(drop_recreate_sql)
    logging.info(insert_sql)
    try:
        cur.execute(drop_recreate_sql)
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise
    
with DAG(
    dag_id = 'Country_to_Redshift',
    description='Practice aiflow weekly country data',
    start_date = datetime(2024,11,12),
    schedule = '30 6 * * 6',
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
) as dag:
    
    etl("sko99","country_data")