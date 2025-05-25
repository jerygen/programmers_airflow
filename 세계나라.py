from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')  # 연결 ID
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def fetch_country_data():
    url = Variable.get("country_url")
    response = requests.get(url)
    response.raise_for_status()
    countries = response.json()

    records = []
    for country in countries:
        try:
            name = country["name"]["official"]
            population = country["population"]
            area = country["area"]

            # 필수 데이터가 모두 있는 경우에만 추가
            if name and population is not None and area is not None:
                # SQL 삽입 에러 방지를 위한 작은 따옴표 이스케이프
                safe_name = name.replace("'", "''")
                records.append([safe_name, population, area])
        except KeyError as e:
            # 데이터 누락이 있을 경우 무시하고 로그 출력
            logging.warning(f"Skipping country due to missing field: {e}")

    return records


@task
def load_to_redshift(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country VARCHAR(256),
    population BIGINT,
    area FLOAT
);""")
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.error(e)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id='CountryInfoToRedshift',
    start_date=datetime(2024, 1, 1),
    schedule='30 6 * * 6',  # 매주 토요일 06:30 (UTC)
    catchup=False,
    tags=['API', 'Redshift', 'Country'],
) as dag:

    result = fetch_country_data()
    load_to_redshift("myksphone2001", "country_info", result)

