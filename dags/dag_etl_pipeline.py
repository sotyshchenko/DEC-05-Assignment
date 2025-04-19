from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd
import io
from pathlib import Path


default_args = {
    'owner': 'sofiia',
    'depends_on_past': False,
    'email_on_failure': False,
}


DAG_DIR = Path(__file__).parent
PROJECT_DIR = DAG_DIR.parent


def extract_sales(**kwargs):
    sales_path = (DAG_DIR / '..' / 'include' / 'sales.csv').resolve()
    df = pd.read_csv(sales_path)
    ti = kwargs['ti']
    ti.xcom_push(key='sales_json', value=df.to_json(orient='records'))
    print(f"[extract_sales] pushed sales_json ({len(df)} rows)")


def extract_mysql(**kwargs):
    ti = kwargs['ti']
    hook = MySqlHook(mysql_conn_id='localhost')
    emp_df = hook.get_pandas_df('SELECT * FROM employees;')
    prod_df = hook.get_pandas_df('SELECT * FROM products;')
    ti.xcom_push(key='employees_data', value=emp_df.to_json(orient='records'))
    ti.xcom_push(key='products_data',  value=prod_df.to_json(orient='records'))
    print(f"[extract_mysql] pushed employees_data ({len(emp_df)} rows) and products_data ({len(prod_df)} rows)")


def transform(**kwargs):
    ti = kwargs['ti']
    sales_json = ti.xcom_pull(task_ids='extract_sales', key='sales_json')
    emp_json = ti.xcom_pull(task_ids='extract_mysql', key='employees_data')
    prod_json = ti.xcom_pull(task_ids='extract_mysql', key='products_data')
    print(f"[transform] pulled: sales_json={sales_json is not None}, emp_json={emp_json is not None}, prod_json={prod_json is not None}")
    sales_df = pd.read_json(io.StringIO(sales_json), orient='records')
    emp_df = pd.read_json(io.StringIO(emp_json),   orient='records')
    prod_df = pd.read_json(io.StringIO(prod_json),  orient='records')
    df = (
        sales_df
        .merge(emp_df,  how='left', left_on='SalesPersonID', right_on='EmployeeID')
        .merge(prod_df, how='left', on='ProductID')
    )
    df['TotalPrice'] = df['Quantity'] * df['Price']

    df['SalesPersonName'] = df['FirstName'].str.strip() + ' ' + df['LastName'].str.strip()

    df = df[['SalesID','SalesDate','ProductName','Price','Quantity','TotalPrice','SalesPersonName']]

    out_dir = PROJECT_DIR / 'tmp'
    out_dir.mkdir(parents=True, exist_ok=True)
    result_path = out_dir / 'etl_result.csv'

    df.to_csv(result_path, index=False)
    print(f"[transform] wrote intermediate CSV to {result_path} ({len(df)} rows)")
    ti.xcom_push(key='result_path', value=str(result_path))


def load_to_db(**kwargs):
    ti = kwargs['ti']
    src_path = ti.xcom_pull(task_ids='transform', key='result_path')
    print(f"[load_to_db] pulled src_path: {src_path!r}")
    if not src_path:
        raise ValueError("No result_path found in XCom")
    df = pd.read_csv(src_path)
    print(f"[load_to_db] read {len(df)} rows from {src_path}")
    hook = MySqlHook(mysql_conn_id='localhost')
    try:
        engine = hook.get_sqlalchemy_engine()
    except AttributeError:
        from sqlalchemy import create_engine
        conn = hook.get_connection('localhost')
        engine = create_engine(
            f"mysql+pymysql://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
    df.to_sql('etl_sales', con=engine, if_exists='replace', index=False)
    print(f"[load_to_db] inserted {len(df)} rows into etl_sales")


with DAG(
    dag_id='etl_sales_employees_products',
    default_args=default_args,
    start_date=datetime(2025, 4, 18),
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
) as dag:
    t1 = PythonOperator(task_id='extract_sales', python_callable=extract_sales)
    t2 = PythonOperator(task_id='extract_mysql', python_callable=extract_mysql)
    t3 = PythonOperator(task_id='transform', python_callable=transform)
    t4 = PythonOperator(task_id='load_to_db', python_callable=load_to_db)
    [t1, t2] >> t3 >> t4
