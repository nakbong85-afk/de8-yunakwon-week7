"""Q6 - XCom 전달 + 재시도.

- compute_value: /opt/airflow/dags 폴더의 .py 파일 개수를 세어 return 한다 (XCom return_value 로 저장).
  retries=2, retry_delay=15초. 첫 번째 시도(try_number == 1)에서만 일부러 실패해 재시도로 성공한다.
- use_value: xcom_pull 로 앞 작업의 반환값을 받아 로그에 출력한다.
"""
import os
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAGS_DIR = "/opt/airflow/dags"


def compute_value(**context):
    ti = context["ti"]
    print("try_number =", ti.try_number, "(max_tries 까지 재시도)")
    if ti.try_number == 1:
        raise RuntimeError("첫 번째 시도는 일부러 실패시킵니다. 재시도에서 성공합니다.")

    py_files = sorted(f for f in os.listdir(DAGS_DIR) if f.endswith(".py"))
    print("dags 폴더의 .py 파일:", py_files)
    result = len(py_files)
    print("반환값(XCom 으로 저장될 값) =", result)
    return result


def use_value(**context):
    ti = context["ti"]
    value = ti.xcom_pull(task_ids="compute_value")
    print("xcom_pull(task_ids='compute_value') 로 받은 값 =", value, "(type: %s)" % type(value).__name__)
    print("dags 폴더에는 파이썬 파일이 %s 개 있습니다." % value)


with DAG(
    dag_id="xcom_demo_yunakwon",
    description="Q6: XCom return/pull + retry",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["week7", "Q6"],
) as dag:
    compute = PythonOperator(
        task_id="compute_value",
        python_callable=compute_value,
        retries=2,
        retry_delay=timedelta(seconds=15),
    )
    use = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    compute >> use
