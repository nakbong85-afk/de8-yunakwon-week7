"""Q3 - 시작 -> 파이썬 작업 2개 -> 종료 (4 task 순차).

- 시작/종료: EmptyOperator (아무 동작 없음)
- 파이썬 작업: 문자열을 return (XCom return_value 로 저장, Task Log 에 'Returned value was: ...' 로 찍힘)
- 스케줄: 매일 1회(@daily), catchup=False 로 과거 구간은 자동 실행하지 않음
"""
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def python_task_1():
    msg = "python_task_1 의 반환값: 첫 번째 파이썬 작업 완료"
    return msg


def python_task_2():
    msg = "python_task_2 의 반환값: 두 번째 파이썬 작업 완료"
    return msg


with DAG(
    dag_id="sample_dag",
    description="Q3: start -> python x2 -> end",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["week7", "Q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    t1 = PythonOperator(task_id="python_task_1", python_callable=python_task_1)
    t2 = PythonOperator(task_id="python_task_2", python_callable=python_task_2)
    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> end
