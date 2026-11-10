"""Q7 - 백필(backfill) 데모.

- 스케줄 매일 1회(@daily). start_date 는 작업일(2026-11-11) 기준 7일 전인 2026-11-04 로 고정.
- catchup=True: DAG 를 켜면 start_date 부터 오늘까지 지나간 날짜 구간의 scheduled 실행을 자동으로 채운다.
  (Q3 의 sample_dag 는 catchup=False 라 과거 구간을 건너뛰었다.)
- 각 실행은 자신의 logical date(ds) 이름의 파일에 결과를 쓴다: /opt/airflow/output/backfill_demo/<ds>.txt
  (컨테이너 안 경로. 날짜별로 파일이 분리된다.)
"""
import os
from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

OUT_DIR = "/opt/airflow/output/backfill_demo"


def write_daily_file(**context):
    ds = context["ds"]                       # logical date, YYYY-MM-DD
    run_id = context["run_id"]
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, "%s.txt" % ds)
    with open(path, "w", encoding="utf-8") as f:
        f.write("logical_date=%s\n" % ds)
        f.write("run_id=%s\n" % run_id)
        f.write("data_interval=%s ~ %s\n" % (data_interval_start, data_interval_end))
        f.write("written_at=%s\n" % datetime.now(timezone.utc).isoformat())

    print("wrote", path)
    print("현재 %s 안의 파일:" % OUT_DIR, sorted(os.listdir(OUT_DIR)))
    return path


with DAG(
    dag_id="backfill_demo_yunakwon",
    description="Q7: catchup=True backfill, one file per logical date",
    start_date=datetime(2026, 11, 4),        # 작업일 2026-11-11 의 7일 전, 고정 리터럴
    schedule="@daily",
    catchup=True,
    max_active_runs=3,
    tags=["week7", "Q7"],
) as dag:
    PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )
