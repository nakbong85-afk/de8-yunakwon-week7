"""Q9 - Airflow 로 잇는 S3 · Spark 통합 파이프라인.

세 개의 별도 task 를 순서대로 연결한다.
  download_from_s3 : boto3 로 s3://{버킷}/bronze/netflix_titles.csv 를 컨테이너 안(/opt/airflow/data/)으로 내려받기
  transform_spark  : spark-submit 으로 dags/jobs/transform.py 실행 (기준 연도는 인자로 주입, 기본 2015)
  upload_to_s3     : boto3 로 결과 parquet 을 s3://{버킷}/silver/{오늘날짜}/ 에 올리고 목록·개수 출력

오늘날짜: 이 실행(DAG run)이 실제로 시작된 날의 날짜(한국 시각, YYYY-MM-DD). run_date() 가 계산하고
          transform 과 upload 두 task 가 같은 값을 쓴다 (한 실행 안에서는 값이 변하지 않는다).

자격증명: 코드 · yaml · .env 어디에도 키를 적지 않는다. worker 에 읽기 전용으로 마운트된 ~/.aws 를 boto3 가 읽는다.
버킷 이름: 환경변수 WEEK7_BUCKET (compose override 에서 주입), 없으면 Airflow Variable week7_bucket.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

FILE_NAME = "netflix_titles.csv"
DATA_DIR = "/opt/airflow/data"
LOCAL_INPUT = os.path.join(DATA_DIR, FILE_NAME)
LOCAL_OUTPUT = os.path.join(DATA_DIR, "silver", "{{ run_date(dag_run) }}")   # 실행 날짜별 로컬 출력 폴더
BASE_YEAR = int(os.environ.get("WEEK7_BASE_YEAR", "2015"))
KST = timezone(timedelta(hours=9))


def run_date(dag_run):
    """이 실행이 시작된 날의 날짜 (한국 시각, YYYY-MM-DD) = 오늘 날짜."""
    return dag_run.start_date.astimezone(KST).strftime("%Y-%m-%d")


def get_bucket():
    return os.environ.get("WEEK7_BUCKET") or Variable.get("week7_bucket")


def download_from_s3():
    import boto3

    bucket = get_bucket()
    key = "bronze/" + FILE_NAME
    os.makedirs(DATA_DIR, exist_ok=True)
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, LOCAL_INPUT)
    size = os.path.getsize(LOCAL_INPUT)
    print("downloaded s3://%s/%s -> %s (%s bytes)" % (bucket, key, LOCAL_INPUT, format(size, ",")))
    return LOCAL_INPUT


def upload_to_s3(**context):
    import boto3

    bucket = get_bucket()
    ds = run_date(context["dag_run"])
    local_dir = os.path.join(DATA_DIR, "silver", ds)
    prefix = "silver/%s/" % ds
    s3 = boto3.client("s3")

    # 같은 날 다시 실행해도 결과가 겹쳐 쌓이지 않게, 이 날짜 폴더에 이미 있던 객체를 먼저 지운다.
    # (Spark 는 실행할 때마다 part 파일 이름을 새로 만든다. 지우는 범위는 silver/{날짜}/ 아래뿐이다.)
    old = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    for obj in old:
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
    if old:
        print("이전 실행의 객체 %d개를 지움: s3://%s/%s" % (len(old), bucket, prefix))

    uploaded = 0
    for name in sorted(os.listdir(local_dir)):
        path = os.path.join(local_dir, name)
        if os.path.isfile(path) and not name.startswith("."):   # Spark 가 만드는 숨김 체크섬(.crc)은 제외
            s3.upload_file(path, bucket, prefix + name)
            uploaded += 1
            print("uploaded %s -> s3://%s/%s%s" % (name, bucket, prefix, name))

    print("--- s3://%s/%s 객체 목록 ---" % (bucket, prefix))
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objs = resp.get("Contents", [])
    for obj in objs:
        print("  %-60s %10s bytes" % (obj["Key"], format(obj["Size"], ",")))
    print("silver/%s 객체 개수 = %d (이번에 올린 파일 %d개)" % (ds, len(objs), uploaded))
    return len(objs)


with DAG(
    dag_id="weekly_pipeline_yunakwon",
    description="Q9: S3 bronze -> Spark transform -> S3 silver/{ds}/",
    start_date=datetime(2026, 11, 1),
    schedule="@weekly",
    catchup=False,
    user_defined_macros={"run_date": run_date},
    tags=["week7", "Q9"],
) as dag:
    download = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
    )

    transform = SparkSubmitOperator(
        task_id="transform_spark",
        conn_id="spark_local",                       # host=local[*] : worker 컨테이너 안에서 실행
        application="/opt/airflow/dags/jobs/transform.py",
        application_args=[
            "--input", LOCAL_INPUT,
            "--output", LOCAL_OUTPUT,
            "--year", str(BASE_YEAR),                # 기준 연도 인자 주입 (하드코딩 아님)
        ],
        name="Q9_transform_yunakwon",
        verbose=False,
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    download >> transform >> upload
