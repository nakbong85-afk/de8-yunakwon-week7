# de8-yunakwon-week7

메타코드 데이터 엔지니어링 8기 · 유낙원 · 7주차 과제 (Docker · AWS · Airflow · Spark · Git)

## 구성

| 경로 | 문항 | 내용 |
|---|---|---|
| `docker-compose.yaml` | Q3 | 공식 Airflow compose |
| `docker-compose.spark.yaml` | Q4 | Spark Standalone 클러스터 |
| `Dockerfile`, `requirements.txt` | Q5 | 커스텀 Airflow 이미지 |
| `dags/` | Q3, Q6, Q7, Q9 | DAG 파일 |
| `jobs/wordcount.py` | Q4 | 단어 빈도 집계 잡 |
| `s3_download.py` | Q8 | boto3 S3 다운로드 |

## 실습 환경

Windows 11 + Docker Desktop(WSL2), 터미널은 VS Code 의 Git Bash. Airflow 3.4.0(CeleryExecutor, 커스텀 이미지 week7-airflow:yunakwon)과 Spark Standalone 3.5.3(master 1 + worker 2)을 docker compose 로 띄웠고, AWS 는 S3(ap-northeast-2)만 사용했다.

## 회고

catchup, XCom, 재시도처럼 Airflow 가 실행 이력을 관리하는 방식을 직접 돌려 보며 이해했다. S3 -> Spark -> S3 로 이어지는 파이프라인을 Airflow DAG 하나로 묶으면서, 자격증명을 코드 밖에 두는 방법도 익혔다.
