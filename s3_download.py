"""Q8 - boto3 로 S3 에서 읽어오기.

한 스크립트에서 순서대로:
  1. s3://{버킷}/bronze/ 아래 객체 목록과 각 객체의 크기 출력
  2. bronze/netflix_titles.csv 를 project/data/ 아래로 다운로드
  3. 내려받은 파일의 CSV 레코드 수 출력 (헤더 제외, 따옴표 안 줄바꿈은 한 행으로 셈)

버킷 이름은 코드에 적지 않고 환경변수 WEEK7_BUCKET 또는 첫 번째 실행 인자로 받는다.
자격증명은 AWS CLI 프로필(~/.aws)을 boto3 기본 체인이 읽는다. 코드에 키 없음.

실행:  WEEK7_BUCKET=de-8-yunakwon python s3_download.py
  또는  python s3_download.py de-8-yunakwon
"""
import csv
import os
import sys

import boto3

PREFIX = "bronze/"
FILE_NAME = "netflix_titles.csv"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def get_bucket():
    if len(sys.argv) > 1:
        return sys.argv[1]
    bucket = os.environ.get("WEEK7_BUCKET")
    if not bucket:
        sys.exit("버킷 이름이 없습니다. 환경변수 WEEK7_BUCKET 또는 인자로 주세요.")
    return bucket


def list_bronze(s3, bucket):
    print("[1] s3://%s/%s 객체 목록" % (bucket, PREFIX))
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            total += 1
            print("    %-40s %12s bytes  %s" % (obj["Key"], format(obj["Size"], ","), obj["LastModified"]))
    print("    객체 %d개" % total)


def download(s3, bucket):
    os.makedirs(DATA_DIR, exist_ok=True)
    key = PREFIX + FILE_NAME
    dest = os.path.join(DATA_DIR, FILE_NAME)
    print("[2] 다운로드 s3://%s/%s -> %s" % (bucket, key, dest))
    s3.download_file(bucket, key, dest)
    print("    완료: %s bytes" % format(os.path.getsize(dest), ","))
    return dest


def count_records(path):
    print("[3] CSV 레코드 수 세기 (csv 모듈: 헤더 제외, 따옴표 안 줄바꿈은 한 행)")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        records = sum(1 for _ in reader)
    with open(path, "rb") as f:
        raw_lines = sum(1 for _ in f)
    print("    열 %d개: %s" % (len(header), ", ".join(header)))
    print("    파일의 물리적 줄 수 (참고): %s" % format(raw_lines, ","))
    print("    CSV 레코드 수 (정답으로 쓰는 값): %s" % format(records, ","))
    return records


def main():
    bucket = get_bucket()
    s3 = boto3.client("s3")
    list_bronze(s3, bucket)
    dest = download(s3, bucket)
    count_records(dest)


if __name__ == "__main__":
    main()
