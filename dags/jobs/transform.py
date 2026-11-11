"""Q9 - Spark 집계 잡: netflix_titles.csv -> type x 장르별 작품 수 (parquet, snappy).

처리:
  1. release_year 가 기준 연도 이상인 행만 남긴다. 기준 연도는 --year 인자, 기본 2015.
  2. listed_in (쉼표 구분 장르 목록) 을 장르 하나당 한 행으로 펼치고 앞뒤 공백을 제거한다.
  3. type x genre 별 작품 수를 집계한다.
  4. parquet(snappy) 로 저장하고 집계 행 수를 출력한다.

실행 예:
  spark-submit transform.py --input /opt/airflow/data/netflix_titles.csv \
                            --output /opt/airflow/data/silver/2026-11-10 --year 2015
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="netflix_titles.csv 경로")
    p.add_argument("--output", required=True, help="parquet 출력 폴더 (컨테이너 안 로컬 경로)")
    p.add_argument("--year", type=int, default=2015, help="기준 연도 (이상만 남김). 기본 2015")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("Q9_transform_yunakwon").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("header", True)
        .option("multiLine", True)      # 따옴표 안 줄바꿈이 있는 필드 처리
        .option("escape", '"')
        .csv(args.input)
    )
    total = df.count()
    print("입력 행 수: %d (%s)" % (total, args.input))

    # 1) 기준 연도 필터
    filtered = df.withColumn("release_year", F.col("release_year").cast("int")).filter(
        F.col("release_year") >= args.year
    )
    print("release_year >= %d 인 행 수: %d" % (args.year, filtered.count()))

    # 2) listed_in 을 장르 단위로 펼치고 공백 제거
    exploded = (
        filtered.filter(F.col("listed_in").isNotNull())
        .withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre") != "")
    )

    # 3) type x genre 집계
    result = exploded.groupBy("type", "genre").agg(F.count("*").alias("title_count")).orderBy(
        F.col("type"), F.col("title_count").desc()
    )

    # 4) parquet(snappy) 저장 + 집계 행 수 출력
    result.write.mode("overwrite").option("compression", "snappy").parquet(args.output)
    n = result.count()
    print("집계 결과 상위 10행:")
    result.show(10, truncate=False)
    print("저장 위치: %s (parquet, snappy)" % args.output)
    print("집계 행 수 = %d" % n)

    spark.stop()


if __name__ == "__main__":
    main()
