"""Q4 - wordcount: 텍스트 파일 읽기 -> 공백 기준 단어 분리 -> 단어별 개수 집계 -> 상위 20개 show()

규칙: 분리 기준은 공백(whitespace)뿐. 소문자 변환, 구두점 제거를 하지 않는다.
      (Lorem 과 lorem, amet 과 amet, 는 서로 다른 단어)
입력: 기본 /opt/spark/data/wordcount.txt (컨테이너에 마운트된 data/wordcount.txt). 인자로 바꿀 수 있다.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/wordcount.txt"

spark = SparkSession.builder.appName("Q4_wordcount_yunakwon").getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # INFO 로그를 줄여 결과 표가 잘 보이게

# 1) 텍스트 파일 읽기 (한 줄 = 한 행, 열 이름 value)
lines = spark.read.text(input_path)

# 2) 공백 기준으로 단어 분리 (연속 공백으로 생기는 빈 문자열은 단어가 아니므로 제외)
words = (
    lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
    .filter(F.col("word") != "")
)

# 3) 단어별 개수 집계
counts = words.groupBy("word").count()

total_words = words.count()
unique_words = counts.count()
print("입력 파일        : %s" % input_path)
print("줄 수            : %d" % lines.count())
print("총 단어 수       : %d" % total_words)
print("고유 단어 수     : %d" % unique_words)
print("단어 빈도 상위 20개 (빈도 내림차순, 같은 빈도는 단어 오름차순):")

# 4) 빈도 내림차순 상위 20개
counts.orderBy(F.col("count").desc(), F.col("word").asc()).show(20, truncate=False)

spark.stop()
