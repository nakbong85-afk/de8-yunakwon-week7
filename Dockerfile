# Q5 - 공식 Airflow 이미지를 베이스로 JDK + AWS/Spark provider + pyspark 를 넣은 커스텀 이미지
# 빌드:  docker compose build        (docker-compose.yaml 의 x-airflow-common 에서 image -> build 로 전환)
# 태그:  week7-airflow:yunakwon
ARG AIRFLOW_VERSION=3.4.0
FROM apache/airflow:${AIRFLOW_VERSION}

# 1) JDK 설치 — apt 는 root 로 실행해야 한다
#    베이스(Debian 13 trixie) 저장소에는 OpenJDK 21 만 있다 (17 없음). pyspark 4.x 는 Java 17/21 을 지원한다.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 2) 파이썬 패키지 설치 — pip 은 airflow 사용자로 돌린다 (이미지 규칙)
USER airflow
ARG AIRFLOW_VERSION
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
