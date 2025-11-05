# # Spark 3.5.0 + Scala 2.12
# FROM apache/spark:3.5.0

# USER root
# RUN apt-get update && apt-get install -y curl wget && rm -rf /var/lib/apt/lists/*

# ENV SPARK_HOME=/opt/spark
# ENV SPARK_JARS_DIR=$SPARK_HOME/jars
# ENV PATH=$PATH:$SPARK_HOME/bin

# # --- S3A (MinIO) ---
# # Giữ nguyên Hadoop core từ image; CHỈ bổ sung hadoop-aws + aws sdk BUNDLE KHỚP 3.3.4
# RUN curl -L -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
#       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
#     curl -L -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
#       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# # --- Delta Lake (Spark 3.5 compatible) ---
# # Dùng single-jar delta-spark 3.2.0 (khớp Scala 2.12)
# # Delta Lake
# RUN curl -L -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.2.0.jar \
#       https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
#     curl -L -o ${SPARK_JARS_DIR}/delta-storage-3.2.0.jar \
#       https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar


# --- Kafka Structured Streaming ---
# Connector + token provider cho Spark 3.5.0
# Spark 3.5.0 + Scala 2.12
FROM apache/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y curl wget && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip install pyspark==3.5.1 delta-spark==3.2.1

ENV SPARK_HOME=/opt/spark
ENV SPARK_JARS_DIR=$SPARK_HOME/jars
ENV PATH=$PATH:$SPARK_HOME/bin

# --- S3A (MinIO) ---
# Giữ nguyên Hadoop core từ image; CHỈ bổ sung hadoop-aws + aws sdk BUNDLE KHỚP 3.3.4
RUN curl -L -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# --- Delta Lake (Spark 3.5 compatible) ---
# Dùng single-jar delta-spark 3.2.0 (khớp Scala 2.12)
# Delta Lake
RUN curl -L -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar


# --- Kafka Structured Streaming ---
# Connector + token provider cho Spark 3.5.0
# Kafka Structured Streaming
RUN curl -L -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o ${SPARK_JARS_DIR}/lz4-java-1.8.0.jar \
      https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar  && \
    curl -L -o ${SPARK_JARS_DIR}/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
    # curl -L -o ${SPARK_JARS_DIR}/snappy-java-1.1.10.5.jar \
    #   https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar



# (Tùy chọn) Ghim kafka-clients, chỉnh version khớp Kafka broker (ví dụ 3.6.0)
# RUN curl -L -o ${SPARK_JARS_DIR}/kafka-clients-3.6.0.jar \
#       https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar

# Giữ container ở foreground khi chạy các daemons
ENV SPARK_NO_DAEMONIZE=true

EXPOSE 7077 8080 4040




# (Tùy chọn) Ghim kafka-clients, chỉnh version khớp Kafka broker (ví dụ 3.6.0)
# RUN curl -L -o ${SPARK_JARS_DIR}/kafka-clients-3.6.0.jar \
#       https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar

# Giữ container ở foreground khi chạy các daemons
ENV SPARK_NO_DAEMONIZE=true

EXPOSE 7077 8080 4040

