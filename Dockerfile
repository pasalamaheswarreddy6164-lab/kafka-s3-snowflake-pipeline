FROM apache/spark:3.5.0

USER root

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY spark_pipeline.py /app/spark_pipeline.py

WORKDIR /app

CMD ["/opt/spark/bin/spark-submit","--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0","/app/spark_pipeline.py"]