FROM bitnami/spark:latest

USER root

# Install python dependencies
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Copy pipeline
COPY spark_pipeline.py /app/spark_pipeline.py

WORKDIR /app

CMD ["spark-submit", "spark_pipeline.py"]