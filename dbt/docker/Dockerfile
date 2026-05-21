FROM python:3.10-slim

RUN apt-get update && apt-get install -y git bash && \
    rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

CMD ["dbt", "--version"]