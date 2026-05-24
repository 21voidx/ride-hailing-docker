FROM python:3.10-slim

RUN apt-get update && apt-get install -y git bash && \
    rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

CMD ["dbt", "--version"]

ride_hailing/
    dbt/
    ├── dbt_profiles/
    │     └── profiles.yml
    ├── dbt_project/
    │     ├── macros/        
    │     ├── models/ 
    │     ├── seeds/ 
    │     ├── tests/       
    │     └── dbt_project.yml
    └── docker/
          ├── Dockerfile     
          └── requirements.txt