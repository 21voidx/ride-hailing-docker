FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir faker psycopg[binary] pymysql
COPY generator.py /app/generator.py
CMD ["python", "/app/generator.py"]
