FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY taskforge/ ./taskforge/
COPY demo/ ./demo/
COPY benchmarks/demo_api.py ./benchmarks/demo_api.py

ENV PYTHONUNBUFFERED=1 \
    REDIS_URL=redis://redis:6379/0

ENTRYPOINT ["python", "-m", "taskforge.cli"]
CMD ["worker", "--handlers-module", "demo.jobs", "--concurrency", "4"]
