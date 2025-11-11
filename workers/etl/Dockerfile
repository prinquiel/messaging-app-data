FROM python:3.11-slim

WORKDIR /worker

COPY etl/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir temporalio==1.6.0

COPY etl/ ./etl/
COPY workers/etl/worker.py ./workers/etl/worker.py

ENV PYTHONPATH=/worker

CMD ["python", "workers/etl/worker.py"]


