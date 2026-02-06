FROM python:3.12-slim

RUN pip install --no-cache-dir paho-mqtt

WORKDIR /app
COPY run.py .

CMD ["python3", "/app/run.py"]
