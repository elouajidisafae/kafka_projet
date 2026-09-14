FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir pytest==8.2.0 pytest-cov==5.0.0

COPY . .
RUN pip install --no-cache-dir --no-deps -e .

EXPOSE 8080
CMD ["python", "main.py", "--mode", "web"]