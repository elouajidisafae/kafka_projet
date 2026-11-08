FROM python:3.11.13-slim-bookworm
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev gcc && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY core ./core
COPY interfaces ./interfaces
COPY templates ./templates
COPY static ./static
COPY main.py pyproject.toml ./
RUN pip install --no-cache-dir --no-deps -e .
EXPOSE 8080
CMD ["python", "main.py", "--mode", "web"]
