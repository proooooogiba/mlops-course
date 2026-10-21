FROM python:3.11.13-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scripts ./scripts
COPY sql ./sql
COPY pipeline ./pipeline
COPY app ./app
COPY tests ./tests
CMD ["python", "-m", "app.scoring_worker"]
