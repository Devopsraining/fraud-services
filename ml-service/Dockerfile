## Dockerfile for ML Service
FROM python:3.10
WORKDIR /app
COPY app.py .
RUN pip install fastapi uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]