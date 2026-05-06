## Dockerfile for the processor service
FROM python:3.10
WORKDIR /app
COPY app.py .
RUN pip install google-cloud-pubsub requests
CMD ["python", "app.py"]