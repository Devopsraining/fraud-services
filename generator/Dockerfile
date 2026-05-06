## generator/Dockerfile
FROM python:3.10
WORKDIR /app
COPY app.py .
RUN pip install google-cloud-pubsub
CMD ["python", "app.py"]