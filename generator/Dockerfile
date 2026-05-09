## generator/Dockerfile
FROM python:3.12
WORKDIR /app
COPY app.py .
RUN pip install google-cloud-pubsub
CMD ["python", "app.py"]