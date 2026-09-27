FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
EXPOSE 8000
CMD ["uvicorn", "serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
