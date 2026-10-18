FROM python:3.11-slim
WORKDIR /usr/python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY open_api_requests.py .
COPY transform.py .
COPY validate.py .
COPY data_load.py .
COPY scheduler.py .
ENV PYTHONUNBUFFERED=1
CMD ["python","scheduler.py"]
