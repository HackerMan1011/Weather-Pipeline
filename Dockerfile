FROM python:3.11-slim
WORKDIR /usr/python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY open_api_requests.py .
COPY data_load.py .
CMD ["python","open_api_requests.py"]
