FROM python:3.10-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    LOCAL_FOLDER=/data/api \
    LOCAL_SALES_FOLDER=/data/sales \
    DB_PATH=/data/hedonism.duckdb \
    API_FILES_BUCKET_NAME=disabled \
    SALES_FILES_BUCKET_NAME=disabled

RUN mkdir -p /data/api /data/sales

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

EXPOSE 8501

CMD ["streamlit", "run", "src/data_viz.py"]
