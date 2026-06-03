FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.35.0 \
    trino==0.328.0 \
    pandas==2.2.2 \
    sqlalchemy==2.0.30

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true"]
