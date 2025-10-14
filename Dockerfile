FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# requirements
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

# streamlit config + source code
COPY .streamlit /app/.streamlit
COPY . /app

EXPOSE 8501
CMD ["bash","-lc","streamlit run app.py --server.port 8501 --server.address 0.0.0.0"]
