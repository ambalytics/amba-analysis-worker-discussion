FROM python:3.7-alpine

WORKDIR /app
COPY . .
RUN apk add --update --no-cache --virtual .build-deps \
        g++ \
        python-dev \
        libxml2 \
        libxml2-dev && \
    apk add libxslt-dev && \
    pip install --no-cache-dir -r requirements.txt && \
RUN python3 twitter_worker.py