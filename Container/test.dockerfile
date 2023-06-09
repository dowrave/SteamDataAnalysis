FROM amd64/python:3.9.16-bullseye

RUN apt-get update && \
    rm -rf var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y default-mysql-client

WORKDIR /app


