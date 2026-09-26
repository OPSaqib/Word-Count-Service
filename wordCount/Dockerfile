# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container (in Docker where all commands will run from)
WORKDIR /app

RUN pip install --no-cache-dir \
    rpyc \
    redis \
    matplotlib

COPY . .

CMD ["python", "server.py"]