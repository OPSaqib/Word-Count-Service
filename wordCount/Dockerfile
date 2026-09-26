# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container (in Docker where all commands will run from)
WORKDIR /app

# Install the packages and libraries required
RUN pip install --no-cache-dir \
    rpyc \
    redis \
    matplotlib

# Copy the current directory contents into the container at /app
COPY . .

# Run server.py when the container launches
CMD ["python", "server.py"]