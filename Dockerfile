FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install system dependencies (headless Java for Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk bash && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find it (use distro default-java symlink)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SHELL=/bin/bash
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python dependencies
RUN pip install --no-cache-dir debugpy pyspark matplotlib

# Copy source (place app files at /app so ./data path in code resolves to /app/data inside the container)
COPY app/ .

EXPOSE 5678 8000

CMD ["python", "-u", "main.py"]