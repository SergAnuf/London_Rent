# Use the apache/airflow:2.7.3-python3.10 base image
FROM apache/airflow:2.7.3-python3.10

# Copy the requirements.txt into the container
COPY requirements.txt ./tmp/requirements.txt

# Upgrade pip to the latest version to avoid potential issues with older versions
RUN pip install -U pip

# Install dependencies from requirements.txt
RUN pip install -r ./tmp/requirements.txt

RUN pip install apify-client