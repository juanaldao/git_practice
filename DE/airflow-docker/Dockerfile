FROM apache/airflow:2.8.1-python3.10

# Copy your Python dependencies into the image
COPY ./dags/requirements.txt /requirements.txt

# Install them using pip
RUN pip install --no-cache-dir -r /requirements.txt
