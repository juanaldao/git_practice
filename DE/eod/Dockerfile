FROM apache/airflow:2.11.0

# Optional: declare the build-time variable
ARG AIRFLOW_VERSION=2.11.0

# Copy requirements.txt into the image
# Tambien se puede usar ADD (que tmbn puede abrir files .tar o buscar en una URL)
COPY requirements.txt .

# Install Airflow (es la version definida arriba) and other dependencies
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
