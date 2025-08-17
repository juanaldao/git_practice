# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Copy the service account key
COPY ppi-trading-bot-sa.json /app/

# Copy the script and install dependencies
COPY script.py /app/
COPY requirements.txt /app/

# Install required Python libraries
RUN pip install -r requirements.txt

# Set the environment variable for authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/ppi-trading-bot-sa.json"

# Run the script when the container starts
CMD ["python", "/app/script.py"]
