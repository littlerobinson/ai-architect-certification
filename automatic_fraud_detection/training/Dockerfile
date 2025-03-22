FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /home

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/home

# Update the package list and install necessary system dependencies
RUN apt-get update && apt-get install -y \
    nano \
    unzip \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install the Deta CLI
# RUN curl -fsSL https://get.deta.dev/cli.sh | sh

# Copy the application code into the container
COPY . .

# Install Python dependencies from the requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Set the default command to run the Python application
CMD ["python", "app/train.py"]
