FROM apache/airflow:2.10.4

# Switch user
USER root

# PEM Key
COPY secrets/jedha-lead-certification-project.pem /opt/airflow/

# Change the UID of airflow user to 1000
RUN usermod -u 1000 airflow

# Ensure correct permissions for the .pem file
RUN chmod 400 /opt/airflow/jedha-lead-certification-project.pem \
    && chown airflow /opt/airflow/jedha-lead-certification-project.pem

# Switch back to airflow user
USER airflow

# Install any additional dependencies if needed
COPY requirements.txt requirements.txt 
RUN pip install -r requirements.txt

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt