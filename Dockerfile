from puckel/docker-airflow:1.10.9
COPY requirements.txt /tmp
WORKDIR /tmp
RUN pip install -r requirements.txt
