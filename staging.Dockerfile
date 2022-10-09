FROM python:3.10
COPY aurora_staging.py aurora_postgres_helpers.py kafka_helpers.py transformation_helpers.py hr_alert_helpers.py /./
COPY requirements.txt  .
RUN  pip install -r requirements.txt 
CMD [ "python3", "-u", "./aurora_staging.py" ]