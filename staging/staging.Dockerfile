FROM python:3.10
COPY aurora_staging.py staging_helpers.py /./
COPY requirements.txt  .
RUN  pip install -r requirements.txt 
CMD [ "python3", "-u", "./aurora_staging.py" ]