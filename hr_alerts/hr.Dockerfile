FROM python:3.10
COPY hr_alert.py hr_alert_helpers.py /./
COPY requirements.txt  .
RUN  pip install -r requirements.txt 
CMD [ "python3", "-u", "./hr_alert.py" ]