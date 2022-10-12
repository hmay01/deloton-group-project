FROM python:3.10
COPY app.py app_helpers.py /./
COPY requirements.txt  .
RUN  pip install -r requirements.txt 
CMD CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]