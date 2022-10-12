FROM --platform=linux/x86-64 python
COPY app.py app_helpers.py /./
COPY requirements.txt  .
RUN  pip install -r requirements.txt 
CMD [ "python3", "app.py"]