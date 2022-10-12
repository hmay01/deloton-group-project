FROM public.ecr.aws/lambda/python:3.8

COPY daily_report.py daily_report_helper.py ${LAMBDA_TASK_ROOT}

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD [ "daily_report.handler" ]