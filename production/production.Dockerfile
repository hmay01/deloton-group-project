FROM public.ecr.aws/lambda/python:3.8

COPY production_helpers.py aurora_production_v2.py ${LAMBDA_TASK_ROOT}

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD [ "aurora_production_v2.handler" ]