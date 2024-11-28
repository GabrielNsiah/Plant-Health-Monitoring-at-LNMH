FROM public.ecr.aws/lambda/python:3.9

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY extract.py .

COPY transform.py .

COPY load.py .

COPY etl.py .

EXPOSE 1433

CMD [ "etl.lambda_handler" ]