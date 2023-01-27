FROM python:3.10-slim

WORKDIR /code

COPY ./requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY ./broker ./broker

CMD ["uvicorn", "broker.main:app", "--host", "0.0.0.0", "--port", "80","--reload"]