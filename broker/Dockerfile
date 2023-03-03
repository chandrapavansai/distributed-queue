FROM python:3.10-slim

WORKDIR /code


COPY . /code/broker

RUN python -m pip install --no-cache-dir -r /code/broker/requirements.txt

CMD ["uvicorn", "broker.main:app", "--host", "0.0.0.0", "--port", "8000","--reload"]
