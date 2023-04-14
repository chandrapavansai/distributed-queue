FROM python:3.12-rc-alpine

WORKDIR /code

COPY . /code/raft-broker

RUN python -m pip install --no-cache-dir -r /code/raft-broker/requirements.txt

# CMD ["uvicorn", "raft-broker.main:app", "--host", "0.0.0.0", "--port", "8000","--reload", "--workers", "4"]
