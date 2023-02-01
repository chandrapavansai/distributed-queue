FROM python:3.10-slim

WORKDIR /code


COPY . /code

RUN python -m pip install --no-cache-dir -r /code/requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80","--reload"]
