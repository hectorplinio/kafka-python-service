FROM python:3.11-slim-buster

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

COPY . .

EXPOSE 3000

ENV PYTHONPATH /app

CMD ["python", "./src/app.py"]
