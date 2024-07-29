FROM python:3.11.4-alpine3.18

WORKDIR /src

COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "src/app.py"]
