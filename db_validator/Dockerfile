FROM python_influxdb_base

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

CMD python3 -u script.py
