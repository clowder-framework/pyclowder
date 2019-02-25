FROM python:3.5

ENV RABBITMQ_URI="amqp://guest:guest@rabbitmq/%2F"
EXPOSE 9999

RUN pip install pika==0.11.2 requests==2.18.4

WORKDIR /src
COPY monitor.py /src/

CMD python monitor.py
