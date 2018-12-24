FROM rabbitmq:3-alpine

RUN mkdir -p /certs

COPY tests/certs/ca.pem /certs/
COPY tests/certs/server.key /certs/
COPY tests/certs/server.pem /certs/

ENV RABBITMQ_SSL_CERTFILE=/certs/server.pem
ENV RABBITMQ_SSL_KEYFILE=/certs/server.key
ENV RABBITMQ_SSL_CACERTFILE=/certs/ca.pem

ENV RABBITMQ_DEFAULT_USER=guest
ENV RABBITMQ_DEFAULT_PASS=guest
