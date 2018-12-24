FROM rabbitmq:3-alpine

RUN mkdir -p /certs

COPY ca.pem server.key server.pem /certs/

ENV RABBITMQ_SSL_CERTFILE=/certs/server.pem
ENV RABBITMQ_SSL_KEYFILE=/certs/server.key
ENV RABBITMQ_SSL_CACERTFILE=/certs/CA.pem

ENV RABBITMQ_DEFAULT_USER=guest
ENV RABBITMQ_DEFAULT_PASS=guest
