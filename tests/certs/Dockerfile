# Test broker for aiormq. Build from the repository root:
#
#   make rabbitmq-image
#
# RabbitMQ 4.3.7 and later advertise the accept_consumer_cancel_ok
# capability, which the consumer cancel tests require.
FROM rabbitmq:4.4-management-alpine

RUN mkdir -p /certs/

COPY tests/certs/ca.pem /certs/
COPY tests/certs/server.key /certs/
COPY tests/certs/server.pem /certs/
COPY tests/certs/rabbitmq.conf /etc/rabbitmq/conf.d/20-aiormq.conf

ENV RABBITMQ_DEFAULT_USER=guest
ENV RABBITMQ_DEFAULT_PASS=guest
