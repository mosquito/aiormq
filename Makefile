all: clean test

RABBITMQ_CONTAINER_NAME:=aiormq_rabbitmq
RABBITMQ_IMAGE:=mosquito/aiormq-rabbitmq

rabbitmq:
	docker kill $(RABBITMQ_CONTAINER_NAME) || true
	docker run --pull=always --rm -d \
		--name $(RABBITMQ_CONTAINER_NAME) \
		-p 5671:5671 \
		-p 5672:5672 \
		-p 15671:15671 \
		-p 15672:15672 \
		$(RABBITMQ_IMAGE)

upload:
	uv build
	uv publish

test:
	uv run pytest -vvx --cov=aiormq \
		--cov-report=term-missing tests README.rst

clean:
	rm -fr *.egg-info .tox

develop: clean
	uv sync
