all: clean sdist test

NAME:=$(shell poetry version -n | awk '{print $1}')
VERSION:=$(shell poetry version -s)
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

sdist:
	poetry build -f sdist

upload: sdist
	poetry upload

test:
	poetry run pytest -vvx --cov=aiormq \
		--cov-report=term-missing tests README.rst

clean:
	rm -fr *.egg-info .tox

develop: clean
	poetry install
