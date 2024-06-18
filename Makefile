all: clean test

NAME:=$(shell poetry version -n | awk '{print $1}')
VERSION:=$(shell poetry version -s)
RABBITMQ_CONTAINER_NAME:=aiormq_rabbitmq
RABBITMQ_IMAGE:=mosquito/aiormq-rabbitmq

rabbitmq:
	docker compose down
	docker compose up -d

upload:
	poetry publish --build --skip-existing

test:
	poetry run pytest -vvx --cov=aiormq \
		--cov-report=term-missing tests README.rst

clean:
	rm -fr *.egg-info .tox

develop: clean
	poetry install
