all: clean sdist test upload

NAME:=$(shell python3 setup.py --name)
VERSION:=$(shell python3 setup.py --version | sed 's/+/-/g')
RABBITMQ_CONTAINER_NAME:=aiormq_rabbitmq
RABBITMQ_IMAGE:=mosquito/aiormq-rabbitmq

rabbitmq:
	docker pull $(RABBITMQ_IMAGE)
	docker kill $(RABBITMQ_CONTAINER_NAME)
	docker run --rm -d \
		--name $(RABBITMQ_CONTAINER_NAME) \
		-p 5671:5671 \
		-p 5672:5672 \
		-p 15671:15671 \
		-p 15672:15672 \
		$(RABBITMQ_IMAGE)

sdist:
	python3 setup.py sdist bdist_wheel

upload: sdist
	twine upload dist/*$(VERSION)*

quick-test:
	TEST_QUICK='1' pytest -x --cov=aiormq --cov-report=term-missing tests

test: quick-test
	tox

clean:
	rm -fr *.egg-info .tox

develop: clean
	virtualenv -p python3.6 env
	env/bin/pip install -Ue '.'
	env/bin/pip install -Ue '.[develop]'
