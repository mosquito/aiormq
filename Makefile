all: clean test

RABBITMQ_CONTAINER_NAME:=aiormq_rabbitmq
RABBITMQ_IMAGE:=mosquito/aiormq-rabbitmq
RABBITMQ_IMAGE_TAG:=4.4
RABBITMQ_IMAGE_PLATFORMS:=linux/amd64,linux/arm64
RABBITMQ_BUILDER:=aiormq

# Builds the test broker image for the host architecture only.
rabbitmq-image:
	docker build -f tests/certs/Dockerfile \
		-t $(RABBITMQ_IMAGE):$(RABBITMQ_IMAGE_TAG) \
		-t $(RABBITMQ_IMAGE):latest .

# A multi-platform build needs the docker-container driver.
rabbitmq-builder:
	docker buildx inspect $(RABBITMQ_BUILDER) > /dev/null 2>&1 || \
		docker buildx create --name $(RABBITMQ_BUILDER) \
			--driver docker-container

# Builds and pushes the image for every platform. CI runs on linux/amd64.
rabbitmq-image-push: rabbitmq-builder
	docker buildx build --builder $(RABBITMQ_BUILDER) \
		--platform $(RABBITMQ_IMAGE_PLATFORMS) \
		-f tests/certs/Dockerfile \
		-t $(RABBITMQ_IMAGE):$(RABBITMQ_IMAGE_TAG) \
		-t $(RABBITMQ_IMAGE):latest \
		--push .

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
		--cov-report=term-missing tests README.md

clean:
	rm -fr *.egg-info .tox

develop: clean
	uv sync
