# Image Preview Example Extractor for Clowder V2

This is an example image preview extractor that works with Clowder V2.

## Prerequisites

Clowder V2 stack (Docker services, Backend, and Frontend) running in the current machine. For more details, please see
the instructions [here](https://github.com/clowder-framework/clowder2#readme).

## Command Line Instructions

### Create and setup Python virtual environment

```shell
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Run extractor

```shell
export CLOWDER_VERSION=2 && export IMAGE_BINARY="/usr/local/bin/convert" && python binary_extractor.py
```

## Docker Instructions

## Build Docker image

`docker build -t clowder/extractors-image-preview .`

## Run Docker Container

`docker run --rm -e CLOWDER_VERSION=2 --network clowder2 --link rabbitmq --link clowder2 clowder/extractors-image-preview`

