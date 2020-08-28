FROM python:3.7-slim

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y git

RUN pip install git+https://scappon@opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder.git@command-line-app

COPY . .

ENTRYPOINT ["./entrypoint.sh"]