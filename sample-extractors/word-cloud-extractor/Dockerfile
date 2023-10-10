FROM python:3.8

WORKDIR /extractor
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY word-cloud-extractor.py extractor_info.json ./
CMD python word-cloud-extractor.py