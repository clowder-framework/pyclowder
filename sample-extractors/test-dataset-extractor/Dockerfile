FROM python:3.8

WORKDIR /extractor
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY test-dataset-extractor.py extractor_info.json ./
CMD python test-dataset-extractor.py