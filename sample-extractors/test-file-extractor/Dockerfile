FROM python:3.8

WORKDIR /extractor
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY test-file-extractor.py extractor_info.json ./
CMD python test-file-extractor.py