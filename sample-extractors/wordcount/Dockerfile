FROM python:3.8

WORKDIR /extractor
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY wordcount.py extractor_info.json ./
CMD python wordcount.py