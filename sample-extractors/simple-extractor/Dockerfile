FROM clowder/pyclowder:onbuild

ENV EXTRACTION_FUNC=os.env.get('EXTRACTION_FUNC', '')
ENV EXTRACTION_MODULE=os.env.get('EXTRACTION_MODULE', '')

CMD python -c "from simple_extractor import SimpleExtractor; from ${EXTRACTION_MODULE} import *; SimpleExtractor(${EXTRACTION_FUNC}).start()"