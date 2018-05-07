FROM clowder/pyclowder:onbuild

ENV EXTRACTION_FUNC="wordcount"

CMD python -c "from pyclowder.simpleextractor import SimpleExtractor; from wordcount import *; SimpleExtractor(${EXTRACTION_FUNC}).start()"
