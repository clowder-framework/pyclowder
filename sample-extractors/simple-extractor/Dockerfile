
ENV EXTRACTION_FUNC=""
ENV EXTRACTION_MODULE=""

COPY simple_extractor.py .

RUN apt-get update && apt-get install -y build-essential python3-dev

# install any packages
ONBUILD COPY packages.* Dockerfile /home/clowder/
ONBUILD RUN if [ -e packages.apt ]; then \
                apt-get -q -q update \
                && xargs apt-get -y install --no-install-recommends < packages.apt \
                && rm -rf /var/lib/apt/lists/*; \
            fi

# install any python packages
ONBUILD COPY requirements.txt* Dockerfile /home/clowder/
ONBUILD RUN if [ -e requirements.txt ]; then \
                pip install --no-cache-dir -r requirements.txt; \
            fi

# copy all files
ONBUILD ADD . /home/clowder/

CMD python -c "from simple_extractor import SimplePythonExtractor; from ${EXTRACTION_MODULE} import *; SimplePythonExtractor(${EXTRACTION_FUNC}).start()"
