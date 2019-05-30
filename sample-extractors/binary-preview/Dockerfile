ARG PYCLOWDER_PYTHON=""
FROM clowder/pyclowder${PYCLOWDER_PYTHON}:onbuild

ENV MAIN_SCRIPT="binary_extractor.py" \
    RABBITMQ_QUEUE="" \
    IMAGE_BINARY="" \
    IMAGE_TYPE="" \
    IMAGE_THUMBNAIL_COMMAND="" \
    IMAGE_PREVIEW_COMMAND="" \
    PREVIEW_BINARY="" \
    PREVIEW_TYPE="" \
    PREVIEW_COMMAND=""

ONBUILD COPY packages.* Dockerfile /home/clowder/
ONBUILD RUN if [ -e packages.apt ]; then \
                apt-get -q -q update \
                && xargs apt-get -y install --no-install-recommends < packages.apt \
                && rm -rf /var/lib/apt/lists/*; \
            fi

ONBUILD COPY extractor_info.json /home/clowder/
