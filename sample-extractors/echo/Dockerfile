ARG PYCLOWDER_PYTHON=""
FROM clowder/pyclowder${PYCLOWDER_PYTHON}:latest

# folder for code
WORKDIR /src

COPY echo.py extractor_info.json /src/

# command to run when starting container
CMD python ./echo.py
