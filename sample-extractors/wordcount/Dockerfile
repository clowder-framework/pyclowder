# FROM clowder/python-base
FROM ncsa/pyclowder2:latest
MAINTAINER Rob Kooper <kooper@illinois.edu>

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
# REGISTRATION_ENDPOINTS should point to a central clowder instance, for example it
# could be https://clowder.ncsa.illinois.edu/clowder/api/extractors?key=secretKey
ENV RABBITMQ_URI="" \
    RABBITMQ_EXCHANGE="clowder" \
    REGISTRATION_ENDPOINTS="https://clowder.ncsa.illinois.edu/extractors" \
    MAIN_SCRIPT="wordcount.py"

# Install any programs needed
# RUN apt-get update && apt-get install -y \
#        imagemagick \
#     && rm -rf /var/lib/apt/lists/*   

# Switch to clowder, copy files and be ready to run
USER clowder

# command to run when starting docker
COPY entrypoint.sh *.py extractor_info.json /home/clowder/
ENTRYPOINT ["/home/clowder/entrypoint.sh"]
CMD ["extractor"]
