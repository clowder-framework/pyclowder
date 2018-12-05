FROM clowder/pyclowder:latest

ENV R_SCRIPT="" \
    R_FUNCTION=""

RUN echo "deb http://cran.rstudio.com/bin/linux/ubuntu xenial/" > /etc/apt/sources.list.d/R.list \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E084DAB9 \
    && apt-get -q -q update \
    && apt-get -y install --no-install-recommends r-base-core r-base-dev \
    && rm -rf /var/lib/apt/lists/* \
    && Rscript --vanilla -e "install.packages('jsonlite', repos='http://cran.rstudio.com/'); print(system.file(package = 'jsonlite')); q(status=as.integer(system.file(package = 'jsonlite') == ''))"

COPY r_extractor.py launcher.R /home/clowder/

# install any packages
ONBUILD COPY packages.* Dockerfile /home/clowder/
ONBUILD RUN if [ -e packages.apt ]; then \
                apt-get -q -q update \
                && xargs apt-get -y install --no-install-recommends < packages.apt \
                && rm -rf /var/lib/apt/lists/*; \
            fi

# install any r packages
ONBUILD COPY docker.R* Dockerfile /home/clowder/
ONBUILD RUN if [ -e docker.R ]; then \
                Rscript docker.R; \
            fi

# copy all files
ONBUILD ADD . /home/clowder/

CMD python r_extractor.py
