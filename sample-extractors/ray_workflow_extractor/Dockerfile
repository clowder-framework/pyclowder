#* Ray on Docker: https://docs.ray.io/en/latest/ray-overview/installation.html?highlight=docker#docker-source-images

# USAGE
# 1.
# docker build -t kastanday/ray_workflow_extractor .
# 2.
# docker run kastanday/ray_workflow_extractor
# (Or optionally, go interactive) docker run -i -t kastanday/maple_minimamba /bin/bash

FROM rayproject/ray:nightly
# -CPU by default, runs Ubuntu image.
# -GPU runs NVIDIA image. Needs CUDA.

WORKDIR /extractor
COPY requirements.txt ./

RUN pip3 install -r requirements.txt

COPY rayWorkflow.py extractor_info.json ./
CMD python rayWorkflow.py
