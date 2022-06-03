FROM continuumio/miniconda3
# FROM continuumio/miniconda3:4.5.4 
# ^^ doesn't work with h5py. Could be problem with conda 4.5.x version.
# (InvalidVersionSpecError: Invalid version spec: =2.7) for conda install/update commands.
# FROM continuumio/miniconda3:4.4.10

# https://pythonspeed.com/articles/activate-conda-dockerfile/#working 

# 1. MUST be x64 (no 3.6 in aarch64)
# export DOCKER_DEFAULT_PLATFORM=linux/amd64
# 2.
# docker build -t kastanday/maple_minimamba .
# 3.
# docker run kastanday/maple_minimamba
# Optional (interactive)
# docker run -i -t kastanday/maple_minimamba /bin/bash

LABEL name="MAPLE_CPU_KastanDay"
LABEL summary="Accept a single .tif image and perform MAPLE ML analysis, returning a set of shape files."
LABEL URL="https://github.com/rajithaud/MAPLE_Local"
LABEL maintaner="Kastan Day"
LABEL architecture="x86_64"
LABEL version="0.1"
LABEL release-date="2022-03-24"

WORKDIR /app

# necessary for cv2 https://exerror.com/importerror-libgl-so-1-cannot-open-shared-object-file-no-such-file-or-directory/
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsm6 \
    libxext6 \
    && apt-get clean

# Create the environment:
RUN conda install -c conda-forge mamba && \
    mamba create --name MAPLE_py36 python=3.6 -y
# Make RUN commands use the new environment:
RUN echo "conda activate MAPLE_py36" >> ~/.bashrc
SHELL ["/bin/bash", "--login", "-c"]
RUN mamba install -c conda-forge tensorflow=1.14 gdal opencv=4.5.3 scikit-image shapely pyshp cached_property keras tifffile pyparsing cycler python-dateutil h5py=2.10.0
RUN mamba clean -a -y

# install pyclowder
COPY requirements.txt ./
RUN pip install -r requirements.txt

# The code to run when container is started:
COPY maple_cpu_kastanday.py extractor_info.json MAPLE run.py entrypoint_script.sh ./

CMD python maple_cpu_kastanday.py


# RUN chmod +x entrypoint_script.sh
# ENTRYPOINT ["./entrypoint_script.sh"]

# # The code to run when container is started:
# COPY run.py .
# ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "myenv", "python", "run.py"]