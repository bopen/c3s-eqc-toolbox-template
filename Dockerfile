FROM continuumio/miniconda3

WORKDIR /src/c3s-eqc-toolbox-template

COPY environment.yml /src/c3s-eqc-toolbox-template/

RUN conda install -c conda-forge gcc python=3.10 \
    && conda env update -n base -f environment.yml

COPY . /src/c3s-eqc-toolbox-template

RUN pip install --no-deps -e .
