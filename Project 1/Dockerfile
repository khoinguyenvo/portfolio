ARG BASE_IMAGE=python:3.11.8-slim
FROM "${BASE_IMAGE}" as dev

ENV PIP_NO_CACHE_DIR=True
ENV PYTHONDONTWRITEBYTECODE=1

RUN pip install -U \
    pip \
    setuptools \
    wheel 

WORKDIR /opt/dagster/app
COPY dbt ./dbt
COPY pipeline ./pipeline
COPY .env credentials.json ./
COPY requirements.txt ./
COPY setup.py pyproject.toml ./
COPY workspace.yaml ./

ENV POETRY_VIRTUALENVS_IN_PROJECT=false
RUN pip install -r requirements.txt

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME
COPY workspace.yaml dagster.yaml $DAGSTER_HOME

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]