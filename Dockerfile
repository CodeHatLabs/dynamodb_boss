FROM python:3.6-slim

RUN apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y git \
    && pip install --upgrade pip \
    && pip install pipenv \
    && rm -rf /var/lib/apt/lists/* \
    && find / | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf \
    && rm -rf /root/.cache/pip

WORKDIR /opt

COPY . .

CMD ["bash", "run-test.sh"]

