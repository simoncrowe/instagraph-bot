FROM python:3.8-buster

ADD ./requirements.txt /build/
RUN pip install --no-cache-dir -r /build/requirements.txt && rm -r /build

ENV PYTHONPATH /
