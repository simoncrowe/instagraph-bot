FROM python:3.7-buster

ADD ./* $HOME/
RUN pip install --no-cache-dir -r requirements.txt 

ENV PYTHONPATH /ig
