FROM ubuntu
USER root

RUN apt update && apt upgrade -y
RUN apt-get install python3-pip \
    openjdk-11-jre-headless -y

ADD ./requirements.txt /home/requirements.txt
RUN pip install -r /home/requirements.txt
