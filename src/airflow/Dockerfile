FROM apache/airflow:2.2.4

USER root

# Install OpenJDK-8
RUN apt-get update -y && \
    apt-get install -y software-properties-common && \
    sudo apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get install -y python3-pip python-dev build-essential gcc libsasl2-dev && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

# install pip Requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
