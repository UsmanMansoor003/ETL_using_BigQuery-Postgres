FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y

ENV KUBECTL_VERSION v1.22.2

RUN apt-get update -y
RUN apt-get install -y apt-utils
RUN apt-get install -y pkg-config
RUN apt-get install -y libsystemd-dev

RUN apt-get update && \
  apt-get install -y curl jq gnupg python3.8 python3-pip git && \
  curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl && \
  chmod +x ./kubectl && \
  mv ./kubectl /usr/local/bin/kubectl && \
  export CLOUD_SDK_REPO="cloud-sdk-cosmic" && \
  echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
  apt-get update -y && apt-get install google-cloud-sdk -y

COPY ./requirements.txt /opt
COPY script /opt/script


RUN pip3 install -r /opt/requirements.txt && \
  python3 /opt/script/ETL_script_for_PostgresSql.py && \
  python3 /opt/script/ETL_script_for_BigQuery.py