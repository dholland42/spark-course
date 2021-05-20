ARG UBUNTU_VERSION=latest

FROM ubuntu:${UBUNTU_VERSION}

ARG DEV_USER=root

ENV DEBIAN_FRONTEND noninteractive

# general setup
RUN apt-get update
RUN apt-get install -y sudo zsh git vim htop openssh-server less curl gnupg-agent software-properties-common 

# set up shared home directory users
ENV USERHOME=/home/users
RUN mkdir $USERHOME

# zsh setup
RUN curl -fsSL -o /opt/omz.sh https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh
RUN ZSH=/opt/.zsh sh /opt/omz.sh --unattended
RUN chsh -s /bin/zsh root

# python setup
RUN apt-get install -y python3 python3-pip
RUN ln -s $(which python3) /usr/local/bin/python
RUN ln -s $(which pip3) /usr/local/bin/pip

# get poetry and ignore virtual envs because we're in a container
ENV POETRY_HOME=/opt/poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
RUN chmod a+x /opt/poetry/bin/poetry
RUN echo 'export PATH="$PATH:/opt/poetry/bin"' >> $HOME/.zshrc
# RUN /opt/poetry/bin/poetry config virtualenvs.create false

# Spark dependencies
# Default values can be overridden at build time
# (ARGS are in lower case to distinguish them from ENV)
ARG spark_version="3.1.1"
ARG hadoop_version="3.2"
ARG spark_checksum="E90B31E58F6D95A42900BA4D288261D71F6C19FA39C1CB71862B792D1B5564941A320227F6AB0E09D946F16B8C1969ED2DEA2A369EC8F9D2D7099189234DE1BE"
ARG openjdk_version="11"

ENV APACHE_SPARK_VERSION="${spark_version}" \
    HADOOP_VERSION="${hadoop_version}"

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y \
    "openjdk-${openjdk_version}-jre-headless" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Spark installation
WORKDIR /tmp
RUN wget -q "https://archive.apache.org/dist/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    echo "${spark_checksum} *spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | sha512sum -c - && \
    tar xzf "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /usr/local --owner root --group root --no-same-owner && \
    rm "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

WORKDIR /usr/local

# Configure Spark
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin

RUN ln -s "spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" spark && \
    # Add a link in the before_notebook hook in order to source automatically PYTHONPATH
    mkdir -p /usr/local/bin/before-notebook.d && \
    ln -s "${SPARK_HOME}/sbin/spark-config.sh" /usr/local/bin/before-notebook.d/spark-config.sh

# Fix Spark installation for Java 11 and Apache Arrow library
# see: https://github.com/apache/spark/pull/27356, https://spark.apache.org/docs/latest/#downloading
RUN cp -p "$SPARK_HOME/conf/spark-defaults.conf.template" "$SPARK_HOME/conf/spark-defaults.conf" && \
    echo 'spark.driver.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true' >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo 'spark.executor.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true' >> $SPARK_HOME/conf/spark-defaults.conf

RUN echo 'export PATH="$PATH:/usr/local/spark/bin"' >> $HOME/.zshrc

# Add the PySpark classes to the Python path:
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"

# install pyarrow
RUN pip install --no-cache-dir pyarrow

# install any other dependencies we may need
RUN mkdir /opt/dependencies
COPY poetry.lock /opt/dependencies
COPY poetry.toml /opt/dependencies
COPY pyproject.toml /opt/dependencies
WORKDIR /opt/dependencies
RUN /opt/poetry/bin/poetry install

# set up the shared home directory
RUN cp $HOME/.zshrc $USERHOME/.zshrc
RUN chgrp users $USERHOME
RUN chmod g+w $USERHOME

RUN echo "umask 002" >> $USERHOME/.zshrc

COPY adduser.sh /opt/adduser.sh

RUN bash /opt/adduser.sh ${DEV_USER}

USER ${DEV_USER}

WORKDIR $USERHOME

ENTRYPOINT ["/bin/bash"]