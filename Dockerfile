FROM ubuntu:trusty

MAINTAINER real2time <info@real2time.com>

# This container
ENV R2T_STORM_LISTEN_ADDRESS 0.0.0.0
ENV R2T_STORM_LISTEN_PORT 44420
ENV R2T_STORM_HOME /opt/real-2-time-storm/

# Remote services
ENV NIMBUS_HOST 127.0.0.1

RUN apt-get update; apt-get install -y maven git openjdk-7-jdk wget vim openssh-server supervisor

RUN mkdir -p /var/run/sshd /var/log/supervisor

RUN echo 'root:real2time' | chpasswd; sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config


RUN wget -q -O - http://apache.rediris.es/storm/apache-storm-0.9.3/apache-storm-0.9.3.tar.gz | tar -xzf - -C /opt
ENV STORM_HOME /opt/apache-storm-0.9.3

RUN cd /tmp; git clone https://github.com/real2time/ducksboard.git; cd ducksboard; mvn -l output.log install; cd /tmp; rm -rf ducksboard

RUN cd /opt; git clone https://github.com/real2time/real-2-time-storm.git; cd real-2-time-storm; mvn -l output.log package; mvn -l output.log clean

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY docker-entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

EXPOSE $R2T_STORM_LISTEN_PORT
EXPOSE 22