FROM ubuntu:latest

#--------- Install usefull tools -----------
RUN apt-get update && apt-get install -y \
  openssh-server \
  git \
  curl \
  vim \
  apt-utils \
  iputils-ping \
  sudo

#--------- SETUP System -----------

# add user and sudo
RUN useradd -ms /bin/bash -g sudo inabox
# username= inabox, password= inabox
RUN echo 'inabox:inabox' | chpasswd
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Config SSH
# Set SSH timeout
RUN mkdir /var/run/sshd
#set timeout to auto-disconnect when idle (see man sshd)
#RUN echo 'ClientAliveInterval 60' >>  /etc/ssh/sshd_config
#RUN echo 'ClientAliveCountMax 10' >>  /etc/ssh/sshd_config
#RUN echo 'TCPKeepAlive no' >>  /etc/ssh/sshd_config

#--------- SETUP USER -----------

USER inabox
WORKDIR /home/inabox/

#------------- ROOT -------------

USER root

# Setup SSH
EXPOSE 22
# Start SSH Deamon in "not detach" mode. Once SSH connction breaks the container stops
CMD ["/usr/sbin/sshd", "-D"]
