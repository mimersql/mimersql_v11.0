# This Docker image is based on Ubuntu
FROM ubuntu:22.04

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y wget procps file sudo libdw1

# set the name of the package
ENV DEBFILE mimersqlsrv1108_11.0.8E-46583_amd64-openssl3.deb

# fetch the package and install it
RUN wget -nv -o {DEBFILE} https://download.mimer.com/pub/dist/linux_x86_64/${DEBFILE}
RUN dpkg --install ${DEBFILE}
STOPSIGNAL SIGINT

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/sh","/start.sh"]
