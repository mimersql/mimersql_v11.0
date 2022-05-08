# This Docker image is based on Ubuntu
FROM ubuntu:20.04

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y wget procps file sudo

# set the name of the package
ENV MIMVERSION mimersqlsrv1106_11.0.6C
ENV DEBFILE ${MIMVERSION}-37151_amd64.deb

# fetch the package and install it
RUN wget -nv -o {DEBFILE} http://ftp.mimer.com/pub/beta/linux_x86_64/${DEBFILE}
RUN dpkg --install ${DEBFILE}
STOPSIGNAL SIGINT

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/sh","/start.sh"]
