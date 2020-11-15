# This Docker image is based on debian
FROM debian:buster

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y wget procps file sudo

# set the name of the package
ENV MIMVERSION mimersql1104_11.0.4A
ENV DEBFILE ${MIMVERSION}-33898_amd64.deb       

# fetch the package and install it
RUN wget -nv -o {DEBFILE} http://ftp.mimer.com/pub/dist/linux_x64/${DEBFILE}
RUN mkdir /usr/lib32
RUN dpkg --install ${DEBFILE}

STOPSIGNAL SIGINT

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/sh","/start.sh"]
