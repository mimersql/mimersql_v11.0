# This Docker image is based on debian
FROM debian:buster

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y wget procps file

# set the name of the package
ENV DEBFILE mimersql1103_11.0.3C-32192_amd64.deb

# fetch the package and install it
RUN wget http://ftp.mimer.com/pub/beta/linux_x64/${DEBFILE}
RUN dpkg --install ${DEBFILE}

STOPSIGNAL SIGINT

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
CMD ["/bin/sh", "/start.sh"]
