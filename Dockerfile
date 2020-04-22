# This Docker image is based on debian
FROM debian:latest

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y zenity systemd xinetd wget procps file

# set the name of the package
ENV debfile mimersql1103_11.0.3A-31718_amd64.deb

# fetch the package and install it
RUN wget http://ftp.mimer.com/pub/beta/linux_x64/${debfile}
RUN dpkg -i ${debfile}

# create a new, empty database
RUN dbinstall -q -d -u root mimerdb SYSADM /usr/local/MimerSQL

# export the database port
EXPOSE 1360

# copy the start script and launch Mimer SQL
COPY start.sh /
CMD [ "/start.sh" ]
