# Copyright (c) 2017 Mimer Information Technology

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# This Docker image is based on Ubuntu
FROM ubuntu:22.04

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y wget procps file apache2-utils sudo libdw1

# set the name of the package
ENV DEBFILE=mimersqlsrv1108_11.0.8E-46583_amd64-openssl3.deb

# fetch the package and install it
RUN wget -nv -o {DEBFILE} https://download.mimer.com/pub/dist/linux_x86_64/${DEBFILE}
RUN dpkg --install ${DEBFILE}
STOPSIGNAL SIGINT

#install Python3 and required packages
RUN apt-get -y install python3 python3-pip python3-setuptools python3-wheel curl net-tools
RUN pip3 install requests
RUN pip3 install flask flask_htpasswd
RUN pip3 install gunicorn
RUN pip3 install mimerpy

RUN mkdir mimer_controller
COPY mimer_controller/*.pem /mimer_controller/
COPY mimer_controller/mimer_controller.py mimer_controller/
COPY mimer_controller/mimcontrol.py /mimer_controller/

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/sh","/start.sh"]
