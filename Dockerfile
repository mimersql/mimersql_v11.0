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
FROM ubuntu:24.04

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    wget procps file sudo libdw1 apache2-utils ca-certificates \
    python3 python3-pip python3-setuptools python3-wheel curl net-tools && \
    rm -rf /var/lib/apt/lists/*

# fetch the package and install it
RUN case "$(uname -m)" in \
        aarch64) export MIMER_DEB="linux_arm_64/mimersqlsrv1109_11.0.9E-49534_arm64-openssl3.deb" ;; \
        x86_64)  export MIMER_DEB="linux_x86_64/mimersqlsrv1109_11.0.9G-52545_amd64-openssl3.deb" ;; \
    esac; \
    wget -nv -O mimersql.deb https://download.mimer.com/pub/dist/${MIMER_DEB} && \
    dpkg --install mimersql.deb && \
    rm mimersql.deb

STOPSIGNAL SIGINT

#install Python3 and required packages
RUN pip3 install --break-system-packages requests flask flask_htpasswd gunicorn mimerpy

RUN mkdir mimer_controller
COPY mimer_controller/*.pem /mimer_controller/
COPY mimer_controller/mimer_controller.py mimer_controller/
COPY mimer_controller/mimcontrol.py /mimer_controller/

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/sh","/start.sh"]
