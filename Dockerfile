# syntax=docker/dockerfile:1
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

# --- Builder stage: only fetches the Mimer SQL .deb ------------------------
# wget/ca-certificates are only needed to download the package, never at
# runtime, so they're kept out of the final image entirely (see below).
FROM ubuntu:24.04 AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# MIMER_VERSION selects which major version's single .deb gets fetched
# below.
ARG MIMER_VERSION=11.0

RUN set -e; \
    case "$(uname -m)" in \
        aarch64) MIMER_ARCH="arm64"; MIMER_ARCH_DIR="linux_arm_64" ;; \
        x86_64)  MIMER_ARCH="amd64"; MIMER_ARCH_DIR="linux_x86_64" ;; \
        *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    if [ "${MIMER_VERSION}" = "11.0" ]; then \
        case "${MIMER_ARCH}" in \
            arm64) MIMER_DEB="${MIMER_ARCH_DIR}/mimersqlsrv1109_11.0.9E-49534_arm64-openssl3.deb" ;; \
            amd64) MIMER_DEB="${MIMER_ARCH_DIR}/mimersqlsrv1109_11.0.9G-52545_amd64-openssl3.deb" ;; \
        esac; \
    else \
        # --- 11.1 PLACEHOLDER --------------------------------------------
        # Reusing the 11.0 filenames for now so the image actually builds.
        # Replace these with the real 11.1 filenames once published.
        case "${MIMER_ARCH}" in \
            arm64) MIMER_DEB="${MIMER_ARCH_DIR}/mimersqlsrv1109_11.0.9E-49534_arm64-openssl3.deb" ;; \
            amd64) MIMER_DEB="${MIMER_ARCH_DIR}/mimersqlsrv1109_11.0.9G-52545_amd64-openssl3.deb" ;; \
        esac; \
    fi; \
    wget -nv -O /tmp/mimersql.deb https://download.mimer.com/pub/dist/${MIMER_DEB}

# --- Final image -------------------------------------------------------
FROM ubuntu:24.04

# openssl: real (declared) Mimer SQL dependency, made explicit.
# sudo: mimsqlhosts (called by start.sh) and mimlink/mimldconf (run
#   from the package's own postinst) reference it internally.
# procps: mimcontrol.py shells out to `ps` (show_db_perf).
# apache2-utils: provides `htpasswd`, called directly by start.sh to
#   create the REST controller's auth file.
# ca-certificates: needed for pip to fetch packages over HTTPS below.
# python3/pip/setuptools/wheel: run the REST controller and install its
#   dependencies (not split into their own build stage yet).
# Dropped vs. before: wget (build-only, see builder stage above), file
# and libdw1 (unused by anything here or in the Mimer SQL package
# itself), curl and net-tools (no code anywhere calls them).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openssl sudo procps apache2-utils ca-certificates \
    python3 python3-pip python3-setuptools python3-wheel && \
    rm -rf /var/lib/apt/lists/*

# install the package fetched by the builder stage. The bind-mount means
# the .deb itself is never written into a layer of the final image.
RUN --mount=type=bind,from=builder,source=/tmp/mimersql.deb,target=/tmp/mimersql.deb \
    dpkg --install /tmp/mimersql.deb

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
