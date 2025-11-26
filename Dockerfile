# This Docker image is based on Ubuntu
FROM ubuntu:24.04

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    wget procps file sudo libdw1 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# fetch the package and install it
RUN case "$(uname -m)" in \
        aarch64) export MIMER_DEB="linux_arm_64/mimersqlsrv1109_11.0.9E-49534_arm64-openssl3.deb" ;; \
        x86_64)  export MIMER_DEB="linux_x86_64/mimersqlsrv1109_11.0.9E-49534_amd64-openssl3.deb" ;; \
    esac; \
    wget -nv -O mimersql.deb https://download.mimer.com/pub/dist/${MIMER_DEB} && \
    dpkg --install mimersql.deb && \
    rm mimersql.deb

STOPSIGNAL SIGINT

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/sh","/start.sh"]
