# This Docker image is based on Ubuntu
FROM ubuntu:24.04

# update and install necessary utilities
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    wget procps file sudo libdw1 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# MIMER_VERSION selects which major version's single .deb gets installed
# below.
ARG MIMER_VERSION=11.0

# fetch the package and install it
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
    wget -nv -O mimersql.deb https://download.mimer.com/pub/dist/${MIMER_DEB} && \
    dpkg --install mimersql.deb && \
    rm mimersql.deb

STOPSIGNAL SIGINT

# copy the start script and launch Mimer SQL
COPY start.sh /
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/sh","/start.sh"]
