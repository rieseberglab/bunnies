FROM rieseberglab/analytics:3

RUN apt-get update && apt-get install python3-pip --no-install-recommends && \
    pip install platform/python3.6 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
