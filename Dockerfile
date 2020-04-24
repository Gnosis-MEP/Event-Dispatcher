FROM registry.insight-centre.org/sit/mps/docker-images/base-services:latest

ADD . /service
WORKDIR /service
RUN rm Pipfile.lock && pipenv lock -vvv && pipenv --rm && \
    pipenv install --system && \
    pip install -e . && \
    rm -rf /tmp/pip* /root/.cache


