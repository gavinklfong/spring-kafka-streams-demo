#!/bin/bash

VERSION=${1-latest}

docker build -t whalebig27/stock-price-producer:${VERSION} \
  --pull \
  -f docker/stock-price-producer/Dockerfile .

docker push whalebig27/stock-price-producer:${VERSION}
