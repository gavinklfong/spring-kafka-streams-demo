#!/bin/bash

docker build -t whalebig27/stock-price-producer:latest \
  --pull \
  -f docker/stock-price-producer/Dockerfile .

docker push whalebig27/stock-price-producer:latest
