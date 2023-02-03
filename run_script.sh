#!/bin/bash

docker image build -t flight-delay-prediction-python:scripted_build .
docker container run -it -v ~/PycharmProjects/flight-delay-prediction-python/data/:/home/data/ -v ~/PycharmProjects/flight-delay-prediction-python/src/:/home/src/ -v ~/PycharmProjects/flight-delay-prediction-python/main.py:/home/main.py --name container flight-delay-prediction-python:scripted_build
docker container rm container