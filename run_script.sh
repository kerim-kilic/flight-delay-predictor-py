#!/bin/bash

docker image build -t flight-delay-prediction-python:scripted_build .
docker container run -it -v $(pwd)/data/:/home/data/ -v $(pwd)/src/:/home/src/ --name container flight-delay-prediction-python:scripted_build
docker container rm container