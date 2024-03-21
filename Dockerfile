FROM ubuntu:22.04
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y python3.11-full python3-pip xvfb
RUN python3.11 -m pip install --upgrade pip
WORKDIR /code
COPY LICENSE.txt README.md pyproject.toml ./
COPY countess/ ./countess/
RUN python3.11 -m pip install -e .[dev]
COPY tests/ ./tests/
COPY script/ ./script/
CMD script/run-tests
