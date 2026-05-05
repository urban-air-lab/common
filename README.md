# Common
repository that contains common methods and classes used in the UrbanAirLab project. 
    - InfluxDB client
    - MQTT client
    - logger
    - data processors
    - config handler

## Setup
The projects dependencies are managed with uv. To install and get more information about uv, follow this documentation:
```
https://docs.astral.sh/uv/getting-started/installation/
```

To install the dependencies run

```
uv sync --locked
```

To connect to UrbanAirLabs InfluxDB or Mosquitto (MQTT Broker) the clients need a .env file containing the necessary 
credentials and route information e.g. domain, port, etc. You can get this information from you Supervisor. 

## Bump up version number
After changing the version in pyproject.toml you need to run

```
uv lock
```

## Run Tests
Tests are base on Pytest - run all tests via command line:

```
uv run pytest
```

## Use Ruff for linting
fix linting errors
```
ruff check --fix .
```

format code
```
ruff format . 
```

## Use MyPy for static type checking: 

```
mypy .
```

## Use isort for sort imports
```
isort .
```

## Use pip-audit for dependency vulnerabilities 
```
pip-audit
```