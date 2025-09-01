# Common
repository that contains common methods and classes used in the UrbanAirLab project. 
    - InfluxDB client
    - MQTT client
    - logger
    - data processors
    - config handler

## Setup
The projects dependencies are managed with pipenv. To set up the project its needed to install pipenv via: 

````pip3 install pipenv````

To install the dependencies run

````pipenv install````

To connect to UrbanAirLabs InfluxDB or Mosquitto (MQTT Broker) the clients need a .env file containing the necessary 
credentials and route information e.g. domain, port, etc. 

## Run Tests
Unittest are written in pytest. To run all unittest of the project use:

````pipenv run pytest````