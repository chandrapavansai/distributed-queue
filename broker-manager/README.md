# Broker Manager

## Endpoint Testing

### Steps

#### Requirements
pytest

#### Commands
+ Start the broker instances and database instances
    - Go to main directory `cd ..` 
    - `docker compose --file=docker-compose-endpoint.yml up`

+ Install dependencies
    - `cd broker-manager`
    - `python3 -m venv venv-mgr`
    - Windows : `source venv-mgr/Scripts/activate`
    - Linux : `source venv-mgr/bin/activate`

+ Run the pytest files
    - `DB_PORT=5442 python3 -m pytest`