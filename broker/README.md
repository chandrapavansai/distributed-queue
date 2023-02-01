# Broker server for the Logging Queue service

## Setup

### Environment
- **DB_NAME** - database name in postgres
- **DB_USER** - postgres username which has access to the database 
- **DB_PASSWORD** - password to the username
- **DB_HOST** - host name of the PostgreSQL server
- **DB_PORT** - port of the PostgreSQL server

### With docker
- Build the docker image
  ```sh
   docker build -t broker-app .
   ```
- Run the built image
  ```sh
  docker run -dp 80:80 broker-app --env DB_NAME=brokerdb ...
  ```
  `...` signifies other environment variables. Checkout [Docker run documentation](https://docs.docker.com/engine/reference/commandline/run/#-set-environment-variables--e---env---env-file) for more on setting environment variables
  
  
### Without docker

- Install the pip dependencies from requiements.txt
- Run the following command to start the server on port 80 (Different port can be used too)
  ```sh
  uvicorn main:app --host 0.0.0.0 --port 80 --reload
  ```

### Database Setup
- Install [PostgreSQL](https://www.postgresql.org/download/)
- Make sure PostgreSQL is up and running
- Open postgres prompt and create a database and a user to access the database
  ```sh
  $ sudo -u postgres psql
  postgres=# create database mydb;
  postgres=# create user myuser with encrypted password 'mypass';
  postgres=# grant all privileges on database mydb to myuser;
  ```
  
## Testing
+ Create seperate databse for testing
+ Run the tests with correct environment variables
```sh
python3 -m pytest test_main.py
```
