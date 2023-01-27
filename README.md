# Distributed Queue

### Assignment 1 : Implementing a Distributed Queue

Deadline : 31st Jan

#### To connect to database

* Install PostgresSQL
* Connect to server
* Import database

###### To dump

`pg_dump -U USERNAME DATABASE_NAME > dbexport.pgsql`

###### To Import

`psql DB_NAME < INPUT_FILE`

### Docker Container

#### Start Container

`docker-compose up`

#### Remove Container

`docker-compose down`

#### Postgres Admin Panel

http://localhost:5050/

Email: admin@email.com

Password: admin

#### Steps

* Login to admin panel
* Create new server
  * username: postgres
  * password: postgres
* Goto Query Tools
  * Paste database.sql and run to create database with required tables

##### Docker Helper commands

###### Build image

`docker build -t ds-assgn-1-image .`

###### List all Images

`docker ps`

###### Stop Container

`docker stop ds-assgn-1-container`

###### Remove container

`docker rm ds-assgn-1-container`

###### To create container with volumes

`docker run --name ds-assgn-1-container -p 80:80 -d -v $(pwd):/code ds-assgn-1-image`
