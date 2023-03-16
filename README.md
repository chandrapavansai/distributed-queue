# Distributed Queue

This repo is home for our submissions for the monthly assignements of the course [CS60002 - Distributed Systems](https://cse.iitkgp.ac.in/~sandipc/courses/cs60002/cs60002.html)


## Files

- [/assignments](/assignments) - Problem statements for the monthly assignments
- [/boker](/broker) - Server for the broker service for the logging queue, which is dockerized
- [/boker-manager](/broker-manager) - Server for the broker manager service for the logging queue, which is dockerized

- [/sdk](/sdk) - Contains Python client library - [disqueue](/sdk/disqueue) to use the logging queue service 

## Server Setup

### With docker compose

#### Prerequisites
Docker, Docker Compose


From the root of the cloned repo run the following commands

+ Start the manager instances:
  `docker compose --file ./docker-compose-managers.yml up --build`
+ Wait till managers and start the broker instances:
  `docker compose --file ./docker-compose-brokers.yml up --build`


## Design
Checkout [Design.md](Design.md)

## Team
- 19CS10020 - Bhushan Ram Malani

- 19CS10068 - Jayanth Yindukuri

- 19CS10023 - Pavan Sai Chandra

- 19CS30038 - Ravi Sri Ram Chowdary

- 19CS30045 - Sirusolla Sri Bharath
