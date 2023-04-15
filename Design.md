## Tech Stack

+ **FastAPI** is a modern, fast (high-performance) web framework for building APIs with Python 3.7+ based on standard Python type hints. It is designed to be easy to use and compatible with the latest version of Python, while providing high performance and ease of deployment.
+ **Uvicorn** is one of the most popular ASGI server and works well with fastAPI and has a very detailed and good documentation.
+ **PostgreSQL**is a powerful, open source, object-relational database system with a strong reputation for reliability, feature robustness, and performance. PostgreSQL is fully ACID (Atomicity, Consistency, Isolation, Durability) compliant, which means that it ensures data integrity and consistency even in the face of failures or errors.
+ **Docker** is a containerization platform that makes it easier to create, deploy, and run applications by using containers. It provides portability, isolation, scalability, reproducability. Hence, we have dockerized our server.

### Component wise

+ Database: Postgresql
+ Servers: Uvicorn + FastAPI (for endpoints)
+ Client library: Python

## Key features

### Topic Partitions

Partitions divide Topics horizontally and each partition of a topic and are distributed among the available brokers. This imporves scalability and fault tolerance. If a broker goes down, the partitons that resided on that broker are redistributed among the remaining brokers. This partiton to broker mapping implemented is _consistent_ i.e no partitions residing on active brokers are redistributed.

Clients can subscribe to a single partitions of a topic or the topic as a whole. Manager picks partitons in round robin fashion when a topic is suscribed as a whole; when a group of partitions have been consumed they are not considered for the round robin partition picking.

### Broker manager

Manager acts as intermediary between clients and brokers.

Responsibilities of broker manager:

+ Store client metadata and respond to all the client requests by quering from brokers.
+ Broker cluster management:
  + Adding and deleting brokers
  + Mapping/distributing topics and partitions across available brokers.
+ Healthcheck mechanism that tracks brokers and clients which are alive and remove dead ones

### Horizontal scaling of broker

This architecture facilitates unlimited horizontal scaling of brokers. We can basically add a new broker server anytime by register with a broker manager. Each instance of broker has its own database and is standalone and independent from other instances. Each instance broker server runs multiple identical server processes and each process can handle multiple requests concurrently using multiple threads.

### Horizontal scaling of broker manager

There are multiple instances of broker managers at a time to facilitate high availability, with one instance designated as primary (or 'read/write') manager others designated as secondary (or 'read-only') managers. These designations are common knowledge among the instances. Central database is used by this managers for data consistency. Primary manager handles requests with write to brokers, i.e., adding new log message. Secondary managers handle requests which need not write anything to brokers. All manager server instances run a single multi-threaded process which can multiple requests concurrently.

Primary manager uses mutex locks to ensure order of messages in a written to partitions. Messages are written to a partition in the order of arrival. The purpose of having single read/write manager is to ensure this ordering.

### Write Ahead Logging

Write-Ahead Logging (WAL) is a standard method for ensuring data integrity. We use Postgre, which supports WAL archiving, replication and logical decoding with `wal_level=logical` configured, for storing data storage in broker manager.

### Client side Load balancing

Client library distributes the requests among the available broker manager servers in round-robin fashion. Although, the requests meant for primary broker manager are not distributed.

Advantages of client-side load balancing:

+ No more single point of failure as in the case of the traditional load balancer approach.
+ Reduced cost as the need for server-side load balancer goes away.
+ Less network latency as the client can directly invoke the backend servers removing an extra hop for the load balancer.

## Testing

### API testing

APIs of broker and broker manager are extensively tested all the endpoints cases of success and failure.

+ [broker/tests](broker/tests)
+ [broker-manager/tests](broker-manager/tests)

### Broker Replicas(Raft Implementation)

Each broker has multiple raft instances corresponding to each topic-partition replica it contains.

All the replicas communication with each other and uses raft to achieve consensus.

This gives us a fault tolerant opproach  of broker implementation.

Port Selection for raft instances - Manager request a free port from all broker and assigns the appropriate port as per the response.

<!-- ### End to End
TODO
 -->

<!-- ## Challenges

Faced some issues while trying to make some of the database transactions atomic (for persistence), had to use db.rollback() in case of any issues with the update queries. Faced some issues with dockerization.
 -->
