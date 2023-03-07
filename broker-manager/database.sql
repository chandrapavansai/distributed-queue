-- Create Table in postgresql named Broker
CREATE TABLE IF NOT EXISTS Broker (
    broker_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    last_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (broker_id)   
);

-- Create Table in postgresql named Topic
CREATE TABLE IF NOT EXISTS Topic (
    -- Primary key is name of topic as it is unique
    topic_name TEXT NOT NULL,
    partition_id INTEGER NOT NULL DEFAULT 0,
    broker_id INTEGER,
    size INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (broker_id) REFERENCES Broker(broker_id),
    PRIMARY KEY (topic_name, partition_id)
);


-- Create Table in postgresql named Consumer
CREATE TABLE IF NOT EXISTS Consumer (
    consumer_id CHAR(36) NOT NULL,
    topic_name TEXT NOT NULL,
    is_round_robin BOOLEAN NOT NULL DEFAULT FALSE,
    partition_id INTEGER NOT NULL DEFAULT 0,
    last_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_name, partition_id) REFERENCES Topic(topic_name, partition_id),
    PRIMARY KEY (consumer_id)
);

-- Create Table in postgresql named ConsumerPartition
CREATE TABLE IF NOT EXISTS ConsumerPartition (
    consumer_id CHAR(36) NOT NULL,
    partition_id INTEGER NOT NULL DEFAULT 0,
    offset_val INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (consumer_id) REFERENCES Consumer(consumer_id),
    PRIMARY KEY (consumer_id, partition_id)
);

-- Create Table in postgresql named Producer
CREATE TABLE IF NOT EXISTS Producer (
    producer_id CHAR(36) NOT NULL,
    topic_name TEXT NOT NULL,
    is_round_robin BOOLEAN NOT NULL DEFAULT FALSE,
    partition_id INTEGER NOT NULL DEFAULT 0,
    last_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_name, partition_id) REFERENCES Topic(topic_name, partition_id),
    PRIMARY KEY (producer_id)
);

-- Create Table in postgresql named Manager
CREATE TABLE IF NOT EXISTS Manager (
    url TEXT NOT NULL,
    is_leader BOOLEAN NOT NULL DEFAULT FALSE,
    last_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (url)
);