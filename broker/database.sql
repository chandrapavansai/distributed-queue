-- Create Table in postgresql named Topics
CREATE TABLE IF NOT EXISTS Topic (
    -- Primary key is name of topic as it is unique
    name TEXT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Create Table in postgresql named Producer_Topic
CREATE TABLE IF NOT EXISTS Producer_Topic (
    producer_id INTEGER NOT NULL,
    topic_name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_name) REFERENCES Topic(name),
    PRIMARY KEY (producer_id, topic_name)
);

-- Create Table in postgresql named Consumer_Topic
CREATE TABLE IF NOT EXISTS Consumer_Topic (
    consumer_id INTEGER NOT NULL,
    topic_name TEXT NOT NULL,
    pos INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_name) REFERENCES Topic(name),
    PRIMARY KEY (consumer_id, topic_name)
);

-- Create Table in postgresql named Queue
-- Primary key a combination of topic_id and id
CREATE TABLE IF NOT EXISTS Queue (
    index SERIAL,
    topic_name TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_name) REFERENCES Topic(name),
    PRIMARY KEY (topic_name, index)
);

----------------------------------------------------------------------------------------------------------------------------
-- Test Data
----------------------------------------------------------------------------------------------------------------------------

-- SQL queries to fillin dummy data for testing

-- Add new topics with name
INSERT INTO topic (name) VALUES ('Topic 1');
INSERT INTO topic (name) VALUES ('Topic 2');
INSERT INTO topic (name) VALUES ('Topic 3');

-- Register producers to topics
INSERT INTO producer_topic (topic_name, producer_id) VALUES ('Topic 1', 1);
INSERT INTO producer_topic (topic_name, producer_id) VALUES ('Topic 1', 2);
INSERT INTO producer_topic (topic_name, producer_id) VALUES ('Topic 2', 1);
INSERT INTO producer_topic (topic_name, producer_id) VALUES ('Topic 2', 3);
INSERT INTO producer_topic (topic_name, producer_id) VALUES ('Topic 3', 2);
INSERT INTO producer_topic (topic_name, producer_id) VALUES ('Topic 3', 3);


-- Register consumers to topics
INSERT INTO consumer_topic (topic_name, consumer_id) VALUES ('Topic 1', 1);
INSERT INTO consumer_topic (topic_name, consumer_id) VALUES ('Topic 1', 2);
INSERT INTO consumer_topic (topic_name, consumer_id) VALUES ('Topic 2', 1);
INSERT INTO consumer_topic (topic_name, consumer_id) VALUES ('Topic 2', 3);
INSERT INTO consumer_topic (topic_name, consumer_id) VALUES ('Topic 3', 2);
INSERT INTO consumer_topic (topic_name, consumer_id) VALUES ('Topic 3', 3);

-- Add new messages in queue
INSERT INTO queue (topic_name, message) VALUES ('Topic 1', 'Message 1');
INSERT INTO queue (topic_name, message) VALUES ('Topic 1', 'Message 2');
INSERT INTO queue (topic_name, message) VALUES ('Topic 1', 'Message 3');
INSERT INTO queue (topic_name, message) VALUES ('Topic 2', 'Message 4');
INSERT INTO queue (topic_name, message) VALUES ('Topic 2', 'Message 5');
INSERT INTO queue (topic_name, message) VALUES ('Topic 2', 'Message 6');
INSERT INTO queue (topic_name, message) VALUES ('Topic 3', 'Message 7');
INSERT INTO queue (topic_name, message) VALUES ('Topic 3', 'Message 8');
INSERT INTO queue (topic_name, message) VALUES ('Topic 3', 'Message 9');




