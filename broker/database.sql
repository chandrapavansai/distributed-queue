-- Create Table in postgresql named Topics
CREATE TABLE IF NOT EXISTS Topic (
    -- Primary key is name of topic as it is unique
    name TEXT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Create Table in postgresql named Producer_Topic
CREATE TABLE IF NOT EXISTS Producer_Topic (
    producer_id CHAR(36) NOT NULL,
    topic_name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_name) REFERENCES Topic(name),
    PRIMARY KEY (producer_id, topic_name)
);

-- Create Table in postgresql named Consumer_Topic
CREATE TABLE IF NOT EXISTS Consumer_Topic (
    consumer_id CHAR(36) NOT NULL,
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



