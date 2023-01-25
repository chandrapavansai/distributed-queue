-- Create Table in postgresql named Topics
CREATE TABLE IF NOT EXISTS Topic (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create Table in postgresql named Producer
CREATE TABLE IF NOT EXISTS Producer (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create Table in postgresql named Consumer
CREATE TABLE IF NOT EXISTS Consumer (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create Table in postgresql named Producer_Topic
CREATE TABLE IF NOT EXISTS Producer_Topic (
    producer_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (producer_id) REFERENCES Producer(id),
    FOREIGN KEY (topic_id) REFERENCES Topic(id),
    PRIMARY KEY (producer_id, topic_id)
);

-- Create Table in postgresql named Consumer_Topic
CREATE TABLE IF NOT EXISTS Consumer_Topic (
    consumer_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    pos INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (consumer_id) REFERENCES Consumer(id),
    FOREIGN KEY (topic_id) REFERENCES Topic(id),
    PRIMARY KEY (consumer_id, topic_id)
);

-- Create Table in postgresql named Queue
-- Primary key a combination of topic_id and id
CREATE TABLE IF NOT EXISTS Queue (
    index SERIAL,
    topic_id INTEGER NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic_id) REFERENCES Topic(id),
    PRIMARY KEY (topic_id, index)
);