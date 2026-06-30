CREATE TABLE orders
(
    id             BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL UNIQUE, -- Prevents processing duplicate Kafka messages
    user_id        VARCHAR(64) NOT NULL,
    item_id        VARCHAR(64) NOT NULL,
    status         VARCHAR(32) NOT NULL,
    created_at     TIMESTAMP   NOT NULL
);