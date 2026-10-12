-- Product catalogue: the durable source of truth for stock.
-- Redis holds only a volatile counter copied from here at warm-up time.

CREATE TABLE products
(
    id          VARCHAR(64) PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    total_stock INTEGER      NOT NULL CHECK ( total_stock >= 0 )
);

-- Demo seed data. In a real system this arrives from a catalogue service,
-- not from a schema migration.

INSERT INTO products (id, name, total_stock)
VALUES ('limited-item-01', 'Limited Edition Sneaker', 100);

-- The load test will repeatedly count orders for one item.
CREATE INDEX idx_orders_item_id ON orders (item_id);
