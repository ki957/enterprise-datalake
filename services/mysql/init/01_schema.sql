-- ShopFlow MySQL schema
-- Enables binlog-based CDC by creating tables with primary keys

CREATE DATABASE IF NOT EXISTS shopflow;
USE shopflow;

CREATE TABLE IF NOT EXISTS customers (
    customer_id   INT          NOT NULL AUTO_INCREMENT,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(255) NOT NULL UNIQUE,
    phone         VARCHAR(30),
    segment       ENUM('B2B','B2C','VIP') NOT NULL DEFAULT 'B2C',
    country       VARCHAR(100) NOT NULL DEFAULT 'US',
    city          VARCHAR(100),
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS products (
    product_id    INT          NOT NULL AUTO_INCREMENT,
    name          VARCHAR(255) NOT NULL,
    category      VARCHAR(100) NOT NULL,
    price         DECIMAL(10,2) NOT NULL,
    stock_qty     INT          NOT NULL DEFAULT 0,
    sku           VARCHAR(100) NOT NULL UNIQUE,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
    order_id      INT          NOT NULL AUTO_INCREMENT,
    customer_id   INT          NOT NULL,
    product_id    INT          NOT NULL,
    amount        DECIMAL(10,2) NOT NULL,
    quantity      INT          NOT NULL DEFAULT 1,
    status        ENUM('pending','completed','cancelled') NOT NULL DEFAULT 'pending',
    order_date    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    KEY idx_customer (customer_id),
    KEY idx_product (product_id),
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_order_product  FOREIGN KEY (product_id)  REFERENCES products(product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
