

-- ---------------------------------------------------------------------
CREATE TABLE territories (
    territory_id    INT PRIMARY KEY AUTO_INCREMENT,
    territory_name  VARCHAR(100) NOT NULL,
    region          VARCHAR(50)  NOT NULL,                 
    country         VARCHAR(50)  NOT NULL DEFAULT 'India',

    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      BOOLEAN  NOT NULL DEFAULT FALSE,

    INDEX idx_territories_updated_at (updated_at)
) ENGINE=InnoDB;

--    SCD Type 1 candidate. Skew driver: 20% products = 80% of sales.
-- ---------------------------------------------------------------------
CREATE TABLE products (
    product_id      INT PRIMARY KEY AUTO_INCREMENT,
    product_name    VARCHAR(200)   NOT NULL,
    category        VARCHAR(50)    NOT NULL,   
    manufacturer    VARCHAR(100)   NOT NULL,
    unit_price      DECIMAL(10,2)  NOT NULL,

    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      BOOLEAN  NOT NULL DEFAULT FALSE,

    INDEX idx_products_updated_at (updated_at),
    INDEX idx_products_category   (category)
) ENGINE=InnoDB;



-- 3. SALES_REPS  
CREATE TABLE sales_reps (
    rep_id          INT PRIMARY KEY AUTO_INCREMENT,
    rep_name        VARCHAR(150) NOT NULL,
    email           VARCHAR(150) NOT NULL UNIQUE,
    territory_id    INT          NOT NULL,
    hire_date       DATE         NOT NULL,

    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      BOOLEAN  NOT NULL DEFAULT FALSE,

    FOREIGN KEY (territory_id) REFERENCES territories(territory_id),
    INDEX idx_sales_reps_updated_at  (updated_at),
    INDEX idx_sales_reps_territory   (territory_id)
) ENGINE=InnoDB;



-- 4. CUSTOMERS 
--    *** This is your SCD Type 2 demo dim ***
CREATE TABLE customers (
    customer_id     INT PRIMARY KEY AUTO_INCREMENT,
    customer_name   VARCHAR(200) NOT NULL,
    customer_type   ENUM('Doctor', 'Hospital', 'Clinic', 'Pharmacy') NOT NULL,
    territory_id    INT          NOT NULL,
    address         VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(100),

    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      BOOLEAN  NOT NULL DEFAULT FALSE,

    FOREIGN KEY (territory_id) REFERENCES territories(territory_id),
    INDEX idx_customers_updated_at (updated_at),
    INDEX idx_customers_territory  (territory_id),
    INDEX idx_customers_type       (customer_type)
) ENGINE=InnoDB;


-- ---------------------------------------------------------------------
-- 5. SALES_TRANSACTIONS  (FACT TABLE — 5-10M rows)
CREATE TABLE sales_transactions (
    transaction_id      BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_id          INT            NOT NULL,
    customer_id         INT            NOT NULL,
    rep_id              INT            NOT NULL,
    transaction_date    DATE           NOT NULL,
    quantity            INT            NOT NULL,
    unit_price          DECIMAL(10,2)  NOT NULL,    
    total_amount        DECIMAL(12,2)  NOT NULL,   

    created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP,
    is_deleted          BOOLEAN  NOT NULL DEFAULT FALSE,

    FOREIGN KEY (product_id)  REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (rep_id)      REFERENCES sales_reps(rep_id),

    INDEX idx_sales_updated_at        (updated_at),
    INDEX idx_sales_transaction_date  (transaction_date),
    INDEX idx_sales_product           (product_id),
    INDEX idx_sales_customer          (customer_id),
    INDEX idx_sales_rep               (rep_id)
) ENGINE=InnoDB;
