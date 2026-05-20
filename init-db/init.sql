DROP TABLE IF EXISTS dim_customer, dim_seller, dim_product, dim_store, dim_supplier, fact_sales CASCADE;

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_email VARCHAR(150),
    customer_age INT,
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(20),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100)
);

CREATE TABLE dim_seller (
    seller_id INT PRIMARY KEY,
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(150),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(20)
);

CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    product_brand VARCHAR(100),
    product_price DECIMAL(10,2),
    product_weight DECIMAL(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(20),
    product_material VARCHAR(100),
    product_rating DECIMAL(3,1),
    product_reviews INT,
    pet_category VARCHAR(50)
);

CREATE TABLE dim_store (
    store_name VARCHAR(200) PRIMARY KEY,
    store_location VARCHAR(200),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(150)
);

CREATE TABLE dim_supplier (
    supplier_name VARCHAR(200) PRIMARY KEY,
    supplier_contact VARCHAR(200),
    supplier_email VARCHAR(150),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(200),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

CREATE TABLE fact_sales (
    sale_date DATE,
    customer_id INT,
    product_id INT,
    seller_id INT,
    quantity INT,
    total_price DECIMAL(12,2)
);