--#1 Personal information Table
DROP TABLE IF EXISTS customers CASCADE;
CREATE TABLE customers (
  Customer_ID SERIAL NOT NULL PRIMARY KEY,
  Customer_Name VARCHAR(255) NOT NULL
      CHECK(Customer_Name ~ '^([А-Я]{1}[а-яё\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
  Customer_Surname VARCHAR(255) NOT NULL
      CHECK(Customer_Surname ~ '^([А-Я]{1}[а-яё\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
  Customer_Primary_Email VARCHAR(255) NOT NULL CHECK(Customer_Primary_Email ~ '^\w+([-.'''']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$')
,
  Customer_Primary_Phone VARCHAR(20) NOT NULL CHECK (Customer_Primary_Phone ~ '^((\+7)+([0-9]){10})$')
);

--#2 Cards Table
DROP TABLE IF EXISTS cards CASCADE;
CREATE TABLE cards (
    Customer_Card_ID BIGINT NOT NULL PRIMARY KEY,
    Customer_ID BIGINT NOT NULL,
    FOREIGN KEY (Customer_ID) REFERENCES personal_information (Customer_ID)
);
COMMENT ON COLUMN cards.Customer_ID IS 'One customer can own several cards';

--#3 Transactions Table
DROP TABLE IF EXISTS transaction CASCADE;
CREATE TABLE transaction (
    Transaction_ID SERIAL PRIMARY KEY,
    Customer_Card_ID BIGINT NOT NULL,
    CONSTRAINT fk_card_id FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID),
    Transaction_Summ NUMERIC(10,2) NOT NULL,
    Transaction_DateTime TIMESTAMP,
    Transaction_Store_ID VARCHAR NOT NULL
);
COMMENT ON COLUMN transaction.Transaction_ID IS 'Unique value';
COMMENT ON COLUMN transaction.Transaction_Summ IS 'Transaction sum in rubles(full purchase price excluding discounts)';
COMMENT ON COLUMN transaction.Transaction_DateTime IS 'Date and time when the transaction was made';
COMMENT ON COLUMN transaction.Transaction_Store_ID IS 'The store where the transaction was made';

--#7 SKU group Table
DROP TABLE IF EXISTS group_SKU CASCADE;
CREATE TABLE group_SKU (
    Group_ID BIGINT NOT NULL PRIMARY KEY,
    Group_Name VARCHAR NOT NULL CHECK (Group_Name ~ '^[A-zА-я0-9_\/-]+$')
);

--#5 Product grid Table
DROP TABLE IF EXISTS product_grid CASCADE;
CREATE TABLE product_grid (
    SKU_ID BIGINT NOT NULL PRIMARY KEY,
    SKU_Name varchar NOT NULL
        CHECK (SKU_Name ~ '^[A-Za-zА-Яа-яЁё0-9_@!#%&()+-=*\s\[\]{};:''"\\|,.<>?/`~^$]*$'),
    Group_ID BIGINT NOT NULL,
    FOREIGN KEY (Group_ID) REFERENCES group_SKU (Group_ID)
);
COMMENT ON COLUMN product_grid.Group_ID IS 'The ID of the group of related products to which the product belongs (for example, same type of yogurt of the same manufacturer and volume, but different flavors). One identifier is specified for all products in the group';

--#4 Checks
DROP TABLE IF EXISTS checks;
CREATE TABLE checks (
    Transaction_ID BIGINT NOT NULL PRIMARY KEY,
    SKU_ID BIGINT NOT NULL,
    SKU_Amount REAL NOT NULL,
    SKU_Summ REAL NOT NULL,
    SKU_Summ_Paid REAL NOT NULL,
    SKU_Discount REAL NOT NULL,
    FOREIGN KEY (Transaction_ID) REFERENCES transaction (Transaction_ID),
    FOREIGN KEY (SKU_ID) REFERENCES product_grid (SKU_ID)
);
COMMENT ON COLUMN checks.Transaction_ID IS 'Transaction ID is specified for all products in the check';
COMMENT ON COLUMN checks.SKU_Amount IS 'The quantity of the purchased product';
COMMENT ON COLUMN checks.SKU_Summ IS 'The purchase amount of the actual volume of this product in rubles (full price without discounts and bonuses)';
COMMENT ON COLUMN checks.SKU_Summ_Paid IS 'The amount actually paid for the product not including the discount';
COMMENT ON COLUMN checks.SKU_Discount IS 'The size of the discount granted for the product in rubles';

--#6 Stores Table
DROP TABLE IF EXISTS retail_outlets CASCADE;
CREATE TABLE retail_outlets (
    Transaction_Store_ID BIGINT NOT NULL PRIMARY KEY,
    SKU_ID BIGINT NOT NULL,
    SKU_Purchase_Price NUMERIC NOT NULL,
    CONSTRAINT fk_sku_id FOREIGN KEY (SKU_ID) REFERENCES product_grid (SKU_ID),
    SKU_Retail_Price NUMERIC
);
COMMENT ON COLUMN retail_outlets.SKU_Purchase_Price IS 'Purchasing price of products for this store';
COMMENT ON COLUMN retail_outlets.SKU_Retail_Price IS 'The sale price of the product excluding discounts for this store';

--#8 Date of analysis formation Table
DROP TABLE IF EXISTS date_of_analysis_formation;
CREATE TABLE date_of_analysis_formation (
    Analysis_Formation TIMESTAMP(0)
);

--Procedure for import
DROP PROCEDURE IF EXISTS import();
CREATE OR REPLACE PROCEDURE import(table_name varchar, path text, sep char DEFAULT '\t')
    LANGUAGE plpgsql AS $$
    BEGIN
        IF (sep = '\t') THEN
            EXECUTE concat('COPY ', table_name, ' FROM ''', path, ''' DELIMITER E', '\t', ' CSV HEADER;');
        ELSE
            EXECUTE concat('COPY ', table_name, ' FROM ''', path, ''' DELIMITER E''', sep, ''' CSV HEADER;');
        END IF;
    END;$$;

--Procedure for export
DROP PROCEDURE IF EXISTS export();
CREATE OR REPLACE PROCEDURE export(table_name varchar, path text, sep char DEFAULT '\t')
    LANGUAGE plpgsql AS $$
    BEGIN
        IF (sep = '\t') THEN
            EXECUTE concat('COPY ', table_name, ' TO ''', path, ''' DELIMITER E''\t''', ' CSV HEADER;');
        ELSE
            EXECUTE concat('COPY ', table_name, ' TO ''', path, ''' DELIMITER E''', sep, ''' CSV HEADER;');
        END IF;
    END;$$;

-- Нужно заинсертить по 5 записей в каждую из таблиц, завтра, после того как напишу процедуру, экспортну в .csv и tsv.
 INSERT INTO customers VALUES (1, 'John', 'Smith', 'smith@123.us', '+79991117744');
 INSERT INTO customers VALUES (2, 'Myre', 'Bean', 'myrebean@vk.com', '+79991115544');
 INSERT INTO customers VALUES (3, 'Blackwood', 'Martain', 'blackwoo@ya.ru', '+79142227744');
 INSERT INTO customers VALUES (4, 'Tamela', 'Bears', 'tamelabe@rambler.ru', '+79832221100');
 INSERT INTO customers VALUES (5, 'Aboba', 'Abobovich', 'abobaabo@icq.ru', '+79122227744');

-- INSERT INTO date_of_analysis_formation
-- VALUES ('11-11-2011');
-- SELECT * FROM date_of_analysis_formation
