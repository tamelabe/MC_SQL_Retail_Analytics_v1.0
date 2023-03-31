--#1 Personal information Table
DROP TABLE IF EXISTS Personal_Data CASCADE;
CREATE TABLE Personal_Data (
  Customer_ID SERIAL NOT NULL PRIMARY KEY,
  Customer_Name VARCHAR(255) NOT NULL
      CHECK(Customer_Name ~ '^([А-Я]{1}[а-яё\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
  Customer_Surname VARCHAR(255) NOT NULL
      CHECK(Customer_Surname ~ '^([А-Я]{1}[а-яё\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
  Customer_Primary_Email VARCHAR(255) NOT NULL CHECK(Customer_Primary_Email ~ '^\w+([-.'''']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$')
,
  Customer_Primary_Phone VARCHAR(20) NOT NULL CHECK (Customer_Primary_Phone ~ '^((\+7)+([0-9]){10})$')
);

-- INSERT INTO Personal_Data(Customer_ID, Customer_Name, Customer_Surname,Customer_Primary_Email, Customer_Primary_Phone)
-- VALUES (1, "ALINA", "S", "sultanova.6@mail.ru", "+71234567890");

-- INSERT INTO Personal_Data(Customer_ID, Customer_Name, Customer_Surname, Customer_Primary_Email, Customer_Primary_Phone)
-- VALUES (1, 'Alina', 'Su', 'agdf@t.ru', '+71234567890');
-- SELECT * FROM Personal_Data ORDER BY 1;


--#2 Cards Table
DROP TABLE IF EXISTS Cards CASCADE;
CREATE TABLE Cards (
    Customer_Card_ID BIGINT NOT NULL PRIMARY KEY,
    Customer_ID BIGINT NOT NULL,
    FOREIGN KEY (Customer_ID) REFERENCES Personal_Data (Customer_ID)
);
COMMENT ON COLUMN Cards.Customer_ID IS 'One customer can own several cards';

-- INSERT INTO Cards(Customer_Card_ID, Customer_ID)
-- VALUES (1, 1);
-- SELECT * FROM Cards ORDER BY 1;

--#3 Transactions Table
DROP TABLE IF EXISTS Transactions CASCADE;
CREATE TABLE Transactions (
    Transaction_ID BIGINT PRIMARY KEY,
    Customer_Card_ID BIGINT NOT NULL,
    Transaction_Summ NUMERIC(10,2) NOT NULL,
    Transaction_DateTime TIMESTAMP(0)  WITHOUT TIME ZONE,
    Transaction_Store_ID BIGINT NOT NULL,
    FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID)
);
COMMENT ON COLUMN Transactions.Transaction_ID IS 'Unique value';
COMMENT ON COLUMN Transactions.Transaction_Summ IS 'Transaction sum in rubles(full purchase price excluding discounts)';
COMMENT ON COLUMN Transactions.Transaction_DateTime IS 'Date and time when the transaction was made';
COMMENT ON COLUMN Transactions.Transaction_Store_ID IS 'The store where the transaction was made';

--#7 SKU group Table
DROP TABLE IF EXISTS Groups_SKU CASCADE;
CREATE TABLE Groups_SKU (
    Group_ID BIGINT NOT NULL PRIMARY KEY,
    Group_Name VARCHAR NOT NULL CHECK (Group_Name ~ '^[A-zА-я0-9_\/\s-]+$')
);

--#5 Product grid Table
DROP TABLE IF EXISTS SKU CASCADE;
CREATE TABLE SKU (
    SKU_ID BIGINT NOT NULL PRIMARY KEY,
    SKU_Name varchar NOT NULL
        CHECK (SKU_Name ~ '^[A-Za-zА-Яа-яЁё0-9_@!#%&()+-=*\s\[\]{};:''"\\|,.<>?/`~^$]*$'),
    Group_ID BIGINT NOT NULL,
    FOREIGN KEY (Group_ID) REFERENCES Groups_SKU (Group_ID)
);
COMMENT ON COLUMN SKU.Group_ID IS 'The ID of the group of related products to which the product belongs (for example, same type of yogurt of the same manufacturer and volume, but different flavors). One identifier is specified for all products in the group';

--#4 Checks
DROP TABLE IF EXISTS Checks;
CREATE TABLE Checks (
    Transaction_ID BIGINT NOT NULL,
    SKU_ID BIGINT NOT NULL,
    SKU_Amount REAL NOT NULL,
    SKU_Summ REAL NOT NULL,
    SKU_Summ_Paid REAL NOT NULL,
    SKU_Discount REAL NOT NULL,
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions (Transaction_ID),
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);
COMMENT ON COLUMN Checks.Transaction_ID IS 'Transaction ID is specified for all products in the check';
COMMENT ON COLUMN Checks.SKU_Amount IS 'The quantity of the purchased product';
COMMENT ON COLUMN Checks.SKU_Summ IS 'The purchase amount of the actual volume of this product in rubles (full price without discounts and bonuses)';
COMMENT ON COLUMN Checks.SKU_Summ_Paid IS 'The amount actually paid for the product not including the discount';
COMMENT ON COLUMN Checks.SKU_Discount IS 'The size of the discount granted for the product in rubles';

--#6 Stores Table
DROP TABLE IF EXISTS Stores CASCADE;
CREATE TABLE Stores (
    Transaction_Store_ID BIGINT NOT NULL,
    SKU_ID BIGINT NOT NULL,
    SKU_Purchase_Price NUMERIC NOT NULL,
    CONSTRAINT fk_sku_id FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID),
    SKU_Retail_Price NUMERIC
);
COMMENT ON COLUMN Stores.SKU_Purchase_Price IS 'Purchasing price of products for this store';
COMMENT ON COLUMN Stores.SKU_Retail_Price IS 'The sale price of the product excluding discounts for this store';

--#8 Date of analysis formation Table
DROP TABLE IF EXISTS Date_Of_Analysis_Formation;
CREATE TABLE Date_Of_Analysis_Formation (
    Analysis_Formation TIMESTAMP(0)
);

--Procedure for import
CREATE OR REPLACE PROCEDURE import(table_name varchar, path text, sep char DEFAULT '\t')
    LANGUAGE plpgsql AS $$
    BEGIN
        IF (sep = '\t') THEN
            EXECUTE concat('COPY ', table_name, ' FROM ''', path, ''' DELIMITER E''\t''', ' CSV;');
        ELSE
            EXECUTE concat('COPY ', table_name, ' FROM ''', path, ''' DELIMITER ''', sep, ''' CSV;');
        END IF;
    END;$$;

--Procedure for export
CREATE OR REPLACE PROCEDURE export(table_name varchar, path text, sep char DEFAULT '\t')
    LANGUAGE plpgsql AS $$
    BEGIN
        IF (sep = '\t') THEN
            EXECUTE concat('COPY ', table_name, ' TO ''', path, ''' DELIMITER E''\t''', ' CSV;');
        ELSE
            EXECUTE concat('COPY ', table_name, ' TO ''', path, ''' DELIMITER ''', sep, ''' CSV;');
        END IF;
    END;$$;

-- Data import from datasets
SET DATESTYLE to iso, DMY;
SET imp_path.txt TO '/Users/myrebean/SQL3_RetailAnalitycs_v1.0-2/datasets/';
CALL import('Personal_Data', (current_setting('imp_path.txt') || 'Personal_Data.tsv'));
CALL import('Cards', (current_setting('imp_path.txt') || 'Cards.tsv'));
CALL import('Transactions', (current_setting('imp_path.txt') || 'Transactions.tsv'));
CALL import('Groups_SKU', (current_setting('imp_path.txt') || 'Groups_SKU.tsv'));
CALL import('SKU', (current_setting('imp_path.txt') || 'SKU.tsv'));
CALL import('Checks', (current_setting('imp_path.txt') || 'Checks.tsv'));
CALL import('Date_Of_Analysis_Formation', (current_setting('imp_path.txt') || 'Date_Of_Analysis_Formation.tsv'));
CALL import('Stores', (current_setting('imp_path.txt') || 'Stores.tsv'));

--  Data export to specified path
SET DATESTYLE to iso, DMY;
SET exp_path.txt TO '/Users/myrebean/SQL3_RetailAnalitycs_v1.0-2/src/tables_data/';
CALL export('Personal_Data', (current_setting('exp_path.txt') || 'Personal_Data.csv'), ',');
CALL export('Cards', (current_setting('exp_path.txt') || 'Cards.csv'), ',');
CALL export('Transactions', (current_setting('exp_path.txt') || 'Transactions.csv'), ',');
CALL export('Groups_SKU', (current_setting('exp_path.txt') || 'Groups_SKU.csv'), ',');
CALL export('SKU', (current_setting('exp_path.txt') || 'SKU.csv'), ',');
CALL export('Checks', (current_setting('exp_path.txt') || 'Checks.csv'), ',');
CALL export('Date_Of_Analysis_Formation', (current_setting('exp_path.txt') || 'Date_Of_Analysis_Formation.csv'), ',');
CALL export('Stores', (current_setting('exp_path.txt') || 'Stores.csv'), ',');