
-- Customers View

-- CREATE VIEW customers_view AS
-- SELECT c.Customer_ID as customer_id, sum(Transaction_Summ) / COUNT(Transaction_Summ) as customer_average_check,
-- FROM cards c
-- join transaction t on c.Customer_Card_ID

CREATE OR REPLACE VIEW customer_view (
    Customer_ID,
    Customer_Average_Check,
    Customer_Average_Check_Segment,
    Customer_Frequency,
    Customer_Frequency_Segment,
    Customer_Inactive_Period,
    Customer_Churn_Rate,
    Customer_Churn_Segment,
    Customer_Segment,
    Customer_Primary_Store
) AS
WITH temp_table AS (SELECT *
                    FROM cards
                    JOIN transactions ON cards.Customer_Card_ID = transactions.Customer_Card_ID
                    WHERE Transaction_DateTime < (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation)),




-- Groups View
CREATE MATERIALIZED VIEW IF NOT EXISTS v_history AS
SELECT Customer_ID AS "Customer_ID",
       Transaction_ID AS "Transaction_ID",
       Transaction_DateTime AS "Transaction_DateTime",
       Group_ID AS "Group_ID",
       SUM(sku_purchase_price * sku_amount) AS "Group_Cost",
       SUM(SKU_Sum) AS "Group_Summ",
       SUM(sku_summ_paid) AS "Group_Summ_Paid"
FROM customer_view
GROUP BY Customer_ID, Transaction_ID, Transaction_DateTime, Group_ID;




-- Periods View
-- CREATE MATERIALIZED VIEW IF NOT EXISTS v_periods AS
-- SELECT Customer_ID                          AS "Customer_ID",
--        Group_Id                             AS "Group_ID",
--        min(transaction_datetime)            AS "First_Group_Purchase_Date",
--        max(transaction_datetime)            AS "Last_Group_Purchase_Date",
--        count(DISTINCT transaction_id)       AS "Group_Purchase",
--        (extract(EPOCH FROM max(transaction_datetime) - min(transaction_datetime))::float / 86400.0 + 1)
--            / count(DISTINCT transaction_id) AS "Group_Frequency",
--        min(sku_discount / sku_summ)         AS "Group_Min_Discount"
-- FROM customer_view
-- GROUP BY customer_id, group_id;






-- Purchase History View




