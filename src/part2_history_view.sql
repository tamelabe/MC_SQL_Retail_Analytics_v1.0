DROP VIEW IF EXISTS Purchase_history_view CASCADE;

CREATE OR REPLACE VIEW Purchase_history_view AS
WITH purchases AS (SELECT customer_id, transactions.transaction_id, transaction_datetime, group_id,
                          sku_purchase_price, sku_amount, sku_summ, sku_summ_paid
                   FROM cards
                   JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                   JOIN checks ON transactions.transaction_id = checks.transaction_id
                   JOIN SKU ON SKU.sku_id = checks.sku_id
                   JOIN stores ON SKU.sku_id = stores.sku_id AND
                                  transactions.transaction_store_id = stores.transaction_store_id
                   WHERE transaction_datetime < (SELECT analysis_formation FROM date_of_analysis_formation))
SELECT DISTINCT customer_id, transaction_id, transaction_datetime, group_id,
       SUM(sku_purchase_price * sku_amount)
           OVER (PARTITION BY customer_id, group_id, transaction_id, transaction_datetime) AS Group_Cost,
       SUM(sku_summ)
           OVER (PARTITION BY customer_id, group_id, transaction_id, transaction_datetime) AS Group_Summ,
       SUM(sku_summ_paid)
           OVER (PARTITION BY customer_id, group_id, transaction_id, transaction_datetime) AS Group_Summ_Paid
FROM purchases;

SELECT * FROM Purchase_history_view;