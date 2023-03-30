DROP VIEW IF EXISTS Periods_View CASCADE;

CREATE OR REPLACE VIEW Periods_View (customer_id, Group_ID, First_Group_Purchase_Date,
    Last_Group_Purchase_Date, Group_Purchase, Group_Frequency, Group_Min_Discount) AS
    WITH get_params AS (SELECT customer_id, group_id, MIN(transaction_datetime) AS First_Group_Purchase_Date,
                               MAX(transaction_datetime) AS Last_Group_Purchase_Date, COUNT(*) AS Group_Purchase
                        FROM purchase_history_view
                        WHERE transaction_datetime < (SELECT analysis_formation FROM date_of_analysis_formation)
                        GROUP BY customer_id, group_id),
         get_checks_params AS (SELECT customer_id, transaction_datetime, group_id, sku_discount, sku_summ
                               FROM cards
                               JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                               JOIN checks ON transactions.transaction_id = checks.transaction_id
                               JOIN SKU ON checks.sku_id = SKU.sku_id)
    SELECT DISTINCT get_params.customer_id, get_params.group_id, First_Group_Purchase_Date, Last_Group_Purchase_Date,
           Group_Purchase,
           ((TO_CHAR((Last_Group_Purchase_Date - First_Group_Purchase_Date), 'DD')::real + 1) /
            Group_Purchase) AS Group_Frequency,
                    COALESCE(MIN(sku_discount / sku_summ) OVER (PARTITION BY get_params.customer_id,
                get_params.group_id), 0) AS Group_Min_Discount
    FROM get_params
    JOIN get_checks_params ON get_params.customer_id = get_checks_params.customer_id AND
                              get_params.group_id = get_checks_params.group_id;

SELECT * FROM Periods_View;