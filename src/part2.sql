DROP TABLE IF EXISTS Segments CASCADE;
CREATE TABLE Segments
(
    Segment            bigint,
    Average_Check      varchar(255) NOT NULL,
    Purchase_Frequency varchar(255) NOT NULL,
    Churn_Probability  varchar(255) NOT NULL
);

-- Customers View
CREATE MATERIALIZED VIEW IF NOT EXISTS Customer_View AS
WITH maininfo AS (
    SELECT PD.Customer_ID AS "CI",
           TR.Transaction_DateTime AS "TD",
           TR.Transaction_Store_ID AS "TSI",
           AVG(TR.Transaction_Summ) OVER w_pci AS "ATS",
           ROW_NUMBER() OVER w_pci_otd_d AS RN,
           COUNT(*) OVER w_pcitsi AS CNT
    FROM Personal_Data AS PD
    JOIN Cards AS CR ON PD.Customer_ID = CR.Customer_ID
    JOIN Transactions AS TR ON TR.Customer_Card_ID = CR.Customer_Card_ID
    WINDOW w_pci AS (PARTITION BY PD.Customer_ID ORDER BY TR.Transaction_DateTime DESC),
           w_pcitsi AS (PARTITION BY PD.Customer_ID, TR.Transaction_Store_ID),
           w_pci_otd_d AS (PARTITION BY PD.Customer_ID ORDER BY TR.Transaction_DateTime DESC)
),
cte2 AS (
    SELECT DISTINCT "CI",
           first_value("TSI") OVER (PARTITION BY "CI" ORDER BY CNT DESC, "TD" DESC) AS SHOP,
           first_value("TSI") OVER (PARTITION BY "CI" ORDER BY RN) AS LAST_SHOP FROM maininfo
),
cte3 AS (
    SELECT "CI", COUNT(DISTINCT "TSI") last_3_cnt
    FROM maininfo WHERE RN <= 3 GROUP BY "CI"
)

SELECT "Customer_ID",
       "Customer_Average_Check",
       "Customer_Average_Check_Segment",
       "Customer_Frequency",
       "Customer_Frequency_Segment",
       "Customer_Inactive_Period",
       "Customer_Churn_Rate",
       "Customer_Churn_Segment",
       Segment AS "Segment",
       CASE
           WHEN last_3_cnt = 1 THEN LAST_SHOP
           ELSE SHOP
           END AS Customer_Primary_Store
FROM (SELECT "Customer_ID",
             "Customer_Average_Check",
             CASE
                 WHEN (PERCENT_RANK() OVER w_ocac_d < 0.1) THEN 'High'
                 WHEN (PERCENT_RANK() OVER w_ocac_d < 0.25) THEN 'Medium'
                 ELSE 'Low'
                 END AS "Customer_Average_Check_Segment",
             "Customer_Frequency",
             CASE
                 WHEN (PERCENT_RANK() OVER w_ocf < 0.1) THEN 'Often'
                 WHEN (PERCENT_RANK() OVER w_ocf < 0.35) THEN 'Occasionally'
                 ELSE 'Rarely'
                 END AS "Customer_Frequency_Segment",
             "Customer_Inactive_Period",
             ("Customer_Inactive_Period"/"Customer_Frequency") AS "Customer_Churn_Rate",
             CASE
                 WHEN ("Customer_Inactive_Period"/"Customer_Frequency" < 2) THEN 'Low'
                 WHEN ("Customer_Inactive_Period"/"Customer_Frequency" < 5) THEN 'Medium'
                 ELSE 'High'
                 END AS "Customer_Churn_Segment"

      FROM (SELECT "CI" AS "Customer_ID",
                   "ATS" AS "Customer_Average_Check",
                   EXTRACT(EPOCH from MAX("TD") - MIN("TD"))::float / 86400.0 /
                   COUNT("CI") AS "Customer_Frequency",
                   EXTRACT(EPOCH from (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) - MAX("TD"))/86400.0 AS "Customer_Inactive_Period"
            FROM maininfo GROUP BY "CI", "ATS"
                WINDOW w_oats_d AS (ORDER BY sum("ATS") DESC)) AS avmain
      GROUP BY "Customer_ID",
               "Customer_Average_Check",
               "Customer_Frequency",
               "Customer_Inactive_Period"
          WINDOW w_ocac_d AS (ORDER BY sum("Customer_Average_Check") DESC), w_ocf AS (ORDER BY "Customer_Frequency")) AS biginfo
          JOIN Segments AS S ON (S.Average_Check = "Customer_Average_Check_Segment" AND
                                S.Purchase_Frequency = "Customer_Frequency_Segment" AND
                                S.Churn_Probability = "Customer_Churn_Segment")
         JOIN cte2 ON cte2."CI" = biginfo."Customer_ID"
         JOIN cte3 ON cte3."CI" = biginfo."Customer_ID";

-- Purchase history View
CREATE MATERIALIZED VIEW IF NOT EXISTS Purchase_history_View AS
SELECT CR.Customer_ID AS "Customer_ID",
       TR.Transaction_ID AS "Transaction_ID",
       TR.Transaction_DateTime AS "Transaction_DateTime",
       TR.Transaction_Store_ID AS "Transaction_Store_ID",
       SKU.Group_ID AS "Group_ID",
       CK.SKU_Amount AS "SKU_Amount",
       SR.SKU_ID AS "SKU_ID",
       SR.SKU_Retail_Price AS "SKU_Retail_Price",
       SR.SKU_Purchase_Price AS "SKU_Purchase_Price",
       CK.SKU_Summ_Paid AS "SKU_Summ_Paid",
       CK.SKU_Summ AS "SKU_Summ",
       CK.SKU_Discount AS "SKU_Discount"
FROM Transactions AS TR
JOIN Cards AS CR ON CR.Customer_Card_ID = TR.Customer_Card_ID
JOIN Personal_data AS PD ON PD.Customer_ID = CR.Customer_ID
JOIN Checks AS CK ON TR.Transaction_ID = CK.Transaction_ID
JOIN SKU AS SKU ON SKU.SKU_ID = CK.SKU_ID
JOIN Stores AS SR ON SKU.SKU_ID = SR.SKU_ID
AND TR.Transaction_Store_ID = SR.Transaction_Store_ID;

-- Periods View
CREATE VIEW Periods_View (
        Customer_ID AS "Customer_ID",
        Group_ID AS "Group_ID",
        First_Group_Purchase_Date AS "First_Group_Purchase_Date",
        Last_Group_Purchase_Date AS "Last_Group_Purchase_Date",
        Group_Purchase AS "Group_Purchase",
        Group_Frequency AS "Group_Frequency",
        Group_Min_Discount AS "Group_Min_Discount"
        ) AS
SELECT Customer_ID,
       Group_ID,
       MIN(Transaction_DateTime)::timestamp First_Date,
       MAX(Transaction_DateTime)::timestamp Last_Date,
       COUNT(*)                  Group_Purchase,
       (((TO_CHAR((MAX(Transaction_DateTime)::timestamp - MIN(Transaction_DateTime)::timestamp), 'DD'))::int + 1)*1.0) / COUNT(*)*1.0 as Group_Frequency,
       COALESCE((SELECT MIN(SKU_Discount / SKU_Summ) FROM Checks c1
       JOIN purchase_history_view ph2 ON ph2.Transaction_ID = c1.Transaction_ID
       WHERE (SKU_Discount / SKU_Summ) > 0 AND ph2.Customer_ID = t1.Customer_ID
       AND ph2.Group_ID = t1.Group_ID), 0) AS Group_Minimum_Discount
  FROM (SELECT DISTINCT Customer_ID, t.Transaction_DateTime, c.SKU_Discount, SKU.group_id, c.SKU_Summ
          FROM Cards
                JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                 JOIN checks c ON t.transaction_id = c.transaction_id
                  JOIN SKU ON SKU.SKU_ID = c.sku_id) AS t1
 GROUP BY Group_ID, customer_id;

-- Groups View
CREATE MATERIALIZED VIEW IF NOT EXISTS v_history AS
SELECT Customer_ID AS "Customer_ID",
       Transaction_ID AS "Transaction_ID",
       Transaction_DateTime AS "Transaction_DateTime",
       Group_ID AS "Group_ID",
       SUM(SKU_Purchase_Price * SKU_Amount) AS "Group_Cost",
       SUM(SKU_Summ) AS "Group_Summ",
       SUM(SKU_Summ_Paid) AS "Group_Summ_Paid"
FROM support
GROUP BY Customer_ID, Transaction_ID, Transaction_DateTime, Group_ID;
