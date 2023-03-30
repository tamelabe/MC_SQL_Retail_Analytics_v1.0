CREATE TABLE IF NOT EXISTS Segments
(
    Segment            BIGINT PRIMARY KEY,
    Average_Check      VARCHAR(255) NOT NULL,
    Purchase_Frequency VARCHAR(255) NOT NULL,
    Churn_Probability  VARCHAR(255) NOT NULL
);

-- CALL import('Segments', '/Users/myrebean/SQL3_RetailAnalitycs_v1.0-2/datasets/Segments.csv', ',');

COPY Segments (Segment, Average_Check, Purchase_Frequency, Churn_Probability)
    FROM '/Users/myrebean/SQL3_RetailAnalitycs_v1.0-2/datasets/Segments.csv' DELIMITER ',' CSV HEADER;

-- Customers View



-- CREATE MATERIALIZED VIEW IF NOT EXISTS Customer_View AS
-- WITH maininfo AS (
--     SELECT PD.Customer_ID AS "Cust_ID",
--            TR.Transaction_DateTime AS "Transaction_DateTime",
--            TR.Transaction_Store_ID AS "Transaction_Store_ID",
--            AVG(TR.Transaction_Summ) OVER w_pci AS "Transaction_Summ",
--            ROW_NUMBER() OVER w_pci_otd_d AS RN,
--            COUNT(*) OVER w_pcitsi AS CNT
--     FROM Personal_Data AS PD
--     JOIN Cards AS CR ON PD.Customer_ID = CR.Customer_ID
--     JOIN Transactions AS TR ON TR.Customer_Card_ID = CR.Customer_Card_ID
--     WINDOW w_pci AS (PARTITION BY PD.Customer_ID ORDER BY TR.Transaction_DateTime DESC),
--            w_pcitsi AS (PARTITION BY PD.Customer_ID, TR.Transaction_Store_ID),
--            w_pci_otd_d AS (PARTITION BY PD.Customer_ID ORDER BY TR.Transaction_DateTime DESC)
-- ),
-- cte2 AS (
--     SELECT DISTINCT "Cust_ID",
--            first_value("Transaction_Store_ID") OVER (PARTITION BY "Cust_ID" ORDER BY CNT DESC, "Transaction_DateTime" DESC) AS SHOP,
--            first_value("Transaction_Store_ID") OVER (PARTITION BY "Cust_ID" ORDER BY RN) AS LAST_SHOP FROM maininfo
-- ),
-- cte3 AS (
--     SELECT "Cust_ID", COUNT(DISTINCT "Transaction_Store_ID") last_3_cnt
--     FROM maininfo WHERE RN <= 3 GROUP BY "Cust_ID"
-- )
--
-- SELECT "Customer_ID",
--        "Customer_Average_Check",
--        "Customer_Average_Check_Segment",
--        "Customer_Frequency",
--        "Customer_Frequency_Segment",
--        "Customer_Inactive_Period",
--        "Customer_Churn_Rate",
--        "Customer_Churn_Segment",
--        Segment AS "Segment",
--        CASE
--            WHEN last_3_cnt = 1 THEN LAST_SHOP
--            ELSE SHOP
--            END AS Customer_Primary_Store
-- FROM (SELECT "Customer_ID",
--              "Customer_Average_Check",
--              CASE
--                  WHEN (PERCENT_RANK() OVER w_ocac_d < 0.1) THEN 'High'
--                  WHEN (PERCENT_RANK() OVER w_ocac_d < 0.25) THEN 'Medium'
--                  ELSE 'Low'
--                  END AS "Customer_Average_Check_Segment",
--              "Customer_Frequency",
--              CASE
--                  WHEN (PERCENT_RANK() OVER w_ocf < 0.1) THEN 'Often'
--                  WHEN (PERCENT_RANK() OVER w_ocf < 0.35) THEN 'Occasionally'
--                  ELSE 'Rarely'
--                  END AS "Customer_Frequency_Segment",
--              "Customer_Inactive_Period",
--              ("Customer_Inactive_Period"/"Customer_Frequency") AS "Customer_Churn_Rate",
--              CASE
--                  WHEN ("Customer_Inactive_Period"/"Customer_Frequency" < 2) THEN 'Low'
--                  WHEN ("Customer_Inactive_Period"/"Customer_Frequency" < 5) THEN 'Medium'
--                  ELSE 'High'
--                  END AS "Customer_Churn_Segment"
--
--       FROM (SELECT "Cust_ID" AS "Customer_ID",
--                    "Transaction_Summ" AS "Customer_Average_Check",
--                    EXTRACT(EPOCH from MAX("Transaction_DateTime") - MIN("Transaction_DateTime"))::float/86400.0/COUNT("Cust_ID") AS "Customer_Frequency",
--                    EXTRACT(EPOCH from (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) - MAX("Transaction_DateTime"))/86400.0 AS "Customer_Inactive_Period"
--             FROM maininfo GROUP BY "Customer_ID", "Transaction_Summ"
--                 WINDOW w_oats_d AS (ORDER BY sum("Transaction_Summ") DESC)) AS avmain
--       GROUP BY "Customer_ID",
--                "Customer_Average_Check",
--                "Customer_Frequency",
--                "Customer_Inactive_Period"
--           WINDOW w_ocac_d AS (ORDER BY sum("Customer_Average_Check") DESC), w_ocf AS (ORDER BY "Customer_Frequency")) AS biginfo
--           JOIN Segments AS S ON (S.Average_Check = "Customer_Average_Check_Segment" AND
--                                 S.Purchase_Frequency = "Customer_Frequency_Segment" AND
--                                 S.Churn_Probability = "Customer_Churn_Segment")
--          JOIN cte2 ON cte2."Cust_ID" = biginfo."Customer_ID"
--          JOIN cte3 ON cte3."Cust_ID" = biginfo."Customer_ID";



-- -- Purchase history View
CREATE MATERIALIZED VIEW IF NOT EXISTS Purchase_History_View AS
SELECT CR.Customer_ID,
       TR.Transaction_ID,
       TR.Transaction_DateTime,
       TR.Transaction_Store_ID,
       SKU.Group_ID,
       CK.SKU_Amount,
       SR.SKU_ID,
       SR.SKU_Retail_Price,
       SR.SKU_Purchase_Price,
       CK.SKU_Summ_Paid,
       CK.SKU_Summ,
       CK.SKU_Discount
FROM Transactions AS TR
JOIN Cards AS CR ON CR.Customer_Card_ID = TR.Customer_Card_ID
JOIN Personal_data AS PD ON PD.Customer_ID = CR.Customer_ID
JOIN Checks AS CK ON TR.Transaction_ID = CK.Transaction_ID
JOIN SKU AS SKU ON SKU.SKU_ID = CK.SKU_ID
JOIN Stores AS SR ON SKU.SKU_ID = SR.SKU_ID
AND TR.Transaction_Store_ID = SR.Transaction_Store_ID;

CREATE MATERIALIZED VIEW IF NOT EXISTS support AS
SELECT Customer_ID,
       Transaction_ID,
       Transaction_DateTime,
       Group_ID,
       sum(SKU_Purchase_Price * SKU_Amount),
       sum(SKU_Summ),
       sum(SKU_Summ_Paid)
FROM Purchase_History_View
GROUP BY Customer_ID, Transaction_ID, Transaction_DateTime, Group_ID;

-- Periods View
CREATE MATERIALIZED VIEW IF NOT EXISTS Periods_View (
        Customer_ID,
        Group_ID,
        First_Group_Purchase_Date,
        Last_Group_Purchase_Date,
        Group_Purchase,
        Group_Frequency,
        Group_Min_Discount
        ) AS
SELECT Customer_ID,
       Group_ID,
       MIN(Transaction_DateTime)::timestamp First_Date,
       MAX(Transaction_DateTime)::timestamp Last_Date,
       COUNT(*) Group_Purchase,
       (((TO_CHAR((MAX(Transaction_DateTime)::timestamp - MIN(Transaction_DateTime)::timestamp), 'DD'))::int + 1)*1.0) / COUNT(*)*1.0 AS Group_Frequency,
       COALESCE((SELECT MIN(c1.SKU_Discount / c1.SKU_Summ) FROM Checks c1
       JOIN Purchase_History_View ph2 ON ph2.Transaction_ID = c1.Transaction_ID
       WHERE (c1.SKU_Discount / c1.SKU_Summ) > 0 AND ph2.Customer_ID = t1.Customer_ID
       AND ph2.Group_ID = t1.Group_ID), 0) AS Group_Minimum_Discount
  FROM (SELECT DISTINCT Customer_ID, t.Transaction_DateTime, c.SKU_Discount, SKU.Group_ID, c.SKU_Summ
          FROM Cards
                JOIN Transactions t ON cards.Customer_Card_ID = t.Customer_Card_ID
                 JOIN Checks c ON t.Transaction_ID = c.Transaction_ID
                  JOIN SKU ON SKU.SKU_ID = c.SKU_ID) AS t1
 GROUP BY Group_ID, Customer_ID;

-- -- Groups View
CREATE MATERIALIZED VIEW IF NOT EXISTS v_more_info AS
SELECT VH."Customer_ID"               AS "Customer_ID",
       VH."Group_ID"                  AS "Group_ID",
       VH."Transaction_ID"            AS "Transaction_ID",
       VH."Transaction_DateTime"      AS "Transaction_DateTime",
       VH."Group_Cost"                AS "Group_Cost",
       VH."Group_Summ"                AS "Group_Summ",
       VH."Group_Summ_Paid"           AS "Group_Summ_Paid",
       VP."First_Group_Purchase_Date" AS "First_Group_Purchase_Date",
       VP."Last_Group_Purchase_Date"  AS "Last_Group_Purchase_Date",
       VP."Group_Purchase"            AS "Group_Purchase",
       VP."Group_Frequency"           AS "Group_Frequency",
       VP."Group_Min_Discount"        AS "Group_Min_Discount"
FROM Periods_View AS VP
         JOIN support AS VH ON VH."Customer_ID" = VP."Customer_ID" AND
                                 VH."Group_ID" = VP."Group_ID";

CREATE OR REPLACE FUNCTION fnc_create_v_group(IN int default 1, IN interval default '5000 days'::interval,
                                              IN int default 100)
    RETURNS TABLE
            (
                Customer_ID            bigint,
                Group_ID               bigint,
                Group_Affinity_Index   float,
                Group_Churn_Rate       float,
                Group_Stability_Index  float,
                Group_Margin           float,
                Group_Discount_Share   float,
                Group_Minimum_Discount numeric,
                Group_Average_Discount numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT "Customer_ID"                                      AS "Customer_ID",
               "Group_ID"                                         AS "Group_ID",
               "Group_Affinity_Index"                             AS "Group_Affinity_Index",
               "Group_Churn_Rate"                                 AS "Group_Churn_Rate",
               coalesce(avg("Group_Stability_Index"), 0)          AS "Group_Stability_Index",

               coalesce(CASE
                            WHEN ($1 = 1) THEN
                                        sum("Group_Margin"::float)
                                        FILTER (WHERE "Transaction_DateTime" BETWEEN (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) - $2 AND
                                            (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) )
                            WHEN ($1 = 2) THEN
                                (SELECT sum(GM)::float
                                 FROM (SELECT "Group_Summ_Paid" - "Group_Cost" as GM
                                       FROM v_more_info
                                       WHERE VMI."Customer_ID" = v_more_info."Customer_ID"
                                         AND VMI."Group_ID" = v_more_info."Group_ID"
                                       ORDER BY "Transaction_DateTime" DESC
                                       LIMIT $3) as SGM)
                            END, 0)                               AS "Group_Margin",

               "Group_Discount_Share",

               coalesce((SELECT min(sku_discount / sku_summ)
                         FROM Purchase_History_View AS VB
                         WHERE VB.customer_id = VMI."Customer_ID"
                           AND VB.group_id = VMI."Group_ID"
                           AND sku_discount / sku_summ > 0.0), 0) AS "Group_Minimum_Discount",

               avg(VMI."Group_Summ_Paid") / avg(VMI."Group_Summ") AS "Group_Average_Discount"


        from (SELECT "Customer_ID"                                          AS "Customer_ID",
                     "Group_ID"                                             AS "Group_ID",

                     "Group_Purchase"::float /
                     (SELECT count("Transaction_ID")
                      FROM v_more_info AS VMI
                      WHERE VMI."Customer_ID" = v_more_info."Customer_ID"
                        AND VMI."Transaction_DateTime"
                          BETWEEN v_more_info."First_Group_Purchase_Date"
                          AND v_more_info."Last_Group_Purchase_Date")       AS "Group_Affinity_Index",

                     extract(EPOCH from (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) -
                                        max("Transaction_DateTime")
                                        OVER (PARTITION BY "Customer_ID", "Group_ID"))::float / 86400.0 /
                     "Group_Frequency"                                      AS "Group_Churn_Rate",

                     abs(extract(epoch from "Transaction_DateTime" - lag("Transaction_DateTime", 1)
                                                                     over (partition by "Customer_ID", "Group_ID"
                                                                         order by "Transaction_DateTime"))::float /
                         86400.0 - "Group_Frequency") / "Group_Frequency"   as "Group_Stability_Index",

                     "Group_Summ_Paid" - "Group_Cost"                       AS "Group_Margin",
                     "Transaction_DateTime",

                     (SELECT count(transaction_id)
                      FROM Purchase_History_View AS VB
                      WHERE v_more_info."Customer_ID" = VB.customer_id
                        AND v_more_info."Group_ID" = VB.group_id
                        AND VB.SKU_Discount != 0)::float / "Group_Purchase" AS "Group_Discount_Share",
                     "Group_Summ_Paid",
                     "Group_Summ"

              FROM v_more_info) as VMI
        GROUP BY "Customer_ID", "Group_ID", "Group_Affinity_Index", "Group_Churn_Rate", "Group_Discount_Share";
END ;
$$ LANGUAGE plpgsql;

CREATE MATERIALIZED VIEW v_group AS
select *
from fnc_create_v_group();
