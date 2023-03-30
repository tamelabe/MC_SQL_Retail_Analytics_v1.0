DROP FUNCTION IF EXISTS find_margin(integer,integer) CASCADE;

CREATE OR REPLACE FUNCTION find_margin(mode int DEFAULT 1, option int DEFAULT 5000)
RETURNS TABLE (customer_id_new bigint, group_id_new bigint, Group_Margin real) AS $$
    BEGIN
        IF (mode = 1) THEN
            RETURN QUERY SELECT mode1.customer_id, mode1.group_id, SUM(margin) AS Group_Margin
                         FROM (SELECT customer_id, group_id, (group_summ_paid - group_cost) AS Margin
                               FROM purchase_history_view
                               WHERE transaction_datetime <=
                                     (SELECT analysis_formation FROM date_of_analysis_formation)
                               ORDER BY transaction_datetime DESC
                               LIMIT option) AS mode1
                         GROUP BY customer_id, group_id;
        ELSEIF (mode = 2) THEN
            RETURN QUERY SELECT mode2.customer_id, mode2.group_id, SUM(margin) AS Group_Margin
                         FROM (SELECT customer_id, group_id, (group_summ_paid - group_cost) AS Margin
                               FROM purchase_history_view
                               ORDER BY transaction_datetime DESC
                               LIMIT option) AS mode2
                         GROUP BY group_id, customer_id;
        END IF;
    END;
$$ LANGUAGE plpgsql;

DROP VIEW IF EXISTS Groups_View;

CREATE OR REPLACE VIEW Groups_view AS
    WITH find_group_id AS
        (SELECT DISTINCT customer_id, group_id
        FROM (SELECT cards.customer_id, sku_id
            FROM personal_data
            JOIN cards ON personal_data.customer_id = cards.customer_id
            JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
            JOIN checks ON transactions.transaction_id = checks.transaction_id
            WHERE transaction_datetime < (SELECT analysis_formation FROM date_of_analysis_formation)) AS sku_id
        JOIN SKU ON sku_id.sku_id = SKU.sku_id),

    find_Group_Affinity_Index AS 
        (SELECT cnt_trans.customer_id, group_id, (group_purchase::real / cnt::real) AS Group_Affinity_Index
        FROM (SELECT max_min_date.customer_id, COUNT(transaction_id) AS cnt
              FROM (SELECT periods_view.customer_id, periods_view.group_id,
                           MAX(transaction_datetime) AS max_date,
                           MIN(transaction_datetime) AS min_date
                    FROM purchase_history_view
                    JOIN periods_view ON purchase_history_view.customer_id = periods_view.customer_id AND
                                         periods_view.group_id != purchase_history_view.group_id
                    GROUP BY periods_view.customer_id, periods_view.group_id) AS max_min_date
              JOIN purchase_history_view ON max_min_date.customer_id = purchase_history_view.customer_id AND
                                            max_min_date.group_id = purchase_history_view.group_id
              WHERE transaction_datetime >= min_date AND transaction_datetime <= max_date
              GROUP BY max_min_date.customer_id) AS cnt_trans
        JOIN periods_view ON cnt_trans.customer_id = periods_view.customer_id),

     find_Group_Churn_Rate AS (SELECT dif_date.customer_id, dif_date.group_id,
                                      (dif_date / 86400.0 / periods_view.group_frequency) AS Group_Churn_Rate
                               FROM (SELECT find_group_id.customer_id, find_group_id.group_id,
                                            EXTRACT(EPOCH FROM ((SELECT analysis_formation FROM date_of_analysis_formation) -
                                                                        MAX(transaction_datetime))) AS dif_date
                                     FROM find_group_id
                                     JOIN purchase_history_view ON find_group_id.customer_id = purchase_history_view.customer_id AND
                                                                   find_group_id.group_id = purchase_history_view.group_id
                                     GROUP BY find_group_id.customer_id, find_group_id.group_id) AS dif_date
                               JOIN periods_view ON dif_date.customer_id = periods_view.customer_id AND
                                                    dif_date.group_id = periods_view.group_id),

     find_Group_Stability_Index AS (SELECT customer_id, group_id, COALESCE(AVG(temp), 0) AS Group_Stability_Index
                                    FROM (SELECT ph.customer_id, ph.group_id,
                                                 (ABS(EXTRACT(EPOCH FROM (transaction_datetime -
                                                  LAG(transaction_datetime, 1) OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY transaction_datetime))) /
                                                      86400.0 - Group_Frequency) / Group_Frequency) AS temp
                                          FROM purchase_history_view AS ph
                                          JOIN periods_view ON ph.customer_id = periods_view.customer_id AND
                                                               ph.group_id = periods_view.group_id) AS temp_value
                                    GROUP BY customer_id, group_id),

     find_Group_Discount_Share AS (SELECT DISTINCT periods_view.customer_id, periods_view.group_id,
                                                   COUNT(checks.transaction_id) FILTER (WHERE sku_discount > 0)
        OVER (PARTITION BY periods_view.customer_id, periods_view.group_id)::real / group_purchase AS Group_Discount_Share
                                   FROM checks
                                   JOIN purchase_history_view ON checks.transaction_id = purchase_history_view.transaction_id
                                   JOIN periods_view ON purchase_history_view.customer_id = periods_view.customer_id AND
                                                        purchase_history_view.group_id = periods_view.group_id),

     find_Group_Minimum_Discount AS (SELECT periods_view.customer_id, periods_view.group_id,
                                            MIN(Group_Min_Discount) AS Group_Minimum_Discount
                                     FROM purchase_history_view
                                     JOIN periods_view ON purchase_history_view.customer_id = periods_view.customer_id AND
                                                          purchase_history_view.group_id = periods_view.group_id
                                     WHERE Group_Min_Discount > 0
                                     GROUP BY periods_view.customer_id, periods_view.group_id),
     find_Group_Average_Discount AS (SELECT customer_id, group_id,
                                            (SUM(Group_Summ_Paid) / SUM(Group_Summ)) AS Group_Average_Discount
                                     FROM purchase_history_view
                                     GROUP BY customer_id, group_id)

SELECT GAI.customer_id, GAI.group_id, Group_Affinity_Index, Group_Churn_Rate, Group_Stability_Index,
       Group_Margin, Group_Discount_Share, COALESCE(Group_Minimum_Discount, 0) AS Group_Minimum_Discount,
       Group_Average_Discount
FROM find_Group_Affinity_Index AS GAI
JOIN find_Group_Churn_Rate AS GCR ON GAI.customer_id = GCR.customer_id AND GAI.group_id = GCR.group_id
JOIN find_Group_Stability_Index AS GSI ON GAI.customer_id = GSI.customer_id AND GAI.group_id = GSI.group_id
JOIN find_margin() AS GM ON GAI.customer_id = GM.customer_id_new AND GAI.group_id = GM.group_id_new
JOIN find_Group_Discount_Share AS GDS ON GAI.customer_id = GDS.customer_id AND GAI.group_id = GDS.group_id
LEFT JOIN find_Group_Minimum_Discount AS GMD ON GAI.customer_id = GMD.customer_id AND GAI.group_id = GMD.group_id
JOIN find_Group_Average_Discount AS GAD ON GAI.customer_id = GAD.customer_id AND GAI.group_id = GAD.group_id;