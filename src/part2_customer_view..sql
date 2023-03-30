-- Создание таблицы segments
DROP TABLE IF EXISTS segments CASCADE;

CREATE TABLE segments (
    Segment bigint PRIMARY KEY NOT NULL,
    Average_check varchar NOT NULL,
    Frequency_of_purchases varchar NOT NULL,
    Churn_probability varchar NOT NULL
);

SET DATESTYLE to iso, DMY;
SET imp_path.txt TO '/Users/tamelabe/Documents/repo/SQL3_RetailAnalitycs_v1.0-2/datasets/';
CALL import('segments', (current_setting('imp_path.txt') || 'Segments.tsv'));

-- Создание вьюхи Customers_View
DROP VIEW IF EXISTS Customers_View CASCADE;

CREATE OR REPLACE VIEW Customers_View (
    Customer_ID,
    Customer_Average_Check,
    Customer_Average_Check_Segment,
    Customer_Frequency,
    Customer_Frequency_Segment,
    Customer_Inactive_Period,
    Customer_Churn_Rate,
    Customer_Churn_Segment,
    Customer_Segment,
    Customer_Primary_Store) AS

WITH temp_table AS (SELECT *
                    FROM cards
                    JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                    WHERE transaction_datetime < (SELECT analysis_formation FROM date_of_analysis_formation)),

     size_avg_check AS (SELECT customer_id, Customer_Average_Check,
                              (ROW_NUMBER() OVER (ORDER BY Customer_Average_Check DESC)) AS r1
                        FROM (SELECT customer_id,
                                     (SUM(transaction_summ) / COUNT(transaction_summ)) AS Customer_Average_Check
                              FROM temp_table
                              GROUP BY customer_id) AS avg_check),

     freq_of_visits AS (SELECT customer_id, (TO_CHAR(dif_date, 'DD')::int +
                                            TO_CHAR(dif_date, 'HH')::int / 24.0 +
                                            TO_CHAR(dif_date, 'MM')::int / 1440.0 +
                                            TO_CHAR(dif_date, 'SS')::int / 86400.0) AS Customer_Frequency,
                              (ROW_NUMBER() OVER (ORDER BY dif_date)) AS r2
                       FROM (SELECT customer_id, ((MAX(transaction_datetime) - MIN(transaction_datetime)) /
                                                        (COUNT(transaction_id))) AS dif_date
                             FROM temp_table
                             GROUP BY customer_id) AS avg_trans),

     dif_date_last_trans AS (SELECT customer_id, (TO_CHAR(dif_date, 'DD')::int +
                                                  TO_CHAR(dif_date, 'HH')::int / 24.0 +
                                                  TO_CHAR(dif_date, 'MM')::int / 1440.0 +
                                                  TO_CHAR(dif_date, 'SS')::int / 86400.0) AS Customer_Inactive_Period
                             FROM (SELECT customer_id,
                                          (SELECT analysis_formation FROM date_of_analysis_formation) -
                                           MAX(transaction_datetime) AS dif_date
                                   FROM temp_table
                                   GROUP BY customer_id) AS q),

     charn_rate AS (SELECT freq_of_visits.customer_id, Customer_Inactive_Period,
                           (Customer_Inactive_Period / Customer_Frequency) AS Customer_Churn_Rate
                    FROM freq_of_visits
                    JOIN dif_date_last_trans ON freq_of_visits.customer_id = dif_date_last_trans.customer_id),

     list_clients_store AS (SELECT customer_id, transaction_store_id,
                                   COUNT(transaction_store_id) AS cnt
                            FROM temp_table
                            GROUP BY customer_id, transaction_store_id),

     counts_store AS (SELECT customer_id, SUM(cnt) AS all_cnt
                      FROM list_clients_store
                      GROUP BY customer_id),

     share_trans AS (SELECT counts_store.customer_id, list_clients_store.transaction_store_id ,
                            (list_clients_store.cnt / all_cnt) AS share
                     FROM counts_store
                     JOIN list_clients_store ON counts_store.customer_id = list_clients_store.customer_id),

     share_trans_rank AS (SELECT customer_id, transaction_store_id, share,
                                 RANK() OVER (PARTITION BY customer_id ORDER BY share DESC) AS rank
                          FROM share_trans),

     share_trans_rank_cnt AS (SELECT customer_id, COUNT(share) AS share_cnt, MAX(transaction_store_id) AS store_id
                              FROM share_trans_rank
                              WHERE rank = 1
                              GROUP BY customer_id),

     last_trans_of_max_share AS (SELECT customer_id, MAX(max_share) AS max_max_share, MAX(max_date) AS max_max_date
                                 FROM (SELECT a.customer_id, MAX(share) AS max_share,
                                              MAX(transaction_datetime) AS max_date
                                       FROM (SELECT share_trans_rank.customer_id, customer_card_id,
                                                    transaction_store_id, share
                                             FROM share_trans_rank
                                             JOIN cards ON share_trans_rank.customer_id = cards.customer_id) AS a
                                       JOIN transactions ON a.customer_card_id = transactions.customer_card_id
                                       GROUP BY a.customer_id, share) AS b
                                 GROUP BY customer_id),

     last_store_trans AS (SELECT cards.customer_id, transaction_store_id
                          FROM last_trans_of_max_share
                          JOIN cards ON last_trans_of_max_share.customer_id = cards.customer_id
                          JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                          WHERE last_trans_of_max_share.max_max_date = transaction_datetime),

     find_3_stores AS (SELECT customer_id, COUNT(transaction_store_id) AS cnt, transaction_store_id
                       FROM (SELECT customer_id, transaction_store_id, transaction_datetime,
                                    RANK() OVER (PARTITION BY customer_id
                                                 ORDER BY customer_id, transaction_datetime DESC,
                                                          transaction_store_id) AS rank
                             FROM temp_table
                             ORDER BY customer_id, transaction_store_id DESC) AS w
                       WHERE rank < 4
                       GROUP BY customer_id, transaction_store_id),
     primary_store AS (SELECT DISTINCT find_3_stores.customer_id,
                            (CASE WHEN find_3_stores.cnt = 3 THEN find_3_stores.transaction_store_id
                                  WHEN share_trans_rank_cnt.share_cnt = 1 THEN share_trans_rank_cnt.store_id
                                  ELSE last_store_trans.transaction_store_id END) AS Customer_Primary_Store
                       FROM find_3_stores
                       JOIN share_trans_rank_cnt ON find_3_stores.customer_id = share_trans_rank_cnt.customer_id
                       JOIN last_store_trans ON find_3_stores.customer_id = last_store_trans.customer_id),

     full_table AS (SELECT size_avg_check.customer_id, Customer_Average_Check,
                           (CASE WHEN r1 <= round((SELECT COUNT(*) FROM size_avg_check) * 0.1) THEN 'High'
                                 WHEN r1 <= round((SELECT COUNT(*) FROM size_avg_check) * 0.35) AND
                                      r1 > round((SELECT COUNT(*) FROM size_avg_check) * 0.1) THEN 'Medium'
                                 ELSE 'Low' END) AS Customer_Average_Check_Segment, Customer_Frequency,
                           (CASE WHEN r2 <= round((SELECT COUNT(*) FROM freq_of_visits) * 0.1) THEN 'Often'
                                 WHEN r2 <= round((SELECT COUNT(*) FROM freq_of_visits) * 0.35) AND
                                      r2 > round((SELECT COUNT(*) FROM freq_of_visits) * 0.1) THEN 'Occasionally'
                                 ELSE 'Rarely' END) AS Customer_Frequency_Segment, Customer_Inactive_Period,
                           Customer_Churn_Rate,
                           (CASE WHEN Customer_Churn_Rate >= 0 AND Customer_Churn_Rate <= 2 THEN 'Low'
                                 WHEN Customer_Churn_Rate > 2 AND Customer_Churn_Rate <= 5.0 THEN 'Medium'
                                 ELSE 'High' END) AS Customer_Churn_Segment, Customer_Primary_Store

                  FROM size_avg_check
                  JOIN freq_of_visits ON size_avg_check.customer_id = freq_of_visits.customer_id
                  JOIN charn_rate ON size_avg_check.customer_id = charn_rate.customer_id
                  JOIN primary_store ON size_avg_check.customer_id = primary_store.customer_id)

SELECT customer_id, Customer_Average_Check::real, Customer_Average_Check_Segment,
       Customer_Frequency::real, Customer_Frequency_Segment, Customer_Inactive_Period::real,
       Customer_Churn_Rate::real, Customer_Churn_Segment, segments.Segment AS Customer_Segment,
       Customer_Primary_Store
FROM full_table
LEFT JOIN segments ON full_table.Customer_Average_Check_Segment = segments.Average_check AND
                           full_table.Customer_Frequency_Segment = segments.Frequency_of_purchases AND
                           full_table.Customer_Churn_Segment = segments.Churn_probability
ORDER BY customer_id;


SELECT * from customers_view;