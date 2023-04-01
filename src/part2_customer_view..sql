-- Создание таблицы segments
DROP TABLE IF EXISTS segments CASCADE;
CREATE TABLE segments (
    Segment integer PRIMARY KEY NOT NULL,
    Average_check varchar NOT NULL,
    Frequency_of_purchases varchar NOT NULL,
    Churn_probability varchar NOT NULL
);

-- Импорт данных в таблицу segments
SET DATESTYLE to iso, DMY;
SET imp_path.txt TO '/Users/tamelabe/Documents/repo/SQL3_RetailAnalitycs_v1.0-2/datasets/';
CALL import('segments', (current_setting('imp_path.txt') || 'Segments.tsv'));

-- Создание Сustomers_View
DROP VIEW IF EXISTS Customers_View CASCADE;
CREATE VIEW Customers_View (
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

    WITH
    transactions_plus AS (
        SELECT c.customer_id, c.customer_card_id, t.transaction_id ,t.transaction_summ,
               t.transaction_datetime, t.transaction_store_id
        FROM transactions t
        JOIN cards c on c.customer_card_id = t.customer_card_id
        WHERE t.transaction_datetime <=
              (SELECT da.analysis_formation FROM date_of_analysis_formation da)
    ),
    avg_check AS (
        WITH temp AS (
            SELECT customer_id, sum(transaction_summ) / count(transaction_id)::real
                AS Customer_Average_Check
            FROM transactions_plus
            GROUP BY customer_id)
        SELECT row_number() over (ORDER BY Customer_Average_Check DESC) AS row,
               customer_id, Customer_Average_Check
        FROM temp),
    avg_check_seg AS (
        SELECT row, customer_id, Customer_Average_Check,
            (CASE
                WHEN row <= (SELECT (max(row) * 0.1)::bigint FROM avg_check) THEN 'High'
                WHEN row <= (SELECT (max(row) * 0.35)::bigint FROM avg_check)
                   AND row > (SELECT (max(row) * 0.10)::bigint FROM avg_check) THEN 'Medium'
                ELSE 'Low' END)::varchar AS Customer_Average_Check_Segment
        FROM avg_check),
    cus_freq AS (
        WITH temp1 AS (
            SELECT t2.customer_id, (round((
                extract(year from (max(transaction_datetime) - min(transaction_datetime))) * 365) +
                extract(day from (max(transaction_datetime) - min(transaction_datetime))) + (
                extract(hour from (max(transaction_datetime) - min(transaction_datetime))) / 24), 0) / count(transaction_id))::real AS freq
            FROM transactions_plus t2 GROUP BY t2.customer_id
        )
        SELECT row_number() over (ORDER BY temp1.freq) AS row, ac.customer_id, ac.Customer_Average_Check, ac.Customer_Average_Check_Segment,
                temp1.freq AS Customer_Frequency
        FROM avg_check_seg ac
        JOIN temp1 ON temp1.customer_id = ac.customer_id
    ),
    cus_freq_seg AS (
        SELECT *,
            (CASE
                WHEN row <= (SELECT (max(row) * 0.1)::bigint FROM avg_check) THEN 'Often'
                WHEN row <= (SELECT (max(row) * 0.35)::bigint FROM avg_check)
                   AND row > (SELECT (max(row) * 0.10)::bigint FROM avg_check) THEN 'Occasionally'
                ELSE 'Rarely' END)::varchar AS Customer_Frequency_Segment
        FROM cus_freq),
    cus_inact_per AS (
        WITH get_diffrence AS (
                SELECT customer_id,
                       ((SELECT analysis_formation FROM date_of_analysis_formation) -
                       max(t.transaction_datetime)) AS difference
                FROM transactions_plus t
                GROUP BY 1),
            convert_to_days AS (
                SELECT gd.customer_id, ((
                    extract(year from (gd.difference)) * 365) +
                    extract(day from (gd.difference)) + (
                    extract(hour from (gd.difference)) / 24) +
                    extract(minute from (gd.difference)) / 1440)::real AS difference_c
                FROM get_diffrence gd)
        SELECT fs.customer_id, fs.Customer_Average_Check, fs.Customer_Average_Check_Segment, fs.Customer_Frequency,
             fs.Customer_Frequency_Segment, df.difference_c AS Customer_Inactive_Period
        FROM cus_freq_seg fs
        JOIN convert_to_days df ON df.customer_id = fs.customer_id
    ),
    cus_churn_rate AS (
        SELECT *, (cp.Customer_Inactive_Period / cp.Customer_Frequency)::real AS Customer_Churn_Rate
        FROM cus_inact_per cp
    ),
    cus_churn_rate_seg AS (
        SELECT *,
            (CASE
                WHEN Customer_Churn_Rate < 2 THEN 'Low'
                WHEN Customer_Churn_Rate >= 2 AND
                     Customer_Churn_Rate < 5 THEN 'Medium'
                ELSE 'High' END) AS Customer_Churn_Segment
        FROM cus_churn_rate),
    cus_seg AS (
        SELECT crs.customer_id, crs.Customer_Average_Check, crs.Customer_Average_Check_Segment,
               crs.Customer_Frequency, crs.Customer_Frequency_Segment, crs.Customer_Inactive_Period,
               crs.Customer_Churn_Rate, crs.Customer_Churn_Segment, s.Segment AS Customer_Segment
        FROM cus_churn_rate_seg crs
        JOIN segments s ON  s.average_check = crs.Customer_Average_Check_Segment AND
                            s.frequency_of_purchases = crs.Customer_Frequency_Segment AND
                            s.churn_probability = crs.Customer_Churn_Segment),
    cus_p_store AS (
        WITH stores_trans_total AS (
            SELECT customer_id, count(transaction_id) AS total_trans
            FROM transactions_plus
            GROUP BY 1),
        stores_trans_cnt AS (
            SELECT tp.customer_id, tp.transaction_store_id, count(transaction_store_id) AS trans_cnt, max(transaction_datetime) AS last_date
            FROM transactions_plus tp
            GROUP BY 1, 2),
        stores_trans_share AS (
            SELECT stc.customer_id, stc.transaction_store_id, stc.trans_cnt, (stc.trans_cnt::real / stt.total_trans)::real AS trans_share, stc.last_date
            FROM stores_trans_cnt stc
            JOIN stores_trans_total stt ON stt.customer_id = stc.customer_id
            ORDER BY 1, 3 DESC),
        stores_trans_share_rank AS (
            SELECT *, row_number() over (partition by customer_id order by trans_share DESC, last_date DESC) AS row_share_date
            FROM stores_trans_share),
        trans_num AS (
            SELECT t1.customer_id, t1.transaction_store_id, t1.transaction_datetime, t1.row
            FROM (SELECT *, row_number() over (partition by customer_id ORDER BY transaction_datetime DESC) row FROM transactions_plus t1) t1
            ORDER BY 1, transaction_datetime DESC),
        last_stores_trans AS (
            SELECT tn.customer_id, tn.transaction_store_id, tn.transaction_datetime
            FROM trans_num tn
            WHERE tn.row <= 3
            ORDER BY 1),
        last_store_trans AS (
            SELECT tn.customer_id, tn.transaction_store_id, tn.transaction_datetime
            FROM trans_num tn
            WHERE tn.row <= 1
            ORDER BY 1),
        customers_with_same_stores AS (
            SELECT customer_id
            FROM last_stores_trans
            GROUP BY customer_id
            HAVING count(distinct transaction_store_id) = 1),
        req1_customers AS (
            SELECT customer_id, transaction_store_id AS Customer_Primary_Store
            FROM stores_trans_share_rank
            WHERE row_share_date = 1 AND customer_id IN (SELECT * FROM customers_with_same_stores)),
        req23_customers AS (
            SELECT customer_id, transaction_store_id AS Customer_Primary_Store
            FROM stores_trans_share_rank
            WHERE row_share_date = 1 AND customer_id NOT IN (SELECT * FROM customers_with_same_stores)),
        union_tables AS (
            SELECT * FROM req23_customers
            UNION
            SELECT * FROM req1_customers)
    SELECT cs.*, ut.Customer_Primary_Store
    FROM cus_seg cs
    JOIN union_tables ut ON ut.customer_id = cs.customer_id)

        SELECT *
        FROM cus_p_store
        ORDER BY 1;

SELECT * from customers_view;