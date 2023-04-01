SELECT * FROM offersGrowthCheck(2, '12-12-2018 11-11-2020', 4, 1.5, 1.5, 1.5, 1.5);

DROP FUNCTION IF EXISTS offersGrowthCheck(integer, varchar, bigint, real, real, real, real);
CREATE FUNCTION offersGrowthCheck
    (calc_method integer, fst_n_lst_date_m1 varchar,
    transact_cnt_m2 bigint, k_check_incs real, churn_idx real,
    trans_share_max real, marge_share_avl real)
    RETURNS table (Customer_ID bigint, Required_Check_Measure real)
--                     Group_Name varchar, Offer_Discount_Depth real)
    LANGUAGE plpgsql AS
    $$
    BEGIN
--      Выбор метода расчета среднего чека
        IF (calc_method = 1) THEN
            RETURN QUERY (
                SELECT ch.Customer_ID, ch.Required_Check_Measure
                FROM avgCheckM1(fst_n_lst_date_m1, k_check_incs) AS ch
            );
        ELSEIF (calc_method = 2) THEN
            RETURN QUERY (
                SELECT ch.Customer_ID, ch.Required_Check_Measure
                FROM avgCheckM2(transact_cnt_m2, k_check_incs) AS ch
            );
        ELSE
            RAISE EXCEPTION
                'Average check calculation method must be 1 or 2 (1 - per period, 2 - per quantity)';
        END IF;
    END;
    $$;

-- Считаем целевое значение среднего чека по первому методу
DROP FUNCTION IF EXISTS avgCheckM1(character varying, real);
CREATE FUNCTION avgCheckM1 (fst_n_lst_date_m1 varchar, k_check_incr real)
    RETURNS TABLE (Customer_ID bigint, Required_Check_Measure real)
    LANGUAGE plpgsql AS
    $$
    DECLARE
        lower_date date := split_part(fst_n_lst_date_m1, ' ', 1)::date;
        upper_date date := split_part(fst_n_lst_date_m1, ' ', 2)::date;
    BEGIN
        IF (lower_date < getKeyDates(1)) THEN
            lower_date = getKeyDates(1);
        ELSEIF (upper_date > getKeyDates(2)) THEN
            upper_date = getKeyDates(2);
        ELSEIF (lower_date >= upper_date) THEN
            RAISE EXCEPTION
                'last date of the specified period must be later than the first one';
        END IF;
        RETURN QUERY
            WITH pre_query AS (
                SELECT cards.customer_id AS Customer_ID, (t.transaction_summ) AS trans_summ
                FROM cards
                JOIN transactions t on cards.customer_card_id = t.customer_card_id
                WHERE t.transaction_datetime BETWEEN lower_date and upper_date)
            SELECT pq.Customer_ID, avg(trans_summ)::real * k_check_incr AS Avg_check
            FROM pre_query pq
            GROUP BY pq.Customer_ID
            ORDER BY 1;
    END;
    $$;

-- Считаем целевое значение среднего чека по второму методу
DROP FUNCTION IF EXISTS avgCheckM2(bigint, real);
CREATE FUNCTION avgCheckM2 (transact_num bigint, k_check_incr real)
    RETURNS TABLE (Customer_ID bigint, Required_Check_Measure real)
    LANGUAGE plpgsql AS
    $$
    BEGIN
        RETURN QUERY
        WITH pre_query AS (
            SELECT customer_card_id, transaction_summ
            FROM transactions
            ORDER BY transaction_datetime DESC LIMIT transact_num)
        SELECT c.Customer_ID, avg(transaction_summ)::real * k_check_incr AS Avg_check
        FROM pre_query pq
        JOIN cards c ON c.customer_card_id = pq.customer_card_id
        GROUP BY c.Customer_ID
        ORDER BY 1;
    END;
    $$;

-- Получаем даты первой или последней транз-ии в зав-ти от ключа (аргумента)
DROP FUNCTION IF EXISTS getKeyDates(integer);
CREATE FUNCTION getKeyDates(key integer)
    RETURNS SETOF date
    LANGUAGE plpgsql AS
    $$
    BEGIN
        IF (key = 1) THEN
            RETURN QUERY
            SELECT transaction_datetime::date
            FROM transactions
            ORDER BY 1 LIMIT 1;
        ELSEIF (key = 2) THEN
            RETURN QUERY
            SELECT transaction_datetime::date
            FROM transactions
            ORDER BY 1 DESC LIMIT 1;
        END IF;
    END;
    $$;


-- Это все не правильно!
CREATE FUNCTION rewardGroupDetermination (churn_idx real, trans_share_max real, marge_share_avl real)
RETURNS TABLE (

              )
LANGUAGE plpgsql AS
    $$
    BEGIN
        SELECT DISTINCT customer_id FROM groups_view ORDER BY 1;
    WITH avg_table AS (
            SELECT customer_id, group_id, group_affinity_index, group_churn_rate, group_discount_share,
                   row_number() over (partition by customer_id order by group_affinity_index DESC) as rank,
                   group_minimum_discount, group_average_discount
            FROM groups_view
            WHERE group_churn_rate <= 5 AND group_discount_share < 5),
        discount AS (
            SELECT customer_id, group_id,
                   (CEIL((group_minimum_discount * 100) / 5.0) * 5) AS Offer_Discount_Depth
            FROM avg_table ORDER BY 1;
        )
        SELECT * FROM avg_table WHERE rank = 1 ORDER BY 1 DESC;
        FROM t1
--         WHERE avg_churn_rate <= churn_idx AND avg_discount < trans_share_max
        GROUP BY group_id, avg_churn_rate, avg_discount;

--         SELECT *
--         FROM t1 WHERE  t1.group_affinity_index <= churn_idx OB;
    END;
    $$;






select *    from sorted_group();
DROP FUNCTION  sorted_group()
CREATE OR REPLACE FUNCTION sorted_group()
RETURNS TABLE(customer_id bigint, group_id bigint, group_affinity_index double precision,
              group_churn_rate double precision, group_discount_share double precision,
              group_minimum_discount numeric, av_margin double precision) AS $$
    BEGIN
        RETURN QUERY WITH cte_row_groups AS (SELECT *, RANK() OVER (PARTITION BY groups_view.customer_id ORDER BY groups_view.group_affinity_index DESC) AS number_id,
                                       AVG(group_margin) OVER (PARTITION BY groups_view.customer_id, groups_view.group_id) AS av_margin
                                FROM groups_view)
        SELECT cte_row_groups.customer_id, cte_row_groups.group_id, cte_row_groups.group_affinity_index,
               cte_row_groups.group_churn_rate, cte_row_groups.group_discount_share,
               cte_row_groups.group_minimum_discount, cte_row_groups.av_margin
        FROM cte_row_groups;
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS determination_of_the_group() CASCADE;

CREATE OR REPLACE FUNCTION determination_of_the_group(max_churn_index double precision,
        max_share_of_discount_transaction double precision, allowable_margin_share double precision)
RETURNS TABLE (customer_id bigint, Group_id bigint, Offer_Discount_Depth double precision) AS $$
    DECLARE id bigint := -1;
            value record;
            group_cur CURSOR FOR
                (SELECT *
                 FROM sorted_group());
            is_check bool := TRUE;
    BEGIN
        FOR value IN group_cur
            LOOP
                IF (is_check != TRUE AND id = value.customer_id) THEN
                    CONTINUE;
                END IF;
                IF (value.group_churn_rate <= max_churn_index AND
                   value.group_discount_share <= max_share_of_discount_transaction) THEN
                    IF (ABS(value.av_margin * allowable_margin_share / 100) >=
                        CEIL((value.group_minimum_discount * 100) / 5.0) * 0.05 * ABS(value.av_margin)) THEN
                        Customer_ID = value.customer_id;
                        Group_ID = value.group_id;
                        Offer_Discount_Depth = CEIL((value.group_minimum_discount * 100) / 5.0) * 5;
                        is_check = FALSE;
                        id = Customer_ID;
                        RETURN NEXT;
                    ELSE
                        is_check = TRUE;
                    END IF;
                ELSE
                    is_check = TRUE;
                END IF;
            END LOOP;
    END;
$$ LANGUAGE plpgsql;

SELECT *
from growth_of_average_check(2, '10.10.2020 10.10.2022', 200,  1.15, 3, 70, 30);
