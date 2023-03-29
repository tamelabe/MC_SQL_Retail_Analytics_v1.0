CREATE OR REPLACE FUNCTION offersGrowthCheck
    (calc_method smallint, fst_n_lst_date_m1 varchar DEFAULT '0',
    transact_cnt_m2 bigint DEFAULT 0, k_check_incs float8, churn_idx float8,
    trans_share_max real, marge_share_avl real)
    RETURNS table (Customer_ID bigint, Required_Check_Measure real,
                    Group_Name varchar, Offer_Discount_Depth real)
    LANGUAGE plpgsql AS
    $$
    BEGIN
        IF (calc_method = 1) THEN
            RETURN QUERY (
                SELECT * FROM cards
            );
        ELSEIF (calc_method = 2) THEN
            RETURN QUERY (
                SELECT * FROM cards
            );
        ELSE
            RAISE NOTICE
                'Average check calculation method must be 1 or 2 (1 - per period, 2 - per quantity)';
        END IF;
    END;
    $$;

CREATE OR REPLACE FUNCTION avgCheckM1 (fst_n_lst_date_m1 varchar)
    RETURNS TABLE (Customer_ID bigint, Avg_check float8)
    LANGUAGE plpgsql AS
    $$
    DECLARE
        lower_date date;
        upper_date date;
    BEGIN

    END;
    $$

CREATE OR REPLACE FUNCTION getKeyDates(key smallint)
    RETURNS date
    LANGUAGE plpgsql AS
    $$
    DECLARE
        keyAsc varchar := 'ORDER BY 1;';
        keyDesc varchar := 'ORDER BY 1 DESC;';
    BEGIN
        RETURN VARCHAR
    end;





$$
end;
end;
end;
end;
end;