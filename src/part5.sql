CREATE FUNCTION offersAimedFrequencyV (fst_n_lst_dates varchar, transactions_num bigint,
                                        churn_idx_max real, disc_trans_share real,
                                        allow_margin_share real)
RETURNS TABLE (Customer_ID bigint, Start_Date timestamp, End_Date timestamp, Required_Transactions_Count real,
                Group_Name varchar, Offer_Discount_Depth real)
LANGUAGE plpgsql AS
    $$
    DECLARE
        lower_date timestamp := split_part(fst_n_lst_dates, ' ', 1)::timestamp;
        upper_date timestamp := split_part(fst_n_lst_dates, ' ', 2)::timestamp;
    BEGIN
        RETURN QUERY
        SELECT

    END;
    $$;