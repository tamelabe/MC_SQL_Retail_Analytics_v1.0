
-- Customers View

CREATE VIEW customers_view AS
SELECT c.Customer_ID as customer_id, sum(Transaction_Summ) / COUNT(Transaction_Summ) as customer_average_check,
FROM cards c
join transaction t on c.Customer_Card_ID

-- Groups View






-- Periods View







-- Purchase History View




