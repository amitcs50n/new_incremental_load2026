
-- Staging table
ALTER TABLE PHARMA_DB.RAW.STG_SALES_TRANSACTIONS ADD (
    day_of_week    NUMBER(38,0),
    month_year     VARCHAR(7),
    revenue_tier   VARCHAR(10),
    transformed_at TIMESTAMP_NTZ
);

-- Core fact table
ALTER TABLE PHARMA_DB.CORE.FACT_SALES ADD (
    day_of_week    NUMBER(38,0),
    month_year     VARCHAR(7),
    revenue_tier   VARCHAR(10),
    transformed_at TIMESTAMP_NTZ
);


DESC TABLE PHARMA_DB.RAW.STG_SALES_TRANSACTIONS;
DESC TABLE PHARMA_DB.CORE.FACT_SALES;


  UPDATE PHARMA_DB.CORE.FACT_SALES
SET 
    day_of_week  = DAYOFWEEK(transaction_date),
    month_year   = TO_CHAR(transaction_date, 'YYYY-MM'),
    revenue_tier = CASE 
                     WHEN total_amount < 1000 THEN 'Low'
                     WHEN total_amount < 10000 THEN 'Mid'
                     ELSE 'High'
                   END,
    transformed_at = CURRENT_TIMESTAMP()
WHERE day_of_week IS NULL;




