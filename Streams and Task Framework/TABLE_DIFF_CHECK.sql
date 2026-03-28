            SELECT *
            FROM (
                (SELECT * 
                FROM DB.SCHEMA.SOURCE_TABLE
                MINUS
                SELECT LOAN_NUMBER, CUSTOMER_NAME, LOAN_AMOUNT, LOAN_START_DATE, LOAN_STATUS 
                FROM DB.SCHEMA.TARGET_TABLE 
                WHERE IS_ACTIVE = TRUE)
                
                UNION ALL
                
                (SELECT LOAN_NUMBER, CUSTOMER_NAME, LOAN_AMOUNT, LOAN_START_DATE, LOAN_STATUS 
                FROM DB.SCHEMA.TARGET_TABLE 
                WHERE IS_ACTIVE = TRUE
                MINUS
                SELECT * 
                FROM DB.SCHEMA.SOURCE_TABLE)
            ) AS DIFF;





SELECT
    *
FROM
    DB.SCHEMA.TARGET_TABLE;