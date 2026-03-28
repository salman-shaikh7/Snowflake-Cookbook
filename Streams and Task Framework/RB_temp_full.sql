CREATE OR REPLACE PROCEDURE DB.SCHEMA.ROLLBACK_TEST()
RETURNS VARCHAR()
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    STREAM_STALE_STATUS BOOLEAN;
BEGIN

    BEGIN TRANSACTION;
    CREATE OR REPLACE TEMP TABLE STREAM_SNAPSHOT
    AS
    SELECT
        LOAN_NUMBER ,    
        CUSTOMER_NAME  , 
        LOAN_AMOUNT ,    
        LOAN_START_DATE , 
        LOAN_STATUS
    FROM
        DB.SCHEMA.SOURCE_TABLE_STREAM;

    --operation on it. (fail/sucess)

    -- INSERT INTO DB.SCHEMA.TARGET_TABLE 
    -- (
    --     LOAN_NUMBER ,    
    --     CUSTOMER_NAME  , 
    --     LOAN_AMOUNT ,    
    --     LOAN_START_DATE , 
    --     LOAN_STATUS
    -- )
    -- SELECT
    --     LOAN_NUMBER ,    
    --     CUSTOMER_NAME  , 
    --     LOAN_AMOUNT ,    
    --     LOAN_START_DATE , 
    --     LOAN_STATUS
    -- FROM 
    --     DB.SCHEMA.SOURCE_TABLE_STREAM;

    INSERT INTO DB.SCHEMA.TARGET_TABLE_2
    (
        LOAN_NUMBER ,    
        CUSTOMER_NAME  , 
        LOAN_AMOUNT ,    
        LOAN_START_DATE , 
        LOAN_STATUS
    )
    SELECT
        LOAN_NUMBER ,    
        CUSTOMER_NAME  , 
        LOAN_AMOUNT ,    
        LOAN_START_DATE , 
        LOAN_STATUS
    FROM 
        STREAM_SNAPSHOT;
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILED: ';
END;
$$;



CALL DB.SCHEMA.ROLLBACK_TEST();

SELECT * FROM  DB.SCHEMA.TARGET_TABLE;

    SELECT
        *
    FROM
        DB.SCHEMA.SOURCE_TABLE_STREAM;


BEGIN;

CREATE TEMP TABLE temp_test (id INT); -- auto-commit
INSERT INTO temp_test VALUES (1);

-- Only DML can be rolled back
ROLLBACK;
