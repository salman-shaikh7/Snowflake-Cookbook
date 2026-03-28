CREATE OR REPLACE PROCEDURE DB.SCHEMA.LOAD_TARGET_TABLE()
RETURNS VARCHAR()
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    STREAM VARCHAR(50);
    STREAM_STALE_STATUS BOOLEAN;
    SOURCE_TARGET_COUNT_DIFFERENCE NUMBER;
BEGIN
    --Check Stream staleness
    DESCRIBE STREAM DB.SCHEMA.SOURCE_TABLE_STREAM;

    SELECT "stale"
    INTO :STREAM_STALE_STATUS
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    --stream has data or not,

    --Check Source vs Target Differance
    SELECT COUNT(*)
    INTO :SOURCE_TARGET_COUNT_DIFFERENCE --just difference
    FROM (
        (SELECT LOAN_NUMBER, CUSTOMER_NAME, LOAN_AMOUNT, LOAN_START_DATE, LOAN_STATUS 
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
        SELECT LOAN_NUMBER, CUSTOMER_NAME, LOAN_AMOUNT, LOAN_START_DATE, LOAN_STATUS 
        FROM DB.SCHEMA.SOURCE_TABLE)
        ) AS DIFF;


    --If Steam is stale and there is difference between source and destination we will run master SP to load updated records
    IF (STREAM_STALE_STATUS = TRUE) THEN

            IF (SOURCE_TARGET_COUNT_DIFFERENCE > 0) THEN
                CALL DB.SCHEMA.LOAD_TARGET_TABLE_FROM_SOURCE();
            END IF;

        CALL DB.UTILITIES.SEND_ALERT();
        --Recreate staled stream
        CREATE OR REPLACE STREAM DB.SCHEMA.SOURCE_TABLE_STREAM ON TABLE DB.SCHEMA.SOURCE_TABLE;
        ---Recreate Task and resume the task 

    END IF;



    --Create temp table to store stream data. Doing this seprately because direct select creatation will not support rollback
    CREATE OR REPLACE TEMP TABLE SOURCE_TABLE_STREAM_DATA (
        LOAN_NUMBER        VARCHAR(20),
        CUSTOMER_NAME      VARCHAR,
        LOAN_AMOUNT        NUMBER(18,2),
        LOAN_START_DATE    DATE,
        LOAN_STATUS        VARCHAR(100),
        ACTION             VARCHAR(10),
        ISUPDATE           BOOLEAN, 
        ROW_ID             VARCHAR(50)
    );


    --INSERT  (One full transaction it will fail/succes no in between)
    --DELETE
    --DDL 

    --Putting all the transformation in one transaction so even if failure happend in the transformation phase it will keep streams as it.
    BEGIN TRANSACTION;

        INSERT INTO SOURCE_TABLE_STREAM_DATA 
        (
            LOAN_NUMBER, 
            CUSTOMER_NAME, 
            LOAN_AMOUNT, 
            LOAN_START_DATE,
            LOAN_STATUS,
            ACTION,
            ISUPDATE,
            ROW_ID
        )
        SELECT
            LOAN_NUMBER,    
            CUSTOMER_NAME, 
            LOAN_AMOUNT,    
            LOAN_START_DATE, 
            LOAN_STATUS,
            METADATA$ACTION,
            METADATA$ISUPDATE,
            METADATA$ROW_ID
        FROM
            DB.SCHEMA.SOURCE_TABLE_STREAM;


        --MERGE OPERATION on final table using stream data
        MERGE INTO DB.SCHEMA.TARGET_TABLE AS TARGET_TABLE --SCD Type 2 deactiving delete/updated record
            USING (
                    SELECT
                        *, --SELECTING REQUIRED COLUMNS FROM STREAM DATA
                        HASH(LOAN_NUMBER) AS HASHVALUE
                    FROM 
                        SOURCE_TABLE_STREAM_DATA
                    WHERE 
                        ACTION = 'DELETE'
                    ) SOURCE_TABLE
                        ON SOURCE_TABLE.LOAN_NUMBER = TARGET_TABLE.LOAN_NUMBER 
                            AND TARGET_TABLE.IS_ACTIVE = TRUE
        WHEN MATCHED THEN
            UPDATE SET
                TARGET_TABLE.IS_ACTIVE       = FALSE,
                TARGET_TABLE.RECORD_END_DATE = CURRENT_TIMESTAMP();


        INSERT INTO DB.SCHEMA.TARGET_TABLE 
        (
            TARGET_TABLE.LOAN_NUMBER,      
            TARGET_TABLE.CUSTOMER_NAME ,   
            TARGET_TABLE.LOAN_AMOUNT,      
            TARGET_TABLE.LOAN_START_DATE,  
            TARGET_TABLE.LOAN_STATUS,

            TARGET_TABLE.IS_ACTIVE,        
            TARGET_TABLE.RECORD_BEGIN_DATE,
            TARGET_TABLE.RECORD_END_DATE
        )
        SELECT
            SOURCE_TABLE.LOAN_NUMBER,      
            SOURCE_TABLE.CUSTOMER_NAME ,   
            SOURCE_TABLE.LOAN_AMOUNT,      
            SOURCE_TABLE.LOAN_START_DATE,  
            SOURCE_TABLE.LOAN_STATUS,       --error

            TRUE,        
            CURRENT_TIMESTAMP(),
            NULL
        FROM
            SOURCE_TABLE_STREAM_DATA AS SOURCE_TABLE
        WHERE 
            ACTION = 'INSERT';
    COMMIT;

    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            CALL DB.UTILITIES.SEND_ALERT();
            RAISE;
END
$$
;






--source recreated at 9 AM 

--task run 8PM 

--