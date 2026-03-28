
-   **Storage Layer**
    -   Stores all structured and semi-structured data in a centralized, compressed, and encrypted format.

-   **Compute Layer (Virtual Warehouses)**
    -   Independent clusters that process queries. 
    -   You can scale them up or down without affecting storage. 
    -   We use 1 compute warehouse for our team ```DW_WH``` with size X-Small.
    -   X-Small gives 1 cluster with 1 “server” of compute resources.
    -   Moderate usage: 10–20 people may experience queuing if they all run queries simultaneously.
    
-   **Time Travel** 
    -   Lets you query historical data (up to 90 days) for recovery or auditing.

-   **Zero-Copy Cloning**
    -  Create instant, cost-free copies of databases, schemas, or tables without duplicating data. 
    -  Copy-on-write (CoW) behavior

        | Action                 | Original                     | Clone                         | Storage behavior                                             |
        | ---------------------- | ---------------------------- | ----------------------------- | ------------------------------------------------------------ |
        | Before any changes     | Points to block A            | Points to block A             | 1 physical block                                             |
        | Update row in original | Points to block B (modified) | Points to block A (unchanged) | New block B created, original block A preserved for clone    |
        | Update row in clone    | Points to block A (original) | Points to block C (modified)  | New block C created, original block A preserved for original |

    -  Key idea: Snowflake doesn’t overwrite blocks. It creates new versions of blocks for every change, which enables Time Travel and zero-copy cloning.


-   **Multi-Cloud Compatibility**
    -   Snowflake runs on AWS, Azure, and Google Cloud.
    -   Provides cross-cloud replication and data sharing, so you can operate in a multi-cloud environment seamlessly


-   **Snowpipe**
    -   Continuous data ingestion service.
    -   Automates loading of new data as it arrives in your cloud storage (S3, GCS, Azure Blob).

-  **Streams & Tasks**
    -   Streams: Track changes (inserts, updates, deletes) to a table.
    -   Tasks: Automate SQL operations on a schedule or triggered by an event.


-   **Micro-Partitioning**
    -   Snowflake automatically splits data into small chunks (micro-partitions).
This makes queries faster because it only scans what’s needed.


-   **Internal and External Stage**

    -  Files in a stage are not tables, so you cannot run arbitrary SQL queries on them directly.
    -  Use ```COPY INTO``` to load stage data into a temporary table or staging table.
    - Internal : Files stored in snowflake
    - External : Files Stored in external storage you define just pointer.


-   



