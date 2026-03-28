import os
import snowflake.connector
from sqlalchemy.pool import QueuePool

def get_snowflake_connection( user=None, password=None, account=None, warehouse=None, database=None, schema=None, role=None, authenticator=None, timeout=60):
    user = user or os.getenv("SNOWFLAKE_USER")
    password = password or os.getenv("SNOWFLAKE_PASSWORD")
    account = account or os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    database = database or os.getenv("SNOWFLAKE_DATABASE")
    schema = schema or os.getenv("SNOWFLAKE_SCHEMA")
    role = role or os.getenv("SNOWFLAKE_ROLE")
    authenticator = authenticator or os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake")

    if not (user and password and account):
        raise ValueError("SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, and SNOWFLAKE_ACCOUNT are required")

    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        authenticator=authenticator,
        login_timeout=timeout
    )

def get_snowflake_connection_pool(pool_size=5, max_overflow=10):
    return QueuePool(get_snowflake_connection, pool_size=pool_size, max_overflow=max_overflow)

# usage example
if __name__ == "__main__":
    pool = get_snowflake_connection_pool()
    conn = pool.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_VERSION()")
            print(cur.fetchone())
    finally:
        conn.close()