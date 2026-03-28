from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine,text

engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user='SALMANSHAIKH',
        password='Password',
        account_identifier='QSISEDK-PSA51301',
        database="SNOWFLAKE_LEARNING_DB",
        role="ACCOUNTADMIN",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
    )
)
try:
    connection = engine.connect()
    results = connection.execute(text('select current_version()')).fetchone()
    print(results[0])
finally:
    connection.close()
    engine.dispose()