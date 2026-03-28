from io import BytesIO
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from snowflake_connection_pooling.connection_pooling import get_snowflake_connection_pool


mypool = get_snowflake_connection_pool()

urls = []

def download_and_upload(url):
    filename = os.path.basename(url)

    with requests.get(url, stream=True, timeout=500) as response:
        response.raise_for_status()

        with BytesIO() as file_stream:
            for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                file_stream.write(chunk)

            file_stream.seek(0)

            connection = mypool.connect()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        PUT file://{filename} @INTERNAL_STAGE_NAME/RAW_FILES/
                        PARALLEL = 16
                        AUTO_COMPRESS = TRUE
                        SOURCE_COMPRESSION = NONE
                        OVERWRITE = TRUE
                        """,
                        file_stream=file_stream,
                    )
            finally:
                if connection:
                    connection.close()


# Run in parallel
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(download_and_upload, url)
        for url in urls
    ]

    for future in as_completed(futures):
        future.result()