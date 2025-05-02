import logging
import multiprocessing

"""
filepath = "C:/Users/Edify/PycharmProjects/PythonProject7/large_log.txt"

try:
    with open(filepath, 'r') as file:
        content = file.read()
        print(content)
except FileNotFoundError:
    print(f"File not found")

else:
    print("File read sucessfully")
finally:
    file.close()
    print("File closed sucessfully")
"""
# --------------------------------------------------------------------
"""
class DataValidationError(Exception):   # custom exception
    pass

records = [
    {'id': None, 'amount': 100},
    {'id': 1, 'amount': -50},
    {'id': 2, 'amount': 200},
    {'id': 3, 'amount': 0},
]

def validate_record(record):
    if record['id'] is None:
        raise DataValidationError("Missing record id value")
    elif record['amount'] < 0:
        raise DataValidationError("Amount value is negative")

for record in records:
    try:
        validate_record(record)
    except DataValidationError as e:
        print(f"Validation error in record {record}: {e}")
    else:
        print(f"Valid data: {record}")
"""
# --------------------------------------------------------------------
"""
import logging

logging.basicConfig(level=logging.INFO)

class DataValidationError(Exception):
    pass

def etl_step(data):
    try:
        if 'id' not in data:
            logging.info("Started to processing: %s", data)
            raise DataValidationError("Missing 'id' in data")
        logging.info("Processing data: %s", data)
    except DataValidationError as e:
        logging.error("Validation error: %s",e)
        print(f"Validation error in record {data}: {e}")
    except Exception as e:
        logging.error("Unexpected error: %s", e)
    else:
        logging.info("ETL step completed successfully")
        print(f"Valid data: {data}")
    finally:
        logging.info("ETL step finished (cleanup)")


etl_step({'name': 'Alice'})
etl_step({'id': 1, 'name': 'Bob'})
"""
#  -----------------------------------------------------------
"""
import concurrent.futures
import requests
import multiprocessing

urls = [
    "https://jsonplaceholder.typicode.com/posts/1",
    "https://jsonplaceholder.typicode.com/posts/2",
    "https://jsonplaceholder.typicode.com/posts/3",
    "https://api.agify.io/?name=michael",
    "https://api.genderize.io/?name=michael",
    "https://api.nationalize.io/?name=michael"
]

def fetch(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}

with concurrent.futures.ThreadPoolExecutor() as executor:
# max_workers = 3
# with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

    results = executor.map(fetch, urls)
    print(multiprocessing.cpu_count())  # prints total logical cores

for result in results:
    print(result)
"""
#  -----------------------------------------------------------

import pandas as pd
import threading

def process_chunk(chunk):
    print(chunk.describe())

df = pd.read_csv('large_log.txt', chunksize=10000)
threads = []

for chunk in df:
    thread = threading.Thread(target=process_chunk, args=(chunk,))
    thread.start()
    threads.append(thread)
    print(f"Active threads: {threading.active_count()}")
    print(f"Total cpu core in machine : {multiprocessing.cpu_count()}")

for thread in threads:
    thread.join()

