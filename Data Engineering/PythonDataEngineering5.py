import requests

url = 'https://jsonplaceholder.typicode.com/posts'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data[:2])
else:
    print(f"Error {response.status_code}")

# -------------------------------------------------------

import httpx

with httpx.Client() as client:
    response = client.get('https://jsonplaceholder.typicode.com/posts')

if response.status_code == 200:
    data = response.json()
    print(data[:2])
else:
    print(f"Error {response.status_code}")

# -------------------------------------------------------

import httpx
import asyncio

async def fetch_data():
    async with httpx.AsyncClient() as client:
        response = await client.get('https://jsonplaceholder.typicode.com/posts')
        if response.status_code == 200:
            data = response.json()
            print(data[:2])

asyncio.run(fetch_data())

# -------------------------------------------------------

# ✅sqlalchemy - A universal database toolkit — supports multiple DBs (PostgreSQL, MySQL, SQLite, etc.), handles connections, abstracts SQL, integrates smoothly with pandas.
#
# ✅For simple ETL scripts or pandas jobs → use sqlalchemy.
# ✅ For raw performance, advanced SQL, or PostgreSQL-specific features → use psycopg2.
# ✅ For large-scale systems → sometimes combine them (sqlalchemy for general use, psycopg2 for hot paths).


import psycopg2

conn = psycopg2.connect(
    dbname='postgres', user='postgres', password='password', host='localhost', port='5432'
)
cursor = conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT)")

cursor.execute("INSERT INTO test_table (name) VALUES (%s)", ('Alice',))
conn.commit()

cursor.close()
conn.close()

# -------------------------------------------------------

from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/mydb')

df = pd.DataFrame({'name': ['Alice', 'Bob']})
df.to_sql('test_table', engine, if_exists='replace', index=False)

# -------------------------------------------------------

