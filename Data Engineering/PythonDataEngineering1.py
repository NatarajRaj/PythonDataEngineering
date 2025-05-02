import json
import re
from csv import DictReader
from functools import reduce
from http.client import responses
from logging import exception
import pandas as pd
import requests
import csv

"""
numbers = [1, 2, 3, 4]
squared = list(map(lambda x: x ** 2, numbers))
evens = list(filter(lambda x: x%2==0, numbers))
products = reduce(lambda x,y: x*y, numbers)
print(squared, evens, products)
square = lambda x: x**2
print(square(4))
"""
# -----------------------------------------------------
"""
# Error Handling with try-except
try:
     num = int(input('Enter the input number: '))
     result = 10/num
except ValueError:
     print("Invalid number")
except ZeroDivisionError:
     print("Number not divided by zero")
except exception as e:
     print(e)
else:
     print(f"Result,{result}")
finally:
     print("Closed the program either success or failure")
"""

# -----------------------------------------------------
"""
words =['apple', 'banana', 'cherry']
words = list(map(lambda x: x.upper(), words))
# words = list(map(str.upper, words))
print(words)
lower_words = list(map(str.lower, words))
print(lower_words)
filter_words = list(filter(lambda x: x.startswith('a'), lower_words))
print(filter_words)
length_words = list(filter(lambda x: len(x)>5, lower_words))
print(length_words)
"""
# -----------------------------------------------------
# Safely process records - handling missing fields
"""
records = [{'name': 'John'}, {'name': 'Jane'}, {}]
for record in records:
    try:
        print(record['name'].upper())
    except KeyError:
        print("Record is the not found")

#  Argument unpacking value

def greet(first,last):
    print(f"firstname: {first} , lastname: {last}")

person = ('John', 'Doe')
greet(*person)  # Unpacking tuple

details = {'first': 'Jane', 'last': 'Smith'}
greet(**details)   # Unpacking dict
"""
# -----------------------------------------------------

# Reusable ETL functions
"""Standardize column names to lowercase with underscores"""
"""
def cleanCols(cols):
    result = [n.strip().lower().replace(' ', '_') for n in cols]
    return result

columns = ["First name", "Last name", "Email address"]
print(cleanCols(columns))
"""
# -----------------------------------------------------
"""
# lambda func
df = pd.DataFrame({'name': ['John', 'Jane'], 'email': ['JOHN@EX.com', 'jane@EX.com']})
df['email'] = df['email'].apply(lambda x: x.lower())
print(df)
"""
# -----------------------------------------------------
"""
def fetch_data(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.Timeout:
        print(f"Timeout while fetching: {url}")
    except requests.HTTPError:
        print(f"Http Error: {url}")
    except requests.ConnectionError:
        print(f"Network connection error while connecting: {url}")
    except exception as e:
        print(f"Unexcepted error: {e}")




url = 'https://randomuser.me/api/'

data = fetch_data(url)
print(data)
"""

# -----------------------------------------------------
# Writing to a file
"""
with open('output.txt', 'w') as f:
    f.write("Hello, Data engineering")
    f.close()

# Reading to a file
with open('output.txt', 'r') as f:
    content = f.read()
    print(content)

with open('output.txt', 'r') as f:
    for line in f:
        print(line.strip())
"""
# -----------------------------------------------------
"""
with open("C:/Users/Edify/PycharmProjects/PythonProject7/data.csv", newline='') as f:
    reader = DictReader(f)
    for line in reader:
        # print(line)
        print(f"{line['user']} : {line['email']}")
"""
# -----------------------------------------------------
"""
with open("C:/Users/Edify/PycharmProjects/PythonProject7/data.json", 'r') as f:
    content1 = json.load(f)
    # print(content1)

    for record in content1:
        total_purchase = sum(p['amount'] for p in record['purchases'])
        print(f"{record['name']} : {record['email']} : {total_purchase}")
"""
# -----------------------------------------------------
# split, join, replace
"""
text = "apple,banana,cherry"
text = text.split(',')
print(text) # ['apple', 'banana', 'cherry']

join_text = ','.join(text)
print(join_text) # apple,banana,cherry

replace_text = join_text.replace('apple','custard_apple')
print(replace_text) # custard_apple,banana,cherry

text = "Order123: $250"
regex_text = re.search(r"\d+", text)
if regex_text:
    print(regex_text.group())  # '123' (extracts first number)

email = "anataraj95@gmail.com"
masked = re.sub(r"@.*", "@*****", email)
masked1 = re.sub(r".*@", "*****@", email)
print(masked, masked1)
"""
# -----------------------------------------------------