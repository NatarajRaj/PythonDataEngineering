"""
Iterator: Any Python object that implements __iter__() and __next__().
Example: lists, tuples, file objects.

Generator: Special kind of iterator created using yield or generator expressions.
Advantage: Doesn’t store everything in memory — produces one value at a time on demand.
"""
"""
# ----------------------------------------------------------
my_list = [1, 2, 3]
iter_values = iter(my_list)

print(next(iter_values))
print(next(iter_values))
print("Explicity iter the values one by one using iter() and next()")
print(next(iter_values))
# ----------------------------------------------------------

# Generator
def number_stream():
    for i in range(1,4):
        yield i
# yield() - the generator does not store all values in memory like a list —
# instead, it produces one value at a time only when requested.
# This is called lazy evaluation or on-demand generation.

gen_obj = number_stream()
# for i in gen_obj:
#     print(i)
print(next(gen_obj))
print(next(gen_obj))
"""
# ----------------------------------------------------------
"""
# Generator with streaming on data
# It saves memory, not time — it avoids loading the whole file into memory but reads efficiently from disk.

def stream_of_lines(path1):

    with open(filepath, 'r') as file:
         for i in file:
            yield i  # immediately return every one value

filepath = "C:/Users/Edify/PycharmProjects/PythonProject7/large_log.txt"
for line in stream_of_lines(filepath):
    print(line)
"""
# ----------------------------------------------------------
"""
# A decorator is a function that wraps another function to add behavior (like logging, timing, or validation).
# The decorator wraps the original function and calls the wrapper function instead.
# A decorator in Python is a function that wraps another function to add extra behavior — like logging, timing, or modifying output — without changing the original function’s code.

def log_function_call(func):
    # This is a decorator that takes the original function 'func'
    def wrapper(*args, **kwargs):
        # Before the function runs, log its name
        print(f"Calling function: {func.__name__}")
        # Call the original function and store the result
        result = func(*args, **kwargs)
        # After the function finishes, log its name
        print(f"Finished function: {func.__name__}")
        # Return the result of the original function
        return result

    # Return the wrapper function
    return wrapper

@log_function_call  # This decorator will be applied to process_data
def process_data(data):
    # Original function body
    print(f"Processing {data}")

# Call the decorated function
process_data("sample")
"""
# ----------------------------------------------------------
import time
"""
def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} ran in {end - start:.4f} seconds")
        return result
    return wrapper

@timeit
def slow_operation():
    time.sleep(2)
    print("Finished slow operation")

slow_operation()
"""
# ----------------------------------------------------------
import time
"""
# Time tracking decorator
def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} ran in {end - start:.4f} seconds")
        return result
    return wrapper

# Example of ETL pipeline steps
@timeit
def extract_data(source):
    time.sleep(3)  # Simulating a 3-second data extraction step
    print(f"Data extracted from {source}")

@timeit
def transform_data(raw_data):
    time.sleep(2)  # Simulating a 2-second transformation step
    print(f"Data transformed: {raw_data}")

@timeit
def load_data(destination, data):
    time.sleep(1)  # Simulating a 1-second loading step
    print(f"Data loaded into {destination}: {data}")

# Simulating an ETL pipeline
source = "API Endpoint"
destination = "Data Warehouse"
raw_data = "Raw JSON data"

extract_data(source)
transform_data(raw_data)
load_data(destination, raw_data)
"""
# ----------------------------------------------------------
"""
import time
import requests

def timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} ran in {end - start:.4f} seconds")
        return result
    return wrapper

# Simulating an API call for data extraction
@timeit
def fetch_data_from_api(endpoint):
    response = requests.get(endpoint)  # Simulate a real API call
    return response.json()

# Example API endpoint (You can replace with an actual API)
endpoint = "https://jsonplaceholder.typicode.com/todos/1"

# Fetch data from the API
fetch_data_from_api(endpoint)
"""
# ----------------------------------------------------------

# both generator with decorator
def log_call(func):
    def wrapper(*args, **kwargs):
        print(f"Starting {func.__name__}")
        result = func(*args, **kwargs)
        print(f"Finished {func.__name__}")
        return result
    return wrapper

def stream_file_lines(filepath):
    with open(filepath, 'r') as file:
        for line in file:
            yield line.strip()

@log_call
def process_file(filepath):
    for line in stream_file_lines(filepath):
        print(f"Line: {line}")

process_file("C:/Users/Edify/PycharmProjects/PythonProject7/data.log")

# ----------------------------------------------------------






