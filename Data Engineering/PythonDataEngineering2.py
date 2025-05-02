"""
class PipelineStage:
    def run(self, data):
        raise NotImplementedError("Subclass must implement run()")

class ExtractStage(PipelineStage):
    def run(self, data=None):
        print("🔹 Extracting data...")
        extracted_data = ["apple", "banana", "cherry"]
        return extracted_data

class TransformStage(PipelineStage):
    def run(self, data):
        print("🔹 Transforming data...")
        transformed_data = [item.upper() for item in data]
        return transformed_data

class LoadStage(PipelineStage):
    def run(self, data):
        print("🔹 Loading data...")
        for item in data:
            print(f"Loaded: {item}")

# Pipeline runner
class ETLPipeline:
    def __init__(self, stages):
        self.stages = stages

    def run(self):
        data = None
        for stage in self.stages:
            data = stage.run(data)
        print("✅ Pipeline finished.")

# Define stages
stages = [ExtractStage(), TransformStage(), LoadStage()]

# Run the pipeline
pipeline = ETLPipeline(stages)
pipeline.run()
"""

# -----------------------------------------------------------------

class DataConnector:

    def __init__(self, user, password, host):
        self.__user = user
        self.__password = password
        self.__host = host


    def __str__(self):
        return f"User: {self.__user}, Password: {self.__password}, Host: {self.__host}"

    def connect(self):
        print(f"Connecting to the account : {self.__user}")

    def get_user(self):
        return {self.__user}

conn = DataConnector("admin", "secret", "localhost")
conn1 = DataConnector("admin1", "secret1", "localhost1")
print(conn)
print(conn1)
conn.connect()
print(conn.get_user())