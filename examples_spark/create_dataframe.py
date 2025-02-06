from pyspark.sql import SparkSession


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CreateDataFrame") \
    .master("spark://localhost:7077") \
    .getOrCreate()


# Define data
data = [
    ('James', '', 'Smith', '1991-04-01', 'M', 3000),
    ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
    ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
    ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
    ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
]

# Define column names
columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

# Create DataFrame
df = spark.createDataFrame(data=data, schema=columns)

# Show DataFrame
df.show()
