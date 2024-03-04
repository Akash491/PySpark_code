# Identifying missing values and treating/imputing missing values is an important step in exploratory data analysis. Let's try it in PySpark dataframe API.
# Task - In a PySpark dataframe,
# a) Identify columns that contain no null values.
# b) Identify columns where every value is null.
# c) Identify columns with at least one null value

# emp_data = [
#  (1,'Neha' , 30 , None, 'IT'),
#  (2,'Mark' , None , None, 'HR'),
#  (3,'David' , 25 , None, 'HR'),
#  (4,'Carol' , 30 , None, None)
# ]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('Missing values').getOrCreate()

emp_data = [
 (1,'Neha' , 30 , None, 'IT'),
 (2,'Mark' , None , None, 'HR'),
 (3,'David' , 25 , None, 'HR'),
 (4,'Carol' , 30 , None, None)
]

emp_schema = StructType([
 StructField("Id" , IntegerType()) ,
 StructField("Name" , StringType()) ,
 StructField("Age" , IntegerType()) ,
 StructField("Salary" , IntegerType()) ,
 StructField("Department" , StringType()) ]
)

df = spark.createDataFrame(data = emp_data, schema=emp_schema)

df.show()

any_null_value_cols = [ column for column in df.columns if df.filter(col(column).isNull()).count() != 0 ]

all_null_values_cols = [ column for column in df.columns if df.filter(col(column).isNotNull()).count() == 0 ]

no_null_values_cols = [ column for column in df.columns if df.filter(col(column).isNull()).count() == 0 ]

print(f"Column with a null value: {any_null_value_cols}")

print(f"Column with all null value: {all_null_values_cols}")

print(f"Column with no null value: {no_null_values_cols}")
