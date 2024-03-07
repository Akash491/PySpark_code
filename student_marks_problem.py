# Task - Transform the input DataFrame into a new DataFrame , where each row represents a unique student, and the columns include the student's name along with the marks for the "math" and "eng" subjects.

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, first

def method_1(df):
    print("Method 1")
    df = df.withColumn('math',when(df['Sub']=='math',df['Marks']).otherwise(None)).withColumn('eng',when(df['Sub']=='eng',df['Marks']).otherwise(None)).drop('Sub','Marks')

    df.show()

    df = df.groupby('Name').max('eng','math').withColumnRenamed('max(eng)','eng').withColumnRenamed('max(math)','math')

    df.show()

def method_2(df):
    print("Method 2")
    df = df.groupby("Name").pivot("Sub").agg(first(df.Marks))
    df.show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Student Marks app').getOrCreate()

    data = [
    ('Rudra','math',79),
    ('Rudra','eng',60),
    ('Shivu','math', 68),
    ('Shivu','eng', 59),
    ('Anu','math', 65),
    ('Anu','eng',80)
    ]

    df = spark.createDataFrame(data=data,schema=['Name','Sub','Marks'])

    df.show()
    
    method_1(df)
    method_2(df)