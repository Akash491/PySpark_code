# Task - Find the missing numbers in the column

# data = [
#  (1, ),
#  (2,),
#  (3,),
#  (6,),
#  (7,),
#  (8,)]


from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max

spark = SparkSession.builder.appName('Missing column').getOrCreate()

data = [
 (1, ),
 (2,),
 (3,),
 (6,),
 (7,),
 (8,)]

df = spark.createDataFrame(data).toDF("id")

df.show()

min_value = df.select(min(df["id"])).first()[0]

max_value = df.select(max(df["id"])).first()[0]

print(min_value)
print(max_value)

df_with_all_numbers = spark.range(min_value,max_value+1).toDF("id")

df_with_all_numbers.join(df,on="id",how="left_anti").show()


