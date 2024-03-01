#Task - Read the third quarter (25%) of a file in a PySpark dataframe.

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, ntile

spark = SparkSession.builder.appName('Read third qtr data').getOrCreate()

data = [("Alice", 28),
 ("Bob", 35),
 ("Charlie", 42),
 ("David", 25),
 ("Eva", 31),
 ("Frank", 38),
 ("Grace", 45),
 ("Henry", 29)]

df = spark.createDataFrame(data=data,schema=['Name','Age'])

df.show()

df = df.withColumn('id',monotonically_increasing_id())

df.show()

WindowSpec = Window.orderBy('id')

df = df.withColumn('ntile',ntile(4).over(WindowSpec))

df.show()

df = df.filter(df['ntile'] == 3).drop('id').drop('ntile')

df.show()
