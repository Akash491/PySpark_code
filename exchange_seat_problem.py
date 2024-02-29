# Task
# Write a solution to swap the seat id of every two consecutive students. If the number of students is odd, the id of the last student is not swapped.

# seat_data = [
#  (1 , 'Abbot'),
#  (2, 'Doris'),
#  (3, 'Emerson' ),
#  (4, 'Green'),
#  (5,'Jeames' )
# ]

from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, lead, when, coalesce
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Exchange Seat').getOrCreate()

seat_data = [
 (1 , 'Abbot'),
 (2, 'Doris'),
 (3, 'Emerson' ),
 (4, 'Green'),
 (5,'Jeames' )
]

df = spark.createDataFrame(data=seat_data,schema=['Seat no','Student'])

df.show()

df = df.withColumn('prev_student',lag('Student').over(Window.orderBy('Seat no'))).withColumn('next_student',lead('Student').over(Window.orderBy('Seat no')))

df.show()

df = df.withColumn('exchange_seat',coalesce(when(df['Seat no']%2 != 0,df['next_student']).when(df['Seat no']%2 == 0,df['prev_student']),df['Student'])).drop('prev_student').drop('next_student')

df.show()
