from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL-AvgAge-ofFriends").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///C:/Work/SparkCourse/data/fakefriends-header.csv")
    
friendsByAge = people.select('age', 'friends') 

# friendsByAge.groupBy('age').avg('friends').show()

# friendsByAge.groupBy('age').avg('friends').sort('age').show()

friendsByAge.groupBy('age').agg(func.round(func.avg('friends'),2).alias('avg_friends')).sort('age').show()

spark.stop()