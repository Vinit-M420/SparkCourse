import os
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsCounter")
sc = SparkContext(conf=conf)

# Safer way to build the path
local_path = os.path.abspath("C:/Work/SparkCourse/data/ml-100k/u.data")
spark_path = "file:///" + local_path.replace("\\", "/")

lines = sc.textFile(spark_path)
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

for key, value in sorted(result.items()):
    print(f"{key}: {value}")