from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObsureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///C:/Work/SparkCourse/data/Marvel+names")

lines = spark.read.text("file:///C:/Work/SparkCourse/data/Marvel+graph")

connections = lines.withColumn('id', func.split(func.trim(func.col("value")), " ")[0]) \
                .withColumn('connections', func.size(func.split(func.trim(func.col("value")),' ')) -1 ) \
                .groupBy('id').agg(func.sum("connections").alias("connections"))
                
obsureSuperheroes = connections.sort(func.col('connections').asc())#.first()

obsureSuperHeroesName = obsureSuperheroes.join(names, 'id').orderBy(func.asc("connections"))

#print(obsureSuperHeroesName[0] + " is the most obscure superhero with " + str(obsureSuperheroes[1]) + " co-appearances.")

obsureSuperHeroesName.show()

spark.stop()