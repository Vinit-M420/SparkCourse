from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalAmtSpentByEachCustomer").getOrCreate()

schema = StructType([StructField('customerID', IntegerType(), True), \
                    StructField('orderID', IntegerType(), True), \
                    StructField('amount', FloatType(), True)])

df = spark.read.schema(schema).csv("file:///C:/Work/SparkCourse/data/customer-orders.csv")
df.printSchema()

custByAmt = df.select('customerId', 'amount')

# custByAmt.show()

TotalAmtPerCust = custByAmt.groupBy('customerId').agg(func.round(func.sum('amount'),2).alias('total_amount')).sort('total_amount')

TotalAmtPerCust.show(TotalAmtPerCust.count())

spark.stop()