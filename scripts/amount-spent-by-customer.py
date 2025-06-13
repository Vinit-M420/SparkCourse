from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmtSpentByCustomer")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///C:/Work/SparkCourse/data/customer-orders.csv")

def parseOrders(order):
    fields = order.split(',')
    custId = int(fields[0])
    orderAmt = float(fields[2])
    return (custId, orderAmt)

allOrders = input.map(parseOrders)

AmtSpent = allOrders.reduceByKey(lambda x, y: round(x+y,2))
SortedAmtSpentByCust = AmtSpent.map(lambda x: (x[1], x[0])).sortByKey()

results = SortedAmtSpentByCust.collect()

for result in results:
    print(result[1], ' : ' ,result[0])
    
