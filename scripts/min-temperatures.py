from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    #temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 ## FARENHEIT
    temperature = fields[3]
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///C:/Work/SparkCourse/data/1800.csv")
parsedLines = lines.map(parseLine)

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

print('Minimun temp at Each Stations')
for result in results:
    print(result[0] , result[1])
