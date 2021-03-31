from pyspark import SparkContext, SparkConf
def main() -> None:
    spark_conf = SparkConf() \
        .setAppName("Spanish Airports") \
        .setMaster("local[4]")

    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    lines = spark_context.textFile("data/airports.csv")
    linedata = lines.map(lambda line: list(line.split(',')))
    airport_spain = linedata.filter(lambda array: array[8]=="\"ES\"")
    airportType = airport_spain.map(lambda array : array[2])
    pair = airportType.map(lambda word: (word,1))
    group = pair.reduceByKey(lambda a,b:a+b)
    results = group.sortBy(lambda pair: pair[1],ascending = False).take(10)

    for (word, count) in results:
       print("%s: %i" % (word, count))
#    results.saveAsTextFile("output.txt")
if __name__ == '__main__':
    main()
