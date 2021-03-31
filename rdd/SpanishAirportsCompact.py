from pyspark import SparkContext, SparkConf
def main() -> None:
    spark_conf = SparkConf() \
        .setAppName("AddNumbers") \
        .setMaster("local[4]")

    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    results = spark_context.textFile("data/airports.csv")\
        .map(lambda line: list(line.split(',')))\
        .filter(lambda array: array[8] == "\"ES\"")\
        .map(lambda array : array[2])\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(10)

    for (word, count) in results:
        print("%s: %i" % (word, count))

if __name__ == '__main__':
    main()
