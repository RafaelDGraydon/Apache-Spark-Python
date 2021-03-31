from pyspark import SparkContext, SparkConf

def main() -> None:
    spark_conf = SparkConf() \
        .setAppName("Spanish Airports") \
        .setMaster("local[4]")

    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    sc = spark_context

    rdd1 = sc.textFile("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/airports.csv")
    rdd2 = sc.textFile("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/countries.csv")

    data1 = rdd1.map(lambda line: list(line.split(',')))\
        .filter(lambda type: type[2] == '\"large_airport\"')\
        .map(lambda country: (country[8],1))\
        .reduceByKey(lambda a,b: a+b)

    data2 = rdd2.map(lambda line2: list(line2.split(',')))

    newRdd = data1\
        .join(data2.map(lambda x: (x[1], x[2])))\
        .sortBy(lambda x: x[1], ascending=False)\
        .take(10)

    for(word,tupleOfCountAndName) in newRdd:
        print("%s: %i" % (tupleOfCountAndName[1], tupleOfCountAndName[0]))

if __name__ == '__main__':
    main()