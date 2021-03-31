from pyspark import SparkContext, SparkConf

def main() -> None:
    """
    Python program that uses Apache Spark to sum a list of numbers
    """
    spark_conf = SparkConf()\
        .setAppName("AddNumbers")\
        .setMaster("local[4]")

    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    lines = spark_context.textFile("data/quijote.txt")

    words=lines.flatMap(lambda line: line.split(' ')

    pais=words.map(lambda word: (word,1))

    grouped_pairs = pairs.reduceByKey(lamba a, b: a+b)

    ordered_pairs = grouped_pairs-sortBy(lamba pair: pair[1], ascending= False)

    result = ordered_pairs.take(10)

    for(word, count) in result:
        print("%s: %i" %(word,count))

    spark_context.stop()

if __name__ == '__main__':
    main()