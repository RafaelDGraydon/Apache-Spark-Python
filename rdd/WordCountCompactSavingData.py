from datetime import time

from pyspark import SparkContext, SparkConf
from nltk.corpus import stopwords


def main() -> None:
    """
    Python program that uses Apache Spark to sum a list of numbers
    """
    spark_conf = SparkConf() \
        .setAppName("AddNumbers") \
        .setMaster("local[4]")

    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    ##stopWords = ("que", "de","y","la","a","el","en")
    stopWords = set(stopwords.words('spanish'))
    print("NUmber of stopwords:" + str(len(stopWords)))

    results = spark_context.textFile("data/quijote.txt") \
        .flatMap(lambda line: line.split(' ')) \
        .filter(lambda word: word not in stopWords) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(10)

    results.saveAsTextFile("output.txt")
    spark_context.stop()


if __name__ == '__main__':
    main()
