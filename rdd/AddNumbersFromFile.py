import time

from pyspark import SparkContext,SparkConf

def main() -> None:

    spark_conf = SparkConf()\
        .setAppName("AddNumbersFromFile")\
        .setMaster("local[8]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    init_time = time.time()
    sum =spark_context\
        .textFile("data/manyNumbers.txt")\
        .map(lambda line: int(line))\
        .reduce(lambda number1, number2: number1+number2)

    print("Sum: "+str(sum))
    finish_time = time.time()
    print("Computing time: " + str(finish_time - init_time))
    spark_context.stop()

if __name__ == '__main__':
    main()