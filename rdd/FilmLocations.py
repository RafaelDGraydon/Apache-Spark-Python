from pyspark import SparkContext, SparkConf

def main() -> None:
    spark_conf = SparkConf() \
        .setAppName("Films Locations") \
        .setMaster("local[4]")

    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    lines = spark_context.textFile("data/Film_Locations_in_San_Francisco.csv")

    linedata = lines.map(lambda line : list(line.split(',')))

    films = linedata.map(lambda array: array[0])

    numberFilm = films.map(lambda word: (word,1))

    group = numberFilm.reduceByKey(lambda a,b:a+b)

    greaterTwenty = group.filter(lambda pair: pair[1] >= 20)

    total_films_greater_Twenty = greaterTwenty.map(lambda tuple: tuple[1]).reduce(lambda a,b: a+b)

    results = greaterTwenty.sortBy(lambda pair: pair[1],ascending=False).take(5)

    for (word,count) in results:
        print("(%i, %s)" % (count,word))

    print("Total number of films: " + str(group.count()-1))
    print("The average of film locations per film: " + str(total_films_greater_Twenty/(group.count()-1)))

if __name__ == '__main__':
    main()