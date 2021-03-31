from pyspark import SparkConf, SparkContext

if __name__ == "__main__":

    spark_conf = SparkConf()\
        .setAppName("Tweets")\
        .setMaster("local[4]")
    spark_context = SparkContext(conf=spark_conf)

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    def clean(word):
        words = ['the','RT','in','to','at','a','on','-','and','for','of','rt','with','nd']
        lowercased_str = word.lower()
        for ch in words:
            if len(lowercased_str) <= 4:
                lowercased_str = lowercased_str.replace(ch,'')
        return lowercased_str

    splited_data = spark_context \
        .textFile("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/tweets.tsv") \
        .map(lambda line: list(line.split('\t')))

    output1 = splited_data \
        .map(lambda word: word[2]) \
        .flatMap(lambda line: list(line.split(" ")))\
        .map(lambda word: clean(word)) \
        .filter(lambda word: word != "") \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False)\
        .take(10)

    for line in output1:
        print(line)

    output2 = splited_data \
        .map(lambda word: word[1]) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(1)

    for (user,tweets) in output2:
        print("\n User: " + str(user) + " , " + str(tweets) + " tweets" )

    output3 = splited_data \
        .map(lambda word: (word[1:4], len(word[2]))) \
        .sortBy(lambda pair: pair[1], ascending=True) \
        .take(1)

    for info,len in output3:
        print("\n Tweet: " + str(info) + " length: " + str(len))

    spark_context.stop()