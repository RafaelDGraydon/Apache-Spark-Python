from pyspark.sql import SparkSession, functions


def main():
    spark_session = SparkSession \
        .builder \
        .master("local[8]") \
        .getOrCreate()

    data_frame = spark_session \
        .read \
        .csv("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/tweets.tsv", sep="\t")

    # data_frame.printSchema()
    data_frame.show()

    df1 = data_frame\
        .select(functions.split(data_frame['_c2'], ' '))\
        .withColumnRenamed('split(_c2,  , -1)', 'col')

    df1 = df1.select(functions.explode(df1['col']))

    df1 = df1.filter(~df1['col'].contains("http"))\
        .filter(~df1['col'].contains("#"))\
        .filter(~df1['col'].contains(":"))\
        .filter(~df1['col'].contains("?"))\
        .filter(~df1['col'].contains("."))

    df1 = df1.filter(~df1['col'].isin(['RT', '-', "rt", "nd", "the","in","to","at","with","a","for","of","on","and"]))\
        .groupBy(df1['col'])\
        .count()\
        .sort('count', ascending=False)\
        .show(10)

    df2=data_frame\
        .select(data_frame['_c1'])\
        .groupBy(data_frame['_c1'])\
        .count()\
        .sort('count',ascending=False)\
        .show(10)

    df3=data_frame\
        .select(data_frame['_c1'], functions.length(data_frame['_c2']),data_frame['_c3'])\
        .sort(data_frame['_c2'], ascending=True)\
        .show(1)

if __name__ == "__main__":
    main()
