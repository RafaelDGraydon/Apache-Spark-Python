from pyspark.sql import SparkSession, functions

def main():
    spark_session = SparkSession\
        .builder\
        .master("local[8]")\
        .getOrCreate()

    data_frame = spark_session \
        .read \
        .format("csv") \
        .options(header='true', inferschema='true') \
        .load("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/airports.csv")

    data_frame.printSchema()
    data_frame.show()

    data_frame.createOrReplaceTempView("airports")
    sql_data_frame = spark_session.sql("SELECT iso_country, type FROM airports WHERE iso_country == \"ES\"")
    sql_data_frame.printSchema().show()

    sql_data_frame\
        .groupBy("type")\
        .count()\
        .sort("count",ascending=False)\
        .show()

'''
    data_frame\
        .filter(data_frame["iso_country"]=="ES")\
        .groupBy(data_frame["Type"])\
        .count()\
        .sort('count', ascending=False)\
        .show()
'''


if __name__ == "__main__":
    main()