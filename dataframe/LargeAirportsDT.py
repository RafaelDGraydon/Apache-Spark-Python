from pyspark.sql import SparkSession, functions


def main():
    spark_session = SparkSession \
        .builder \
        .master("local[8]") \
        .getOrCreate()

    data_frame_1 = spark_session \
        .read \
        .format("csv") \
        .options(header='true', inferschema='true') \
        .load("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/airports.csv")

    data_frame_1.printSchema()
    data_frame_1.show()

    data_frame_2 = spark_session \
        .read \
        .format("csv") \
        .options(header='true', inferschema='true') \
        .load("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/countries.csv")

    data_frame_2.printSchema()
    data_frame_2.show()

    data_frame_1 = data_frame_1.withColumnRenamed('name', 'name_airport')

    data_frame_join = data_frame_1 \
        .join(data_frame_2, on=(data_frame_1['iso_country'] == data_frame_2['code']))
    data_frame_join.show()

    data_frame_join \
        .filter(data_frame_join['type'] == "large_airport") \
        .groupBy(data_frame_join['name']) \
        .count() \
        .sort('count', ascending=False) \
        .show(10)

if __name__ == "__main__":
    main()
