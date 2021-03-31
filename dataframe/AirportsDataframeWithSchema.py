from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructField, StringType, DoubleType, IntegerType, StructType


def main():
    spark_session = SparkSession\
        .builder\
        .master("local[8]")\
        .getOrCreate()

    fields = [StructField("id", StringType(), True),
              StructField("ident", StringType(), True),
              StructField("type", StringType(), True),
              StructField("name", StringType(), True),
              StructField("latitude_deg", DoubleType(), True),
              StructField("longitude_deg", DoubleType(), True),
              StructField("elevation_ft", IntegerType(), True),
              StructField("continent", StringType(), True),
              StructField("iso_country", StringType(), True),
              StructField("iso_region", StringType(), True),
              StructField("municipality", StringType(), True),
              StructField("scheduled_service", StringType(), True),
              StructField("gps_code", StringType(), True),
              StructField("iata_code", StringType(), True),
              StructField("local_code", StringType(), True),
              StructField("home_link", StringType(), True),
              StructField("wikipedia_link", StringType(), True),
              StructField("keywords", StringType(), True)]

    data_frame = spark_session \
        .read \
        .format("csv") \
        .schema(StructType(fields))\
        .options(header='true', inferschema='true') \
        .load("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/airports.csv")

    data_frame.printSchema()
    data_frame.show()

    data_frame\
        .filter(data_frame["iso_country"]=="ES")\
        .groupBy(data_frame["Type"])\
        .count()\
        .sort('count', ascending=False)\
        .show()



if __name__ == "__main__":
    main()