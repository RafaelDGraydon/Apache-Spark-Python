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
        .load("C:\\Users\\Rafael\\Documents\\Master\\Modulo-9/simple.csv")

    data_frame.printSchema()

    data_frame.show()

    # Print the data types of the data frame
    print("data types: " + str(data_frame.dtypes))

    # Describe the dataframe
    data_frame\
        .describe()\
        .show()

    data_frame.select("Name",functions.length(data_frame["Name"]) > 4).show()

    data_frame.select("Name",data_frame["Name"].startswith("L")).withColumnRenamed("startswith(Name, L)","L-Name").show()

    # Add a new column "Senior" containing true if the person age is > 45
    data_frame.withColumn("Senior", data_frame["Age"] > 45).show()
    # Rename column HasACar as Owner
    data_frame.withColumnRenamed("HasACar", "Owner").show()
    # Sort by age
    data_frame.sort(data_frame["Age"].desc()).show()


    # Get a RDD
    rdd_from_dataframe = data_frame\
        .rdd\
        .cache()

    for i in rdd_from_dataframe.collect():
        print(i)

    # Sum all the weights (RDD)
    sum_of_weights = rdd_from_dataframe\
        .map(lambda row: row[2])\
        .reduce(lambda x, y: x + y) # sum()
    print("Sum of weights (RDDs): " + str(sum_of_weights))

    v = data_frame.select("Weight").groupBy().sum().collect()
    print(v)
    print(v[0][0])

    # Sum all the weights (dataframe)
    data_frame.select(functions.sum(data_frame["Weight"])).show()
    data_frame.agg({"Weight": "sum"}).show()

    # Get the mean age (RDD)
    mean_age = rdd_from_dataframe\
        .map(lambda row: row[1])\
        .reduce(lambda x, y: x + y) / rdd_from_dataframe.count()

    print("Mean age (RDDs): " + str(mean_age))

    # Get the mean age (dataframe)
    data_frame.select(functions.avg(data_frame["Weight"])).show()
    data_frame.agg({"Weight" : "avg"}).show()

    # Write to a json file
    data_frame\
        .write\
        .save("output.json", format="json")

    # Write to a CSV file
    data_frame\
        .write\
        .format("csv")\
        .save("output.csv")

if __name__ == "__main__":
    main()