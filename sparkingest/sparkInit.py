from pyspark.sql import SparkSession


def startspark():
    """
    Function that starts the spark session
    :return: a spark variable so others can import and use
    """
    builder = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
                               "org.apache.kafka:kafka-clients:3.2.1,"
                               "org.postgresql:postgresql:42.7.3"
    ) \
        .master("local[*]") \
        .appName("task5")

    sparksession = builder.getOrCreate()
    return sparksession


spark = startspark()
