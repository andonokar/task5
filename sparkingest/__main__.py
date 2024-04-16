import pyspark.sql

from sparkingest.sparkInit import spark
from pyspark.sql import functions as f
from pyspark.sql import types as t


def ingest_data(dfs: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Functions that gets a dataframe with bytes data, converts to string and explodes them into the columns
    needed for ingestion
    :param dfs: The dataframe that starts the ingestion
    :return: The dataframe with the correct columns exploded ready to be saved
    """
    # Converting the column to normal string
    df_data = dfs.withColumn("ascii_data", f.col("value").cast(t.StringType()))
    # Splitting the column into an array
    df_select = df_data.select(f.split(f.col("ascii_data"), '\t').alias("split_data"))
    # Exploding the array into the column to return, adding processed and current_timestamp to save them
    final_df = df_select.select(
        f.col("split_data").getItem(0).cast(t.LongType()).alias("user_id"),
        f.col("split_data").getItem(1).alias("event_name"),
        f.col("split_data").getItem(2).alias("advertiser"),
        f.split(f.col("split_data").getItem(3), " ")
        .getItem(0).cast(t.IntegerType()).alias("campaign"),
        f.col("split_data").getItem(4).alias("gender"),
        f.col("split_data").getItem(5).alias("income"),
        f.col("split_data").getItem(6).alias("page_url"),
        f.col("split_data").getItem(7).alias("region"),
        f.col("split_data").getItem(8).alias("country"),
        f.current_timestamp().alias("ingestion_time"),
        f.lit(False).alias("processed")
    )

    return final_df


def save_batch(df, epoch_id):
    """
    Function that saves for batch in writeStream() because postgres doesn't accept data streaming
    :param df: df_batch
    :param epoch_id: id_batch
    :return: None
    """
    df.write.format("jdbc").mode("append").option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/postgres") \
        .option("dbtable", 'bronze') \
        .option("user", 'postgres') \
        .option("password", 'example') \
        .save()


def main():
    """
    Main function that absorbs data from a public kafka, process and saves it streaming per batch
    :return:
    """
    # Setting sparkcontext log to see everything spark is doing
    spark.sparkContext.setLogLevel("INFO")
    # Topic from kafka we will consume
    topic = "ad_events"
    # Kafka parameters to ingest data
    kafka_params = {
        "bootstrap.servers": "public-kafka.memcompute.com:9092",
        "group.id": "liander",
        "auto.offset.reset": "earliest"
    }
    # Read stream from kafka using the parameters
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"]) \
        .option("subscribe", topic) \
        .option("group.id", kafka_params["group.id"]) \
        .option("auto.offset.reset", kafka_params["auto.offset.reset"]) \
        .load()
    # Calling the processing method
    final_df = ingest_data(df)
    # Saving the data per batch
    final_df \
        .writeStream \
        .foreachBatch(save_batch) \
        .option("checkpointLocation", "/tmp/pyspark/bronze/") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()
