import pyspark.sql

from sparkingest.sparkInit import spark
from pyspark.sql import functions as f
from pyspark.sql import types as t


def ingest_data(dfs: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df_data = dfs.withColumn("ascii_data", f.col("value").cast(t.StringType()))
    df_select = df_data.select(f.split(f.col("ascii_data"), '\t').alias("split_data"))
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
    df.write.format("jdbc").mode("append").option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", 'bronze') \
        .option("user", 'admin') \
        .option("password", 'example') \
        .save()


def main():
    spark.sparkContext.setLogLevel("INFO")
    topic = "ad_events"

    kafka_params = {
        "bootstrap.servers": "public-kafka.memcompute.com:9092",
        "group.id": "liander",
        "auto.offset.reset": "earliest"
    }

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"]) \
        .option("subscribe", topic) \
        .option("group.id", kafka_params["group.id"]) \
        .option("auto.offset.reset", kafka_params["auto.offset.reset"]) \
        .load()

    final_df = ingest_data(df)

    final_df \
        .writeStream \
        .foreachBatch(save_batch) \
        .option("checkpointLocation", "/tmp/pyspark/bronze/") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()
