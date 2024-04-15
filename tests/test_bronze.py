import unittest
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql import functions as f
import base64
from sparkingest import bronze


class BronzeTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("task5").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_ingest_data(self):
        encoded_text = ("OTY2ODY1NjAxMwlJbXByZXNzaW9uCURvbGxhciBUcmVlCTMgOQlGZW1hbGUJMTAwaysJLzIwMTYvMDUvZGl5LWZyaWRhL"
                        "WthaGxvLWhlYWQtdmFzZS5odG1sLwlGbG9yaWRhCVVT")

        decoded_bytes = base64.b64decode(encoded_text)
        decoded_string = decoded_bytes.decode('utf-8')
        schema = t.StructType().add("value", t.StringType())
        data = [(decoded_string,)]
        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn("value", f.col("value").cast(t.BinaryType()))
        finaldf = bronze.ingest_data(df)
        row_object = finaldf.collect()[0]
        self.assertEqual("9668656013", row_object.user_id)
        self.assertEqual("Impression", row_object.event_name)
        self.assertEqual("Dollar Tree", row_object.advertiser)
        self.assertEqual("3 9", row_object.campaign)
        self.assertEqual("Female", row_object.gender)
        self.assertEqual("100k+", row_object.income)
        self.assertEqual("Florida", row_object.region)
        self.assertEqual("US", row_object.country)


if __name__ == '__main__':
    unittest.main()
