from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__=="__main__":
    print("App Started")
    spark=SparkSession.builder.appName("ORC Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    input_schema = StructType([
        StructField("registration_dttm", StringType(), True),
        StructField("id", IntegerType(), True), StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("cc", StringType(), True),
        StructField("country", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("comments", StringType(), True)
    ])

    ORC_df=spark.readStream.format("orc").schema(input_schema) \
        .option("header","True") \
        .load(path="stream_data/orc")
    print(ORC_df.isStreaming)
    print(ORC_df.printSchema())


    writer_stream=ORC_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation", "orc_streaming_loc") \
        .trigger(processingTime="20 second") \
        .start()

    writer_stream.awaitTermination()

    spark.stop()
    print("App end")
