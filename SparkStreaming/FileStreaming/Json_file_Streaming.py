from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__=="__main__":
    print("App Started")
    spark=SparkSession.builder.appName("Json Streaming").getOrCreate()
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

    json_df=spark.readStream.format("json").schema(input_schema) \
        .option("header","True") \
        .load(path="stream_data/json")
    print(json_df.isStreaming)
    print(json_df.printSchema())

    json_df_count=json_df.groupby("country") \
        .count() \
        .orderBy("count", ascending=False) \
        .limit(10)

    writer_stream=json_df_count.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "json_streaming_loc") \
        .trigger(processingTime="20 second") \
        .start()
    writer_stream.awaitTermination()

    spark.stop()
    print("App end")
