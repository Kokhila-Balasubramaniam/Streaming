from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__=="__main__":
    print("App Started")
    spark=SparkSession.builder.appName("Parquet Streaming") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

  #  spark.sparkContext.setLogLevel("ERROR")


    parquet_df=spark.readStream\
        .format("parquet")\
        .load(path="stream_data/parquet")

    print(parquet_df.isStreaming)
    print(parquet_df.printSchema())



    writer_stream=parquet_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation", "parquet_streaming_loc") \
        .trigger(processingTime="15 second") \
        .start()

    writer_stream.awaitTermination()

    spark.stop()
    print("App end")
