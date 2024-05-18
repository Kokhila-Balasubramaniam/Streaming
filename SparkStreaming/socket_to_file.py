from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Application Started ...")

    spark = (
        SparkSession
        .builder
        .appName("Handling errors and Exceptions")
        .config("spark.sql.shuffle.partitions", "1000")
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')
        .getOrCreate()
    )

    stream_df = spark \
                .readStream \
                .format("socket") \
                .option("host", "localhost") \
                .option("port", "1100") \
                .load()

    print(stream_df.isStreaming)
    stream_df.printSchema()
    write_query = stream_df \
        .writeStream \
        .format("text") \
        .option("checkpointLocation", "/tmp") \
        .option("path", "file1.txt") \
        .start()

    write_query.awaitTermination()

    print("Application Completed.")
    spark.stop()
