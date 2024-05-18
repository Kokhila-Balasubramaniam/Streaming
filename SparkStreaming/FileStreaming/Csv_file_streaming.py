from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__=="__main__":
    print("App Started")
    spark=SparkSession.builder.appName("Csv streaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    input_schema=StructType([
        StructField("registration_dttm",StringType(),True),
        StructField("id",IntegerType(),True),StructField("first_name", StringType(), True),
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
    csv_df=spark.readStream.format("csv").schema(input_schema).option("header","true").load(path="stream_data/csv")
    csv_df.printSchema()

    csv_df_query=csv_df.writeStream.format("console").option("checkpointLocation", "csv_streaming_loc") .start()

    csv_df_query.awaitTermination()
    spark.stop()
    print("App Stopped")
