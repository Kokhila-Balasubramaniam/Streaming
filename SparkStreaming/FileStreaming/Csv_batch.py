from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__=="__main__":
    print("App Started")
    spark=SparkSession.builder.appName("Csv streaming").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    csv_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(path="user-data/csv")
    csv_df.printSchema()

    csv_df.show(10)
    spark.stop()
    print("App Stopped")
