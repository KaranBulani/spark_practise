from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j
from pyspark.sql import *

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDf = spark.read \
        .format("parquet") \
        .load("07-DataSinkDemo/data/flight*.parquet")

    logger.info("Num Partitions before: " + str(flightTimeParquetDf.rdd.getNumPartitions()))
    flightTimeParquetDf.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDf.repartition(5)
    logger.info("Num Partitions after: " +str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    flightTimeParquetDf.write \
        .format("json") \
        .option("path", "07-DataSinkDemo/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()