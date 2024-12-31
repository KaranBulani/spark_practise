from lib.logger import Log4j
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeCsvDf = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("06-SparkSchemaDemo/data/flight*.csv")

    flightTimeCsvDf.show(5)
    logger.info("CSV Schema:" +flightTimeCsvDf.schema.simpleString())

    flightTimeJsonDf = spark.read \
        .format("json") \
        .load("06-SparkSchemaDemo/data/flight*.json")

    flightTimeJsonDf.show(5)
    logger.info("JSON Schema:" + flightTimeJsonDf.schema.simpleString())

    flightTimeParquetDf = spark.read \
        .format("parquet") \
        .load("06-SparkSchemaDemo/data/flight*.parquet")

    flightTimeParquetDf.show(5)
    logger.info("PARQUET Schema:" + flightTimeParquetDf.schema.simpleString())