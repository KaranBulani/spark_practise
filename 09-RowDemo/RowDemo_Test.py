from datetime import date
from unittest import TestCase

from pyspark.sql import *
from pyspark.sql.types import *

from RowDemo import to_date_df

class RowDemoTestCase(TestCase):

    #A class method called before tests in an individual class are run. setUpClass is called with the class as the only argument and must be decorated as a classmethod():
    #https://docs.python.org/3/library/unittest.html#unittest.TestCase.setUpClass
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
                .master("local[3]") \
                .appName("RowDemoTest") \
                .getOrCreate()
        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [Row("123","04/05/2020"),Row("124","4/5/2020"),Row("125","04/5/2020"),Row("126","4/05/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows,2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_data_type(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_date_value(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))

    #A class method called after tests in an individual class have run. tearDownClass is called with the class as the only argument and must be decorated as a classmethod():
    #https://docs.python.org/3/library/unittest.html#unittest.TestCase.tearDownClass
    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()