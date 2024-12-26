from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_survey_df, count_by_country

class UtilsTestCase(TestCase):

    #A class method called before tests in an individual class are run. setUpClass is called with the class as the only argument and must be decorated as a classmethod():
    #https://docs.python.org/3/library/unittest.html#unittest.TestCase.setUpClass
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
                .master("local[3]") \
                .appName("HelloSparkTest") \
                .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 9, "Record count should be 9")

    def test_country_count(self):
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        count_list = count_by_country(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["Country"]] = row["count"]
        self.assertEqual(count_dict["United States"], 4, "Count of United States should be 4")
        self.assertEqual(count_dict["Canada"], 2, "Count for Canada should be 2")
        self.assertEqual(count_dict["United Kingdom"], 1, "Count for United Kingdom should be 1")

    #A class method called after tests in an individual class have run. tearDownClass is called with the class as the only argument and must be decorated as a classmethod():
    #https://docs.python.org/3/library/unittest.html#unittest.TestCase.tearDownClass
    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()