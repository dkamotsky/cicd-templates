import unittest
from pyspark.sql import SparkSession
from {{cookiecutter.project_slug}}.jobs.sample.entrypoint import SampleJob
from uuid import uuid4
# pylint: disable=no-name-in-module, import-error
from pyspark.dbutils import DBUtils


class SampleJobIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.job: SampleJob = SampleJob()
        self.dbutils: DBUtils = DBUtils(self.job.spark)
        self.spark: SparkSession = self.job.spark
        self.job.conf["output_path"] = self.job.conf["output_path"] % (uuid4(),)

    def test_sample(self):
        # It is the best practice to run real job in Integration tests. This is why we keep this test code common.
        # But keep in mind that Functional tests should process a lot less data than Acceptance tests.
        # See https://explainagile.com/agile/xp-extreme-programming/practices/10-minute-build/
        self.job.launch()
        output_count: int = self.spark.\
            read.\
            format(self.job.conf["output_format"]).\
            load(self.job.conf["output_path"]).\
            count()
        self.assertEqual(output_count, self.job.conf["output_size"])

    def tearDown(self):
        self.dbutils.fs.rm(self.job.conf["output_path"], True)
