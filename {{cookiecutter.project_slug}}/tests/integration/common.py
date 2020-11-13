import unittest
from {{cookiecutter.project_slug}}.jobs.sample.entrypoint import SampleJob
from uuid import uuid4
# pylint: disable=no-name-in-module, import-error
from pyspark.dbutils import DBUtils


class SampleJobIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.job = SampleJob()
        self.dbutils = DBUtils(self.job.spark)
        self.spark = self.job.spark
        self.job.conf["output_path"] = self.job.conf["output_path"] % (uuid4(),)

    def test_sample(self):
        self.job.launch()
        output_count = (
            self.spark
                .read
                .format(self.job.conf["output_format"])
                .load(self.job.conf["output_path"])
                .count()
        )
        self.assertEqual(output_count, self.job.conf["output_size"])

    def tearDown(self):
        self.dbutils.fs.rm(self.job.conf["output_path"], True)