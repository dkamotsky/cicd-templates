import unittest
import tempfile
import shutil
from {{cookiecutter.project_slug}}.jobs.sample.entrypoint import SampleJob
from pyspark.sql import SparkSession


class SampleJobUnitTest(unittest.TestCase):
    def setUp(self):
        self.job = SampleJob(spark=SparkSession.builder.master("local[1]").getOrCreate(), conf="sample_test.yaml")
        self.test_dir = tempfile.TemporaryDirectory().name
        self.job.conf['output_path'] = self.test_dir

    def test_sample(self):
        self.job.launch()
        output_count = self.job.spark.\
            read.\
            format(self.test_config["output_format"]).\
            load(self.test_config["output_path"]).\
            count()
        self.assertEqual(output_count, self.job.conf["output_size"])

    def tearDown(self):
        shutil.rmtree(self.test_dir)


if __name__ == "__main__":
    unittest.main()
