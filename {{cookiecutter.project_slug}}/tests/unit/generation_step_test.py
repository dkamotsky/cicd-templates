import unittest
import tempfile
import shutil
from . import UnitTestEnvironment
from {{cookiecutter.project_slug}}.steps.generation import GenerationStep
from pyspark.sql import SparkSession


class GenerationStepUnitTest(unittest.TestCase):

    def setUp(self):
        spark: SparkSession = SparkSession.builder.master("local[1]").\
            config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0").\
            config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").\
            config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").\
            config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore").\
            getOrCreate()
        self.env: UnitTestEnvironment = UnitTestEnvironment(spark=spark, conf="generation_step_test.yaml")
        self.step: GenerationStep = GenerationStep(self.env.spark, **self.env.conf)
        self.step.output_path = tempfile.TemporaryDirectory().name

    def test_sample(self):
        self.step()
        output_count: int = self.env.spark.read.format(self.step.output_format).load(self.step.output_path).count()
        self.assertEqual(output_count, self.step.output_size)

    def tearDown(self):
        shutil.rmtree(self.step.output_path)


if __name__ == "__main__":
    unittest.main()
