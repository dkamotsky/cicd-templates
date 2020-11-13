import unittest
import tempfile
import shutil
from . import UnitTestEnvironment
from {{cookiecutter.project_slug}}.steps.generation import GenerationStep
from pyspark.sql import SparkSession


class GenerationStepUnitTest(unittest.TestCase):

    def setUp(self):
        # Unit tests should not launch the whole job. It is be much better to break up
        # Jobs into Steps, and test each Step independently.
        # Unit tests should not rely on external Spark. Local Spark is OK.
        spark: SparkSession = SparkSession.builder.master("local[1]").\
            config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0").\
            config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").\
            config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").\
            config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore").\
            getOrCreate()
        # Convenience implementation of Job configuration parsing. Provides access to hierarchical configuration
        # and settings overrides.
        self.env: UnitTestEnvironment = UnitTestEnvironment(spark=spark, conf="generation_step_test.yaml")
        self.temp_dir = tempfile.TemporaryDirectory().name

    def test_step(self):
        # Test individual steps, not the whole job.
        step: GenerationStep = GenerationStep(**self.env.conf)
        step.output_path = self.temp_dir
        step(self.env.spark)
        output_count: int = self.env.spark.read.format(step.output_format).load(step.output_path).count()
        self.assertEqual(output_count, step.output_size)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
