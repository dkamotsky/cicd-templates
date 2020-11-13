import unittest
import tempfile
import shutil
from . import UnitTestEnvironment
from {{cookiecutter.project_slug}}.steps.generation import GenerationStep
from pyspark.sql import SparkSession


class GenerationStepUnitTest(unittest.TestCase):

    def setUp(self):
        self.env: UnitTestEnvironment = UnitTestEnvironment(spark=SparkSession.builder.master("local[1]").getOrCreate(),
                                                            conf="generation_step_test.yaml")
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
