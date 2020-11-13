from pyspark.sql import SparkSession


class GenerationStep:

    def __init__(self, spark: SparkSession, output_size: int, output_format: str, output_path: str, **kwargs) -> None:
        self.spark: SparkSession = spark
        self.output_size: int = output_size
        self.output_format: str = output_format
        self.output_path: str = output_path

    def __call__(self) -> None:
        df = self.spark.range(0, self.output_size)
        df.write.format(self.output_format).mode("overwrite").save(self.output_path)
