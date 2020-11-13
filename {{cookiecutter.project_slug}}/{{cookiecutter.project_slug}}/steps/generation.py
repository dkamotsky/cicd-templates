from pyspark.sql import SparkSession, DataFrame


# Jobs should be organized as sequences of independently testable steps
class GenerationStep:

    def __init__(self, output_size: int, output_format: str, output_path: str, **kwargs) -> None:
        self.output_size: int = output_size
        self.output_format: str = output_format
        self.output_path: str = output_path

    def __call__(self, spark: SparkSession) -> None:
        df: DataFrame = spark.range(0, self.output_size)
        df.write.format(self.output_format).mode("overwrite").save(self.output_path)
