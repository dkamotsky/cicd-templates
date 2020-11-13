from {{cookiecutter.project_slug}}.jobs import Job
from overrides import overrides


class SampleJob(Job):

    @overrides
    def configure(self):
        assert self.conf, "SampleJob does not have configuration. Did you provide a --conf-file?"

    @overrides
    def launch(self):
        self.logger.info("Launching sample job")
        df = self.spark.range(0, self.conf["output_size"])
        df.write.format(self.conf["output_format"]).mode("overwrite").save(self.conf["output_path"])
        self.logger.info("Sample job finished!")


if __name__ == "__main__":
    SampleJob().launch()
