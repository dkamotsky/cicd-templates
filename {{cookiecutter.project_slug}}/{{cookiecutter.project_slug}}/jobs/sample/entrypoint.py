from {{cookiecutter.project_slug}}.jobs import Job
from {{cookiecutter.project_slug}}.steps.generation import GenerationStep
from overrides import overrides


class SampleJob(Job):

    @overrides
    def configure(self):
        assert self.conf, "SampleJob does not have configuration. Did you provide a --conf-file?"
        self.generation_step: GenerationStep = GenerationStep(self.spark, **self.conf)

    @overrides
    def launch(self):
        self.logger.info("Launching sample job")
        self.generation_step()
        self.logger.info("Sample job finished!")


if __name__ == "__main__":
    SampleJob().launch()
