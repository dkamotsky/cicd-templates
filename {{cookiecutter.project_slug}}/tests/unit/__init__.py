from typing import Optional
from overrides import overrides
from {{cookiecutter.project_slug}}.jobs import Job


class UnitTestEnvironment(Job):

    @overrides
    def _setup_mlflow(self) -> Optional[str]:
        return None

    @overrides
    def launch(self) -> None:
        raise NotImplementedError("Unit Tests should not launch jobs")

