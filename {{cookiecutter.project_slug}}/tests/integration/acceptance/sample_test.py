import unittest
from {{cookiecutter.project_slug}}.jobs import locate_code


if __name__ == "__main__":
    locate_code() # Enables imports for common tests code copied to DBFS outside of the library wheel
    from integration.common import SampleJobIntegrationTest
    # Cannot use unittest.main() because it system exits in the end
    suite = unittest.TestSuite()
    suite.addTest(SampleJobIntegrationTest('test_sample'))
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
