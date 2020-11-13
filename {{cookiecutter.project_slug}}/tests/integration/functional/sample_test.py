import unittest
from {{cookiecutter.project_slug}}.jobs import locate_code


# Functional test will run on every Git push, so it must be fast
# That is why, even if we use common code for functional and acceptance tests,
# we still should configure functional test to use less data.
# In this toy example functional test configuration in sample_test.yaml
# specifies smaller output_size compared to the acceptance test.
# See https://explainagile.com/agile/xp-extreme-programming/practices/10-minute-build/
if __name__ == "__main__":
    locate_code() # Enables imports for common tests code copied to DBFS outside of the library wheel
    from integration.common import SampleJobIntegrationTest
    # Cannot use unittest.main() because it system exits in the end
    suite = unittest.TestSuite()
    suite.addTest(SampleJobIntegrationTest('test_sample'))
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
