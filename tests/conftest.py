import os
import sys
import warnings


def pytest_configure(config):
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    sys.path.insert(
        0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
    )
