import os
import sys

sys.path.insert(
    0, os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../")))

from openrefine_wrench import openrefine_api_calls, openrefine_wrench
